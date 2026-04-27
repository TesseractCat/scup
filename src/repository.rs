use ignore::Walk;
use kdam::tqdm;
use log::info;
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::Path, time::SystemTime};

use crate::chunk::split_chunks;
use crate::{Blob, Chunk, List, Map, Object, ObjectId, Repository, Snapshot, rollsum, to_hex};

fn list_object_id(entries: &[ObjectId]) -> ObjectId {
    let mut h = Sha256::new();
    h.update(b"list");
    for id in entries {
        h.update(id);
    }
    <[u8; 32]>::from(h.finalize()).into()
}

fn blob_object_id(list_id: ObjectId) -> ObjectId {
    let mut h = Sha256::new();
    h.update(b"blob");
    h.update(list_id);
    <[u8; 32]>::from(h.finalize()).into()
}

fn map_object_id(files: &BTreeMap<String, ObjectId>) -> ObjectId {
    let mut h = Sha256::new();
    h.update(b"map");
    let mut entries: Vec<_> = files.iter().collect();
    entries.sort_by_key(|(name, _)| name.as_str());
    for (name, id) in entries {
        h.update(&(name.len() as u32).to_le_bytes());
        h.update(name.as_bytes());
        h.update(id);
    }
    <[u8; 32]>::from(h.finalize()).into()
}

fn build_fanout_list(repo: &mut Repository, leaves: &[(ObjectId, u64)]) -> ObjectId {
    if leaves.is_empty() {
        let id = list_object_id(&[]);
        repo.objects
            .entry(id)
            .or_insert_with(|| Object::List(List { entries: vec![] }));
        return id;
    }

    if leaves.len() == 1 {
        let id = list_object_id(&[leaves[0].0]);
        repo.objects.entry(id).or_insert_with(|| {
            Object::List(List {
                entries: vec![leaves[0].0],
            })
        });
        return id;
    }

    let mut level: Vec<(ObjectId, u64)> = leaves.to_vec();

    for (fan_level, mask) in rollsum::FAN_MASKS.iter().enumerate() {
        if level.len() <= 1 {
            break;
        }

        let mut grouped: Vec<(ObjectId, u64)> = Vec::new();
        let mut bucket: Vec<ObjectId> = Vec::new();

        for (id, digest) in &level {
            bucket.push(*id);
            if (*digest & *mask) == *mask {
                let list_id = list_object_id(&bucket);
                repo.objects.entry(list_id).or_insert_with(|| {
                    Object::List(List {
                        entries: bucket.clone(),
                    })
                });
                grouped.push((list_id, *digest));
                bucket.clear();
            }
        }

        if !bucket.is_empty() {
            let list_id = list_object_id(&bucket);
            repo.objects.entry(list_id).or_insert_with(|| {
                Object::List(List {
                    entries: bucket.clone(),
                })
            });
            let digest = level.last().map(|(_, d)| *d).unwrap_or(0);
            grouped.push((list_id, digest));
        }

        level = grouped;

        if level.len() == 1 {
            break;
        }

        if fan_level == rollsum::FAN_MASKS.len() - 1 {
            break;
        }
    }

    while level.len() > 1 {
        let ids: Vec<ObjectId> = level.iter().map(|(id, _)| *id).collect();
        let top_id = list_object_id(&ids);
        repo.objects
            .entry(top_id)
            .or_insert_with(|| Object::List(List { entries: ids }));
        let digest = level.last().map(|(_, d)| *d).unwrap_or(0);
        level = vec![(top_id, digest)];
    }

    level[0].0
}

fn snapshot_object_id(snap: &Snapshot) -> ObjectId {
    let mut h = Sha256::new();
    h.update(b"snapshot");
    h.update(&snap.tree);
    for parent in &snap.parents {
        h.update(parent);
    }
    let msg = snap.message.as_deref().unwrap_or("");
    h.update(&(msg.len() as u32).to_le_bytes());
    h.update(msg.as_bytes());
    let dur = snap
        .date
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    h.update(&dur.as_secs().to_le_bytes());
    h.update(&dur.subsec_nanos().to_le_bytes());
    <[u8; 32]>::from(h.finalize()).into()
}

impl Repository {
    fn map_entries_for_snapshot(&self, snapshot_id: ObjectId) -> BTreeMap<String, ObjectId> {
        let Some(Object::Snapshot(snap)) = self.objects.get(&snapshot_id) else {
            return BTreeMap::new();
        };
        let Some(Object::Map(tree)) = self.objects.get(&snap.tree) else {
            return BTreeMap::new();
        };
        tree.entries.clone()
    }

    fn blob_modified_time(&self, blob_id: ObjectId) -> Option<SystemTime> {
        match self.objects.get(&blob_id) {
            Some(Object::Blob(blob)) => blob.modified_time,
            _ => None,
        }
    }

    pub(crate) fn merge(&mut self, incoming: Repository) {
        if self.repo_uuid != incoming.repo_uuid {
            return;
        }

        let local_head = self.head;
        let remote_head = incoming.head;

        if local_head == remote_head {
            return;
        }

        for (id, obj) in incoming.objects {
            self.objects.insert(id, obj);
        }

        let local_files = self.map_entries_for_snapshot(local_head);
        let remote_files = self.map_entries_for_snapshot(remote_head);

        let mut merged_files: BTreeMap<String, ObjectId> = BTreeMap::new();
        let mut all_paths: BTreeMap<String, ()> = BTreeMap::new();
        for path in local_files.keys() {
            all_paths.insert(path.clone(), ());
        }
        for path in remote_files.keys() {
            all_paths.insert(path.clone(), ());
        }

        for (path, _) in all_paths {
            match (local_files.get(&path), remote_files.get(&path)) {
                (Some(&l), Some(&r)) if l == r => {
                    merged_files.insert(path, l);
                }
                (Some(&l), Some(&r)) => {
                    let l_mt = self.blob_modified_time(l);
                    let r_mt = self.blob_modified_time(r);
                    let chosen = match (l_mt, r_mt) {
                        (Some(lt), Some(rt)) => {
                            if rt > lt {
                                r
                            } else {
                                l
                            }
                        }
                        (Some(_), None) => l,
                        (None, Some(_)) => r,
                        (None, None) => r,
                    };
                    merged_files.insert(path, chosen);
                }
                (Some(&l), None) => {
                    merged_files.insert(path, l);
                }
                (None, Some(&r)) => {
                    merged_files.insert(path, r);
                }
                (None, None) => {}
            }
        }

        let merged_map_id = map_object_id(&merged_files);
        self.objects.insert(
            merged_map_id,
            Object::Map(Map {
                entries: merged_files,
            }),
        );

        let mut parents = vec![local_head, remote_head];
        parents.sort();
        parents.dedup();

        let merged_snapshot = Snapshot {
            parents,
            tree: merged_map_id,
            message: Some("Merge".to_string()),
            date: SystemTime::now(),
        };
        let merged_snapshot_id = snapshot_object_id(&merged_snapshot);
        self.objects
            .insert(merged_snapshot_id, Object::Snapshot(merged_snapshot));
        self.head = merged_snapshot_id;
    }

    pub fn init(base: &Path) -> Self {
        std::fs::create_dir_all(base.join(".syncup/chunks"))
            .expect("failed to create .syncup/chunks");
        let mut repo_uuid = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut repo_uuid);

        let mut repo = Repository {
            repo_uuid,
            objects: BTreeMap::new(),
            head: ObjectId([0u8; 32]),
        };
        repo.snapshot(base, Some("Initial snapshot".to_string()));
        info!("Repository UUID: {}", to_hex(&repo.repo_uuid));
        repo
    }

    pub fn snapshot(&mut self, base: &Path, message: Option<String>) {
        std::fs::create_dir_all(base.join(".syncup/chunks"))
            .expect("failed to create .syncup/chunks");

        // Build path -> (mtime, blob_id) from the previous snapshot's tree.
        let prev_tree: BTreeMap<String, (Option<SystemTime>, ObjectId)> = {
            if let Some(Object::Snapshot(snap)) = self.objects.get(&self.head) {
                let tree_id = snap.tree;
                if let Some(Object::Map(tree)) = self.objects.get(&tree_id) {
                    tree.entries
                        .iter()
                        .map(|(path, &blob_id)| {
                            let mtime = match self.objects.get(&blob_id) {
                                Some(Object::Blob(blob)) => blob.modified_time,
                                _ => None,
                            };
                            (path.clone(), (mtime, blob_id))
                        })
                        .collect()
                } else {
                    BTreeMap::new()
                }
            } else {
                BTreeMap::new()
            }
        };

        let mut tree_files: BTreeMap<String, ObjectId> = BTreeMap::new();

        let mut entries: Vec<_> = Walk::new(base)
            .flatten()
            .filter(|e| e.file_type().map_or(false, |t| t.is_file()))
            .collect();
        entries.sort_by_key(|e| e.path().to_path_buf());

        for entry in tqdm!(entries.iter(), desc = "Processing files", position = 0) {
            let path = entry.path();
            let rel_path = path.to_string_lossy().into_owned();

            let metadata =
                std::fs::metadata(path).unwrap_or_else(|_| panic!("failed to stat {path:?}"));
            let mtime = metadata.modified().ok();

            // Skip files whose mtime hasn't changed since the last snapshot.
            if let Some(&(prev_mtime, prev_blob_id)) = prev_tree.get(&rel_path) {
                if prev_mtime.is_some() && prev_mtime == mtime {
                    tree_files.insert(rel_path, prev_blob_id);
                    continue;
                }
            }

            let file =
                std::fs::File::open(path).unwrap_or_else(|_| panic!("failed to open {path:?}"));
            let mut chunk_leaves: Vec<(ObjectId, u64)> = Vec::new();

            let size = metadata.len() as usize;
            let chunks = size / rollsum::AVERAGE_CHUNK_SIZE;
            for chunk in tqdm!(
                split_chunks(file),
                desc = "Chunking",
                total = chunks,
                position = 1
            ) {
                let (id, data, digest) =
                    chunk.unwrap_or_else(|e| panic!("failed to read {path:?}: {e}"));
                chunk_leaves.push((id, digest));
                self.objects.entry(id).or_insert_with(|| {
                    std::fs::write(
                        base.join(format!(".syncup/chunks/{}", to_hex(&id.0))),
                        &data,
                    )
                    .expect("failed to write chunk");
                    Object::Chunk(Chunk)
                });
            }

            let list_id = build_fanout_list(self, &chunk_leaves);
            let bid = blob_object_id(list_id);
            self.objects.insert(
                bid,
                Object::Blob(Blob {
                    chunks: list_id,
                    created_time: metadata.created().ok(),
                    modified_time: mtime,
                    accessed_time: metadata.accessed().ok(),
                    mode: 0,
                }),
            );
            tree_files.insert(rel_path, bid);
        }

        let tid = map_object_id(&tree_files);
        self.objects.insert(
            tid,
            Object::Map(Map {
                entries: tree_files,
            }),
        );

        let snap = Snapshot {
            parents: if self.head.0.iter().all(|x| *x == 0) {
                vec![]
            } else {
                vec![self.head]
            },
            tree: tid,
            message,
            date: SystemTime::now(),
        };
        let sid = snapshot_object_id(&snap);
        self.objects.insert(sid, Object::Snapshot(snap));
        self.head = sid;

        info!("Snapshot: {}", to_hex(&sid.0));
        self.save(base);
    }

    pub fn save(&self, base: &Path) {
        let bytes = postcard::to_allocvec(self).expect("failed to serialize repository");
        std::fs::write(base.join(".syncup/repository"), bytes)
            .expect("failed to write .syncup/repository");
    }

    pub fn load(base: &Path) -> Self {
        let bytes = std::fs::read(base.join(".syncup/repository"))
            .expect("failed to read .syncup/repository");
        postcard::from_bytes(&bytes).expect("failed to deserialize repository")
    }
}
