use std::collections::BTreeMap;
use std::time::SystemTime;

use ignore::Walk;
use kdam::tqdm;
use log::info;
use relative_path::RelativePath;

use super::RepositorySession;
use crate::chunk;
use crate::repository::ids::{blob_object_id, list_object_id, map_object_id, snapshot_object_id};
use crate::{Blob, Chunk, List, Map, Object, ObjectId, Snapshot, rollsum};

impl RepositorySession {
    pub fn snapshot(&mut self, message: Option<String>) {
        self.chunk_storage
            .ensure_ready()
            .expect("failed to create chunks directory");

        let base = &self.base;
        let prev_tree: BTreeMap<String, (Option<SystemTime>, ObjectId)> = {
            if let Some(Object::Snapshot(snap)) = self.repository.objects.get(&self.repository.head) {
                let tree_id = snap.tree;
                if let Some(Object::Map(tree)) = self.repository.objects.get(&tree_id) {
                    tree.entries
                        .iter()
                        .map(|(path, &blob_id)| {
                            let mtime = match self.repository.objects.get(&blob_id) {
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
            let Some(rel) = path.strip_prefix(base).ok() else {
                continue;
            };
            let Some(rel_path) = RelativePath::from_path(rel)
                .ok()
                .map(|p| p.normalize().into_string())
            else {
                continue;
            };
            if rel_path == crate::REPO_DIR_NAME || rel_path.starts_with(crate::REPO_DIR_PREFIX) {
                continue;
            }

            let metadata =
                std::fs::metadata(path).unwrap_or_else(|_| panic!("failed to stat {path:?}"));
            let mtime = metadata.modified().ok();
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
            for c in tqdm!(chunk::split_chunks(file), desc = "Chunking", total = chunks, position = 1) {
                let (id, data, digest) = c.unwrap_or_else(|e| panic!("failed to read {path:?}: {e}"));
                chunk_leaves.push((id, digest));
                self.repository.objects.entry(id).or_insert_with(|| {
                    self.chunk_storage
                        .write_chunk_if_missing(id, &data)
                        .expect("failed to write chunk");
                    Object::Chunk(Chunk)
                });
            }

            let list_id = build_fanout_list(&mut self.repository, &chunk_leaves);
            let bid = blob_object_id(list_id);
            self.repository.objects.insert(
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
        self.repository
            .objects
            .insert(tid, Object::Map(Map { entries: tree_files }));
        let snap = Snapshot {
            parents: if self.repository.head.0.iter().all(|x| *x == 0) {
                vec![]
            } else {
                vec![self.repository.head]
            },
            tree: tid,
            message,
            date: SystemTime::now(),
        };
        let sid = snapshot_object_id(&snap);
        self.repository.objects.insert(sid, Object::Snapshot(snap));
        self.repository.head = sid;
        info!("Snapshot: {}", sid.to_hex());
        self.save().expect("failed to save repository");
    }
}

fn build_fanout_list(repo: &mut crate::Repository, leaves: &[(ObjectId, u64)]) -> ObjectId {
    if leaves.is_empty() {
        let id = list_object_id(&[]);
        repo.objects
            .entry(id)
            .or_insert_with(|| Object::List(List { entries: vec![] }));
        return id;
    }
    if leaves.len() == 1 {
        let id = list_object_id(&[leaves[0].0]);
        repo.objects
            .entry(id)
            .or_insert_with(|| Object::List(List { entries: vec![leaves[0].0] }));
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
                repo.objects
                    .entry(list_id)
                    .or_insert_with(|| Object::List(List { entries: bucket.clone() }));
                grouped.push((list_id, *digest));
                bucket.clear();
            }
        }
        if !bucket.is_empty() {
            let list_id = list_object_id(&bucket);
            repo.objects
                .entry(list_id)
                .or_insert_with(|| Object::List(List { entries: bucket.clone() }));
            let digest = level.last().map(|(_, d)| *d).unwrap_or(0);
            grouped.push((list_id, digest));
        }
        level = grouped;
        if level.len() == 1 || fan_level == rollsum::FAN_MASKS.len() - 1 {
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
