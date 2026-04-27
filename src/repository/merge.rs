use std::{collections::BTreeMap, time::SystemTime};

use crate::{Map, Object, ObjectId, Repository, Snapshot};

use super::ids::{map_object_id, snapshot_object_id};

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
}
