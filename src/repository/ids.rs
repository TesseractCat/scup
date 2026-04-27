use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, time::SystemTime};

use crate::{ObjectId, Snapshot};

pub(super) fn list_object_id(entries: &[ObjectId]) -> ObjectId {
    let mut h = Sha256::new();
    h.update(b"list");
    for id in entries {
        h.update(id);
    }
    <[u8; 32]>::from(h.finalize()).into()
}

pub(super) fn blob_object_id(list_id: ObjectId) -> ObjectId {
    let mut h = Sha256::new();
    h.update(b"blob");
    h.update(list_id);
    <[u8; 32]>::from(h.finalize()).into()
}

pub(super) fn map_object_id(files: &BTreeMap<String, ObjectId>) -> ObjectId {
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

pub(super) fn snapshot_object_id(snap: &Snapshot) -> ObjectId {
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
