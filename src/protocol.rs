use crate::{Object, ObjectId};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Status,
    Push {
        repo_uuid: [u8; 32],
    },
    PullSnapshotIds {
        repo_uuid: [u8; 32],
    },
    PullObjects {
        repo_uuid: [u8; 32],
        object_ids: Vec<ObjectId>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Status {
        head: ObjectId,
    },
    PullSnapshotIds {
        head: ObjectId,
        snapshot_ids: Vec<ObjectId>,
    },
    PullObjects {
        objects: Vec<(ObjectId, Object)>,
        chunks: Vec<(ObjectId, Vec<u8>)>,
    },
    PushComplete,
    Error(String),
}
