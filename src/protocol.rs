use crate::{ObjectId, Repository};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum Request {
    Status,
    Pull {
        repo_uuid: [u8; 32],
    },
    Push {
        repo_uuid: [u8; 32],
        repository: Repository,
        chunks: Vec<(ObjectId, Vec<u8>)>,
    },
}

#[derive(Serialize, Deserialize)]
pub enum Response {
    Status {
        head: ObjectId,
    },
    Pull {
        repository: Repository,
        chunks: Vec<(ObjectId, Vec<u8>)>,
    },
    PushOk,
    Error(String),
}
