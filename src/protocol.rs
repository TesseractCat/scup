use serde::{Serialize, Deserialize};
use crate::{ObjectId, Repository};

#[derive(Serialize, Deserialize)]
pub enum Request {
    Status,
    Pull,
    Push {
        repository: Repository,
        chunks: Vec<(ObjectId, Vec<u8>)>,
    },
}

#[derive(Serialize, Deserialize)]
pub enum Response {
    Status { head: ObjectId },
    Pull {
        repository: Repository,
        chunks: Vec<(ObjectId, Vec<u8>)>,
    },
    PushOk,
    Error(String),
}
