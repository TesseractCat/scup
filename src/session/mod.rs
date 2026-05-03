use std::path::PathBuf;

use crate::model::Repository;
use crate::storage::ChunkStorage;

mod snapshot;
mod storage;

pub struct RepositorySession {
    pub base: PathBuf,
    pub repository: Repository,
    pub chunk_storage: Box<dyn ChunkStorage>,
}
