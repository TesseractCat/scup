use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Context;
use log::info;
use rand::RngCore;

use super::RepositorySession;
use crate::model::{Repository, RepositoryId};
use crate::storage::{ChunkStorage, FilesystemChunkStorage};
use crate::ObjectId;

impl RepositorySession {
    pub fn load(base: &Path) -> anyhow::Result<Self> {
        let bytes = std::fs::read(base.join(crate::REPOSITORY_FILE))
            .context("failed to read repository file")?;
        let repository = postcard::from_bytes(&bytes).context("failed to deserialize repository")?;
        let chunk_storage: Box<dyn ChunkStorage> = Box::new(FilesystemChunkStorage::new(base));
        chunk_storage.ensure_ready()?;
        Ok(Self {
            base: base.to_path_buf(),
            repository,
            chunk_storage,
        })
    }

    pub fn init(base: &Path) -> anyhow::Result<Self> {
        let chunk_storage: Box<dyn ChunkStorage> = Box::new(FilesystemChunkStorage::new(base));
        chunk_storage.ensure_ready()?;

        let mut repo_uuid = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut repo_uuid);
        let repository = Repository {
            repo_uuid: RepositoryId(repo_uuid),
            objects: BTreeMap::new(),
            head: ObjectId([0u8; 32]),
        };

        let mut session = Self {
            base: base.to_path_buf(),
            repository,
            chunk_storage,
        };
        session.snapshot(Some("Initial snapshot".to_string()));
        info!("Repository UUID: {}", session.repository.repo_uuid);
        Ok(session)
    }

    pub fn save(&self) -> anyhow::Result<()> {
        let bytes = postcard::to_allocvec(&self.repository).context("failed to serialize repository")?;
        std::fs::write(self.base.join(crate::REPOSITORY_FILE), bytes)
            .context("failed to write repository file")?;
        Ok(())
    }
}
