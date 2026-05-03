use std::path::{Path, PathBuf};

use anyhow::Context;

use crate::ObjectId;

pub trait ChunkStorage: Send + Sync {
    fn ensure_ready(&self) -> anyhow::Result<()>;
    fn write_chunk_if_missing(&self, id: ObjectId, data: &[u8]) -> anyhow::Result<()>;
    fn read_chunk(&self, id: ObjectId) -> anyhow::Result<Vec<u8>>;
}


pub struct FilesystemChunkStorage {
    base: PathBuf,
}

impl FilesystemChunkStorage {
    pub fn new(base: &Path) -> Self {
        Self {
            base: base.to_path_buf(),
        }
    }

    fn chunk_path(&self, id: ObjectId) -> PathBuf {
        self.base
            .join(format!("{}/{}", crate::CHUNKS_DIR, id.to_hex()))
    }
}

impl ChunkStorage for FilesystemChunkStorage {
    fn ensure_ready(&self) -> anyhow::Result<()> {
        std::fs::create_dir_all(self.base.join(crate::CHUNKS_DIR))
            .context("failed to create chunks directory")
    }

    fn write_chunk_if_missing(&self, id: ObjectId, data: &[u8]) -> anyhow::Result<()> {
        let path = self.chunk_path(id);
        if !path.exists() {
            std::fs::write(path, data)
                .with_context(|| format!("failed to write chunk {}", id.to_hex()))?;
        }
        Ok(())
    }

    fn read_chunk(&self, id: ObjectId) -> anyhow::Result<Vec<u8>> {
        let path = self.chunk_path(id);
        std::fs::read(path).with_context(|| format!("missing chunk {}", id.to_hex()))
    }
}

#[allow(dead_code)]
pub struct SledChunkStorage {
    base: PathBuf,
}

#[allow(dead_code)]
impl SledChunkStorage {
    pub fn new(base: &Path) -> Self {
        Self {
            base: base.to_path_buf(),
        }
    }

    fn db_path(&self) -> PathBuf {
        self.base.join(format!("{}/sled", crate::CHUNKS_DIR))
    }

    fn db(&self) -> anyhow::Result<sled::Db> {
        sled::open(self.db_path()).context("failed to open sled chunk database")
    }
}

#[allow(dead_code)]
impl ChunkStorage for SledChunkStorage {
    fn ensure_ready(&self) -> anyhow::Result<()> {
        std::fs::create_dir_all(self.base.join(crate::CHUNKS_DIR))
            .context("failed to create chunks directory")?;
        let db = self.db()?;
        db.flush().context("failed to initialize sled chunk database")?;
        Ok(())
    }

    fn write_chunk_if_missing(&self, id: ObjectId, data: &[u8]) -> anyhow::Result<()> {
        let db = self.db()?;
        let _ = db
            .compare_and_swap(id.as_ref(), None as Option<&[u8]>, Some(data))
            .with_context(|| format!("failed to write chunk {}", id.to_hex()))?;
        db.flush().context("failed to flush sled chunk database")?;
        Ok(())
    }

    fn read_chunk(&self, id: ObjectId) -> anyhow::Result<Vec<u8>> {
        let db = self.db()?;
        let val = db
            .get(id.as_ref())
            .with_context(|| format!("failed to read chunk {}", id.to_hex()))?
            .with_context(|| format!("missing chunk {}", id.to_hex()))?;
        Ok(val.to_vec())
    }
}
