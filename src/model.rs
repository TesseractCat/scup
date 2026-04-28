use core::fmt::Debug;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, time::SystemTime};

pub fn to_hex(id: &[u8; 32]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

fn to_short_hex(id: &[u8; 32]) -> String {
    id.iter().take(4).map(|b| format!("{b:02x}")).collect()
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct ObjectId(pub [u8; 32]);

impl ObjectId {
    pub fn to_hex(&self) -> String {
        to_hex(&self.0)
    }

    pub fn to_short_hex(&self) -> String {
        to_short_hex(&self.0)
    }
}

impl Debug for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_short_hex())
    }
}

impl From<[u8; 32]> for ObjectId {
    fn from(value: [u8; 32]) -> Self {
        Self(value)
    }
}

impl AsRef<[u8]> for ObjectId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, PartialOrd, Ord, Eq, Hash, Default)]
pub struct RepositoryId(pub [u8; 32]);

impl RepositoryId {
    pub fn to_hex(&self) -> String {
        to_hex(&self.0)
    }

    pub fn to_short_hex(&self) -> String {
        to_short_hex(&self.0)
    }

    pub fn from_hex(s: &str) -> Option<Self> {
        let trimmed = s.trim();
        if trimmed.len() != 64 {
            return None;
        }

        let mut out = [0u8; 32];
        for i in 0..32 {
            out[i] = u8::from_str_radix(&trimmed[i * 2..i * 2 + 2], 16).ok()?;
        }
        Some(Self(out))
    }
}

impl Debug for RepositoryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_short_hex())
    }
}

impl std::fmt::Display for RepositoryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl From<[u8; 32]> for RepositoryId {
    fn from(value: [u8; 32]) -> Self {
        Self(value)
    }
}

impl AsRef<[u8]> for RepositoryId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Chunk;

#[derive(Serialize, Deserialize, Clone)]
pub struct Blob {
    pub chunks: ObjectId,
    pub modified_time: Option<SystemTime>,
    pub accessed_time: Option<SystemTime>,
    pub created_time: Option<SystemTime>,
    pub mode: u32,
}

impl Debug for Blob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Blob(list={:?}, modified {:?} ago)",
            self.chunks,
            self.modified_time
                .and_then(|t| t.elapsed().ok())
                .unwrap_or_default()
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct List {
    pub entries: Vec<ObjectId>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Map {
    pub entries: BTreeMap<String, ObjectId>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Snapshot {
    #[serde(default)]
    pub parents: Vec<ObjectId>,
    pub tree: ObjectId,
    pub message: Option<String>,
    pub date: SystemTime,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Object {
    Chunk(Chunk),
    Blob(Blob),
    Map(Map),
    List(List),
    Snapshot(Snapshot),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Repository {
    #[serde(default)]
    pub repo_uuid: RepositoryId,
    pub objects: BTreeMap<ObjectId, Object>,
    pub head: ObjectId,
}
