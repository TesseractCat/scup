use std::{collections::HashMap, time::SystemTime};
use serde::{Serialize, Deserialize};
use core::fmt::Debug;
use sha2::{Sha256, Digest};
use kdam::tqdm;
use ignore::Walk;

mod rollsum;
use rollsum::Rollsum;

mod cli;
use cli::cli;

fn to_hex(id: &[u8; 32]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}
fn to_short_hex(id: &[u8; 32]) -> String {
    id.iter().take(4).map(|b| format!("{b:02x}")).collect()
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
struct ObjectId([u8; 32]);
impl Debug for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", to_short_hex(&self.0))
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

#[derive(Serialize, Deserialize)]
struct Chunk {
    data: Vec<u8>
}
impl Debug for Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Chunk of length {} bytes", self.data.len())
    }
}

#[derive(Serialize, Deserialize)]
struct Blob {
    chunks: Vec<ObjectId>
}
impl Debug for Blob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Blob of length {} chunks", self.chunks.len())
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Tree {
    files: HashMap<String, ObjectId>
}

#[derive(Serialize, Deserialize, Debug)]
struct Snapshot {
    parents: Vec<ObjectId>,
    tree: ObjectId,
    message: Option<String>,
    date: SystemTime
}

#[derive(Serialize, Deserialize, Debug)]
enum Object {
    Blob(Blob),
    Tree(Tree),
    Snapshot(Snapshot)
}

#[derive(Serialize, Deserialize, Debug)]
struct Repository {
    chunks: HashMap<ObjectId, Chunk>,
    objects: HashMap<ObjectId, Object>,
    head: ObjectId
}

fn split_chunks(data: &[u8]) -> Vec<(ObjectId, Vec<u8>)> {
    let mut rs = Rollsum::new();
    let mut result = Vec::new();
    let mut start = 0;

    for (i, &byte) in data.iter().enumerate() {
        rs.roll(byte);
        if rs.digest() & 0x1FFF == 0 {
            let chunk = data[start..=i].to_vec();
            let id = <[u8; 32]>::from(Sha256::digest(&chunk));
            result.push((id.into(), chunk));
            start = i + 1;
        }
    }
    if start < data.len() {
        let chunk = data[start..].to_vec();
        let id = <[u8; 32]>::from(Sha256::digest(&chunk));
        result.push((id.into(), chunk));
    }
    result
}

fn blob_object_id(chunk_ids: &[ObjectId]) -> ObjectId {
    let mut h = Sha256::new();
    h.update(b"blob");
    for id in chunk_ids {
        h.update(id);
    }
    <[u8; 32]>::from(h.finalize()).into()
}

fn tree_object_id(files: &HashMap<String, ObjectId>) -> ObjectId {
    let mut h = Sha256::new();
    h.update(b"tree");
    let mut entries: Vec<_> = files.iter().collect();
    entries.sort_by_key(|(name, _)| name.as_str());
    for (name, id) in entries {
        h.update(&(name.len() as u32).to_le_bytes());
        h.update(name.as_bytes());
        h.update(id);
    }
    <[u8; 32]>::from(h.finalize()).into()
}

fn snapshot_object_id(snap: &Snapshot) -> ObjectId {
    let mut h = Sha256::new();
    h.update(b"snapshot");
    h.update(&(snap.parents.len() as u32).to_le_bytes());
    for p in &snap.parents {
        h.update(p);
    }
    h.update(&snap.tree);
    let msg = snap.message.as_deref().unwrap_or("");
    h.update(&(msg.len() as u32).to_le_bytes());
    h.update(msg.as_bytes());
    let dur = snap.date.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    h.update(&dur.as_secs().to_le_bytes());
    h.update(&dur.subsec_nanos().to_le_bytes());
    <[u8; 32]>::from(h.finalize()).into()
}

fn init_repo() -> Repository {
    let mut chunks: HashMap<ObjectId, Chunk> = HashMap::new();
    let mut objects: HashMap<ObjectId, Object> = HashMap::new();
    let mut tree_files: HashMap<String, ObjectId> = HashMap::new();
    let mut file_summaries: Vec<(String, ObjectId, usize)> = Vec::new();

    for entry in tqdm!(Walk::new(std::path::Path::new(".")), desc = "Processing files") {
        if !entry.as_ref().unwrap().file_type().unwrap().is_file() { continue; }

        let path = entry.as_ref().unwrap().path();
        let data = std::fs::read(path).unwrap_or_else(|_| panic!("failed to read {path:?}"));
        let file_chunks = split_chunks(&data);
        let mut chunk_ids: Vec<ObjectId> = Vec::new();

        for (id, chunk_data) in file_chunks {
            chunk_ids.push(id);
            chunks.entry(id).or_insert(Chunk { data: chunk_data });
        }

        let bid = blob_object_id(&chunk_ids);
        let rel_path = path.to_string_lossy().into_owned();
        let n_chunks = chunk_ids.len();
        objects.insert(bid, Object::Blob(Blob { chunks: chunk_ids }));
        tree_files.insert(rel_path.clone(), bid);
        file_summaries.push((rel_path, bid, n_chunks));
    }

    let tid = tree_object_id(&tree_files);
    objects.insert(tid, Object::Tree(Tree { files: tree_files }));

    let snap = Snapshot {
        parents: vec![],
        tree: tid,
        message: Some("Initial snapshot".to_string()),
        date: SystemTime::now(),
    };
    let sid = snapshot_object_id(&snap);
    objects.insert(sid, Object::Snapshot(snap));

    // println!("Snapshot: {}", to_hex(&sid));
    // println!("Tree:     {}", to_hex(&tid));
    // for (rel_path, bid, n_chunks) in &file_summaries {
    //     println!("  {} [{}]: {n_chunks} chunk(s)", rel_path, to_hex(bid));
    // }

    let repo = Repository { chunks, objects, head: sid };
    println!("{:#?}", repo);

    repo
}

fn chunk_file(path: &str) {
    let data = std::fs::read(path).expect("failed to read file");
    let mut rs = Rollsum::new();
    let mut chunk_start = 0;

    for (i, &byte) in data.iter().enumerate() {
        rs.roll(byte);
        if rs.digest() & 0x1FFF == 0 {
            println!("chunk [{chunk_start}, {}), len={}", i + 1, i + 1 - chunk_start);
            chunk_start = i + 1;
        }
    }

    if chunk_start < data.len() {
        println!("chunk [{chunk_start}, {}), len={} (final)", data.len(), data.len() - chunk_start);
    }
}

fn main() {
    let matches = cli().get_matches();

    match matches.subcommand() {
        Some(("init", _)) => {
            init_repo();
        }
        Some(("chunk", sub)) => {
            let path = sub.get_one::<String>("FILE").unwrap();
            chunk_file(path);
        }
        _ => {}
    }
}
