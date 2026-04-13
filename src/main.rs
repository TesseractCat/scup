use std::{collections::BTreeMap, io::Read, path::{Path, PathBuf}, time::SystemTime};
use serde::{Serialize, Deserialize};
use core::fmt::Debug;
use sha2::{Sha256, Digest};
use kdam::tqdm;
use ignore::Walk;
use postcard;

mod rollsum;
use rollsum::Rollsum;

mod cli;
use cli::cli;

mod protocol;
mod discover;
mod serve;

pub fn to_hex(id: &[u8; 32]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}
fn to_short_hex(id: &[u8; 32]) -> String {
    id.iter().take(4).map(|b| format!("{b:02x}")).collect()
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct ObjectId(pub [u8; 32]);
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

#[derive(Serialize, Deserialize, Debug)]
pub struct Chunk;

#[derive(Serialize, Deserialize)]
pub struct Blob {
    pub chunks: Vec<ObjectId>,
    pub modified_time: Option<SystemTime>,
    pub accessed_time: Option<SystemTime>,
    pub created_time: Option<SystemTime>,
    pub mode: u32
}
impl Debug for Blob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Blob of length {} chunks [modified {:?} ago]",
            self.chunks.len(),
            self.modified_time.and_then(|t| t.elapsed().ok()).unwrap_or_default())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Tree {
    pub files: BTreeMap<String, ObjectId>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Snapshot {
    pub tree: ObjectId,
    pub message: Option<String>,
    pub date: SystemTime
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Object {
    Blob(Blob),
    Tree(Tree),
    Snapshot(Snapshot)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Repository {
    pub chunks: BTreeMap<ObjectId, Chunk>,
    pub objects: BTreeMap<ObjectId, Object>,
    pub head: ObjectId
}

struct ChunkReader<R: Read> {
    reader: R,
    rs: Rollsum,
    read_buf: Vec<u8>,
    read_pos: usize,
    read_len: usize,
    chunk_buf: Vec<u8>,
    done: bool,
}

impl<R: Read> Iterator for ChunkReader<R> {
    type Item = std::io::Result<(ObjectId, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            // Refill the read buffer when exhausted.
            if self.read_pos >= self.read_len {
                self.read_len = match self.reader.read(&mut self.read_buf) {
                    Ok(n) => n,
                    Err(e) => return Some(Err(e)),
                };
                self.read_pos = 0;
                if self.read_len == 0 {
                    // EOF: emit the final chunk if any data remains.
                    self.done = true;
                    return if self.chunk_buf.is_empty() {
                        None
                    } else {
                        let data = std::mem::replace(&mut self.chunk_buf, Vec::with_capacity(rollsum::AVERAGE_CHUNK_SIZE));
                        let id: ObjectId = <[u8; 32]>::from(Sha256::digest(&data)).into();
                        Some(Ok((id, data)))
                    };
                }
            }

            // Scan read_buf for a chunk boundary, deferring data copy until one is found.
            let start = self.read_pos;
            while self.read_pos < self.read_len {
                let byte = self.read_buf[self.read_pos];
                self.read_pos += 1;
                self.rs.roll(byte);

                if self.rs.digest() & rollsum::SPLIT_MASK == 0
                    || self.chunk_buf.len() + (self.read_pos - start) >= rollsum::MAX_CHUNK_SIZE
                {
                    self.chunk_buf.extend_from_slice(&self.read_buf[start..self.read_pos]);
                    let data = std::mem::replace(&mut self.chunk_buf, Vec::with_capacity(rollsum::AVERAGE_CHUNK_SIZE));
                    let id: ObjectId = <[u8; 32]>::from(Sha256::digest(&data)).into();
                    //let id: ObjectId = ObjectId([0; 32]);
                    return Some(Ok((id, data)));
                }
            }
            // No boundary in this buffer — bulk copy and refill.
            self.chunk_buf.extend_from_slice(&self.read_buf[start..self.read_pos]);
        }
    }
}

fn split_chunks<R: Read>(reader: R) -> ChunkReader<R> {
    ChunkReader {
        reader,
        rs: Rollsum::new(),
        read_buf: vec![0u8; 64 * 1024],
        read_pos: 0,
        read_len: 0,
        chunk_buf: Vec::new(),
        done: false,
    }
}

fn blob_object_id(chunk_ids: &[ObjectId]) -> ObjectId {
    let mut h = Sha256::new();
    h.update(b"blob");
    for id in chunk_ids {
        h.update(id);
    }
    <[u8; 32]>::from(h.finalize()).into()
}

fn tree_object_id(files: &BTreeMap<String, ObjectId>) -> ObjectId {
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
    h.update(&snap.tree);
    let msg = snap.message.as_deref().unwrap_or("");
    h.update(&(msg.len() as u32).to_le_bytes());
    h.update(msg.as_bytes());
    let dur = snap.date.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    h.update(&dur.as_secs().to_le_bytes());
    h.update(&dur.subsec_nanos().to_le_bytes());
    <[u8; 32]>::from(h.finalize()).into()
}

impl Repository {
    fn init(base: &Path) -> Self {
        std::fs::create_dir_all(base.join(".syncup/chunks"))
            .expect("failed to create .syncup/chunks");
        let mut repo = Repository {
            chunks: BTreeMap::new(),
            objects: BTreeMap::new(),
            head: ObjectId([0u8; 32]),
        };
        repo.snapshot(base, Some("Initial snapshot".to_string()));
        repo
    }

    fn snapshot(&mut self, base: &Path, message: Option<String>) {
        std::fs::create_dir_all(base.join(".syncup/chunks"))
            .expect("failed to create .syncup/chunks");

        // Build path -> (mtime, blob_id) from the previous snapshot's tree.
        let prev_tree: BTreeMap<String, (Option<SystemTime>, ObjectId)> = {
            if let Some(Object::Snapshot(snap)) = self.objects.get(&self.head) {
                let tree_id = snap.tree;
                if let Some(Object::Tree(tree)) = self.objects.get(&tree_id) {
                    tree.files.iter().map(|(path, &blob_id)| {
                        let mtime = match self.objects.get(&blob_id) {
                            Some(Object::Blob(blob)) => blob.modified_time,
                            _ => None,
                        };
                        (path.clone(), (mtime, blob_id))
                    }).collect()
                } else { BTreeMap::new() }
            } else { BTreeMap::new() }
        };

        let mut tree_files: BTreeMap<String, ObjectId> = BTreeMap::new();

        let mut entries: Vec<_> = Walk::new(base)
            .flatten()
            .filter(|e| e.file_type().map_or(false, |t| t.is_file()))
            .collect();
        entries.sort_by_key(|e| e.path().to_path_buf());

        for entry in tqdm!(entries.iter(), desc = "Processing files", position = 0) {
            let path = entry.path();
            let rel_path = path.to_string_lossy().into_owned();

            let metadata = std::fs::metadata(path)
                .unwrap_or_else(|_| panic!("failed to stat {path:?}"));
            let mtime = metadata.modified().ok();

            // Skip files whose mtime hasn't changed since the last snapshot.
            if let Some(&(prev_mtime, prev_blob_id)) = prev_tree.get(&rel_path) {
                if prev_mtime.is_some() && prev_mtime == mtime {
                    tree_files.insert(rel_path, prev_blob_id);
                    continue;
                }
            }

            let file = std::fs::File::open(path)
                .unwrap_or_else(|_| panic!("failed to open {path:?}"));
            let mut chunk_ids: Vec<ObjectId> = Vec::new();

            let size = metadata.len() as usize;
            let chunks = size / rollsum::AVERAGE_CHUNK_SIZE;
            for chunk in tqdm!(split_chunks(file), desc = "Chunking", total=chunks, position = 1) {
                let (id, data) = chunk.unwrap_or_else(|e| panic!("failed to read {path:?}: {e}"));
                chunk_ids.push(id);
                self.chunks.entry(id).or_insert_with(|| {
                    std::fs::write(
                        base.join(format!(".syncup/chunks/{}", to_hex(&id.0))),
                        &data,
                    ).expect("failed to write chunk");
                    Chunk
                });
            }

            let bid = blob_object_id(&chunk_ids);
            self.objects.insert(bid, Object::Blob(Blob {
                chunks: chunk_ids,
                created_time: metadata.created().ok(),
                modified_time: mtime,
                accessed_time: metadata.accessed().ok(),
                mode: 0
            }));
            tree_files.insert(rel_path, bid);
        }

        let tid = tree_object_id(&tree_files);
        self.objects.insert(tid, Object::Tree(Tree { files: tree_files }));

        let snap = Snapshot { tree: tid, message, date: SystemTime::now() };
        let sid = snapshot_object_id(&snap);
        self.objects.insert(sid, Object::Snapshot(snap));
        self.head = sid;

        println!("Snapshot: {}", to_hex(&sid.0));
        self.save(base);
    }

    fn save(&self, base: &Path) {
        let bytes = postcard::to_allocvec(self).expect("failed to serialize repository");
        std::fs::write(base.join(".syncup/repository"), bytes)
            .expect("failed to write .syncup/repository");
    }

    fn load(base: &Path) -> Self {
        let bytes = std::fs::read(base.join(".syncup/repository"))
            .expect("failed to read .syncup/repository");
        postcard::from_bytes(&bytes).expect("failed to deserialize repository")
    }
}

fn chunk_file(path: &Path) {
    let file = std::fs::File::open(path).expect("failed to open file");
    let mut offset = 0usize;

    let size = file.metadata().unwrap().len() as usize;
    let chunks = size/rollsum::AVERAGE_CHUNK_SIZE;
    for chunk in tqdm!(split_chunks(file), desc = "Chunking", total=chunks, position = 1) {
        let (_id, data) = chunk.expect("failed to read file");
        let end = offset + data.len();
        //println!("chunk [{offset}, {end}), len={}", data.len());
        offset = end;
    }
}

fn main() {
    let matches = cli().get_matches();

    match matches.subcommand() {
        Some(("init", _)) => {
            Repository::init(std::path::Path::new("."));
        }
        Some(("snapshot", sub)) => {
            let message = sub.get_one::<String>("message").cloned();
            let base = Path::new(".");
            let mut repo = Repository::load(base);
            repo.snapshot(base, message);
        }
        Some(("debug", sub)) => match sub.subcommand() {
            Some(("chunk", sub)) => {
                let path = sub.get_one::<PathBuf>("PATH").unwrap();
                chunk_file(path);
            }
            Some(("print-repo", _)) => {
                let repo: Repository = Repository::load(std::path::Path::new("."));
                println!("{:#?}", repo);
            }
            _ => unreachable!(),
        }
        Some(("discover", sub)) => {
            let timeout = *sub.get_one::<u64>("timeout").unwrap();
            discover::discover(timeout).unwrap();
        }
        Some(("serve", sub)) => {
            let port = *sub.get_one::<u16>("port").unwrap();
            tokio::runtime::Runtime::new().unwrap()
                .block_on(serve::serve(port))
                .unwrap();
        }
        _ => {}
    }
}
