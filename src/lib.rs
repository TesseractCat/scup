use ignore::Walk;
use kdam::tqdm;
use postcard;
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

mod rollsum;
use rollsum::Rollsum;

mod model;
pub use model::{Blob, Chunk, List, Map, Object, ObjectId, Repository, Snapshot, to_hex};

mod scan;
mod protocol;
mod serve;
mod pull;

pub use pull::pull_all;
pub(crate) use pull::pull_and_merge_with;

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
    type Item = std::io::Result<(ObjectId, Vec<u8>, u64)>;

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
                        let data = std::mem::replace(
                            &mut self.chunk_buf,
                            Vec::with_capacity(rollsum::AVERAGE_CHUNK_SIZE),
                        );
                        let id: ObjectId = <[u8; 32]>::from(Sha256::digest(&data)).into();
                        Some(Ok((id, data, self.rs.digest())))
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
                    self.chunk_buf
                        .extend_from_slice(&self.read_buf[start..self.read_pos]);
                    let data = std::mem::replace(
                        &mut self.chunk_buf,
                        Vec::with_capacity(rollsum::AVERAGE_CHUNK_SIZE),
                    );
                    let id: ObjectId = <[u8; 32]>::from(Sha256::digest(&data)).into();
                    //let id: ObjectId = ObjectId([0; 32]);
                    return Some(Ok((id, data, self.rs.digest())));
                }
            }
            // No boundary in this buffer — bulk copy and refill.
            self.chunk_buf
                .extend_from_slice(&self.read_buf[start..self.read_pos]);
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

fn list_object_id(entries: &[ObjectId]) -> ObjectId {
    let mut h = Sha256::new();
    h.update(b"list");
    for id in entries {
        h.update(id);
    }
    <[u8; 32]>::from(h.finalize()).into()
}

fn blob_object_id(list_id: ObjectId) -> ObjectId {
    let mut h = Sha256::new();
    h.update(b"blob");
    h.update(list_id);
    <[u8; 32]>::from(h.finalize()).into()
}

fn map_object_id(files: &BTreeMap<String, ObjectId>) -> ObjectId {
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

fn build_fanout_list(repo: &mut Repository, leaves: &[(ObjectId, u64)]) -> ObjectId {
    if leaves.is_empty() {
        let id = list_object_id(&[]);
        repo.objects
            .entry(id)
            .or_insert_with(|| Object::List(List { entries: vec![] }));
        return id;
    }

    if leaves.len() == 1 {
        let id = list_object_id(&[leaves[0].0]);
        repo.objects.entry(id).or_insert_with(|| {
            Object::List(List {
                entries: vec![leaves[0].0],
            })
        });
        return id;
    }

    let mut level: Vec<(ObjectId, u64)> = leaves.to_vec();

    for (fan_level, mask) in rollsum::FAN_MASKS.iter().enumerate() {
        if level.len() <= 1 {
            break;
        }

        let mut grouped: Vec<(ObjectId, u64)> = Vec::new();
        let mut bucket: Vec<ObjectId> = Vec::new();

        for (id, digest) in &level {
            bucket.push(*id);
            if (*digest & *mask) == *mask {
                let list_id = list_object_id(&bucket);
                repo.objects
                    .entry(list_id)
                    .or_insert_with(|| Object::List(List { entries: bucket.clone() }));
                grouped.push((list_id, *digest));
                bucket.clear();
            }
        }

        if !bucket.is_empty() {
            let list_id = list_object_id(&bucket);
            repo.objects
                .entry(list_id)
                .or_insert_with(|| Object::List(List { entries: bucket.clone() }));
            let digest = level.last().map(|(_, d)| *d).unwrap_or(0);
            grouped.push((list_id, digest));
        }

        level = grouped;

        if level.len() == 1 {
            break;
        }

        if fan_level == rollsum::FAN_MASKS.len() - 1 {
            break;
        }
    }

    while level.len() > 1 {
        let ids: Vec<ObjectId> = level.iter().map(|(id, _)| *id).collect();
        let top_id = list_object_id(&ids);
        repo.objects
            .entry(top_id)
            .or_insert_with(|| Object::List(List { entries: ids }));
        let digest = level.last().map(|(_, d)| *d).unwrap_or(0);
        level = vec![(top_id, digest)];
    }

    level[0].0
}

fn snapshot_object_id(snap: &Snapshot) -> ObjectId {
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

impl Repository {
    fn map_entries_for_snapshot(&self, snapshot_id: ObjectId) -> BTreeMap<String, ObjectId> {
        let Some(Object::Snapshot(snap)) = self.objects.get(&snapshot_id) else {
            return BTreeMap::new();
        };
        let Some(Object::Map(tree)) = self.objects.get(&snap.tree) else {
            return BTreeMap::new();
        };
        tree.entries.clone()
    }

    fn blob_modified_time(&self, blob_id: ObjectId) -> Option<SystemTime> {
        match self.objects.get(&blob_id) {
            Some(Object::Blob(blob)) => blob.modified_time,
            _ => None,
        }
    }

    pub(crate) fn merge(&mut self, incoming: Repository) {
        if self.repo_uuid != incoming.repo_uuid {
            return;
        }

        let local_head = self.head;
        let remote_head = incoming.head;

        if local_head == remote_head {
            return;
        }

        for (id, obj) in incoming.objects {
            self.objects.insert(id, obj);
        }

        let local_files = self.map_entries_for_snapshot(local_head);
        let remote_files = self.map_entries_for_snapshot(remote_head);

        let mut merged_files: BTreeMap<String, ObjectId> = BTreeMap::new();
        let mut all_paths: BTreeMap<String, ()> = BTreeMap::new();
        for path in local_files.keys() {
            all_paths.insert(path.clone(), ());
        }
        for path in remote_files.keys() {
            all_paths.insert(path.clone(), ());
        }

        for (path, _) in all_paths {
            match (local_files.get(&path), remote_files.get(&path)) {
                (Some(&l), Some(&r)) if l == r => {
                    merged_files.insert(path, l);
                }
                (Some(&l), Some(&r)) => {
                    let l_mt = self.blob_modified_time(l);
                    let r_mt = self.blob_modified_time(r);
                    let chosen = match (l_mt, r_mt) {
                        (Some(lt), Some(rt)) => {
                            if rt > lt {
                                r
                            } else {
                                l
                            }
                        }
                        (Some(_), None) => l,
                        (None, Some(_)) => r,
                        (None, None) => r,
                    };
                    merged_files.insert(path, chosen);
                }
                (Some(&l), None) => {
                    merged_files.insert(path, l);
                }
                (None, Some(&r)) => {
                    merged_files.insert(path, r);
                }
                (None, None) => {}
            }
        }

        let merged_map_id = map_object_id(&merged_files);
        self.objects
            .insert(merged_map_id, Object::Map(Map { entries: merged_files }));

        let mut parents = vec![local_head, remote_head];
        parents.sort();
        parents.dedup();

        let merged_snapshot = Snapshot {
            parents,
            tree: merged_map_id,
            message: Some("Merge".to_string()),
            date: SystemTime::now(),
        };
        let merged_snapshot_id = snapshot_object_id(&merged_snapshot);
        self.objects
            .insert(merged_snapshot_id, Object::Snapshot(merged_snapshot));
        self.head = merged_snapshot_id;
    }

    pub fn init(base: &Path) -> Self {
        std::fs::create_dir_all(base.join(".syncup/chunks"))
            .expect("failed to create .syncup/chunks");
        let mut repo_uuid = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut repo_uuid);

        let mut repo = Repository {
            repo_uuid,
            objects: BTreeMap::new(),
            head: ObjectId([0u8; 32]),
        };
        repo.snapshot(base, Some("Initial snapshot".to_string()));
        println!("Repository UUID: {}", to_hex(&repo.repo_uuid));
        repo
    }

    pub fn snapshot(&mut self, base: &Path, message: Option<String>) {
        std::fs::create_dir_all(base.join(".syncup/chunks"))
            .expect("failed to create .syncup/chunks");

        // Build path -> (mtime, blob_id) from the previous snapshot's tree.
        let prev_tree: BTreeMap<String, (Option<SystemTime>, ObjectId)> = {
            if let Some(Object::Snapshot(snap)) = self.objects.get(&self.head) {
                let tree_id = snap.tree;
                if let Some(Object::Map(tree)) = self.objects.get(&tree_id) {
                    tree.entries
                        .iter()
                        .map(|(path, &blob_id)| {
                            let mtime = match self.objects.get(&blob_id) {
                                Some(Object::Blob(blob)) => blob.modified_time,
                                _ => None,
                            };
                            (path.clone(), (mtime, blob_id))
                        })
                        .collect()
                } else {
                    BTreeMap::new()
                }
            } else {
                BTreeMap::new()
            }
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

            let metadata =
                std::fs::metadata(path).unwrap_or_else(|_| panic!("failed to stat {path:?}"));
            let mtime = metadata.modified().ok();

            // Skip files whose mtime hasn't changed since the last snapshot.
            if let Some(&(prev_mtime, prev_blob_id)) = prev_tree.get(&rel_path) {
                if prev_mtime.is_some() && prev_mtime == mtime {
                    tree_files.insert(rel_path, prev_blob_id);
                    continue;
                }
            }

            let file =
                std::fs::File::open(path).unwrap_or_else(|_| panic!("failed to open {path:?}"));
            let mut chunk_leaves: Vec<(ObjectId, u64)> = Vec::new();

            let size = metadata.len() as usize;
            let chunks = size / rollsum::AVERAGE_CHUNK_SIZE;
            for chunk in tqdm!(
                split_chunks(file),
                desc = "Chunking",
                total = chunks,
                position = 1
            ) {
                let (id, data, digest) =
                    chunk.unwrap_or_else(|e| panic!("failed to read {path:?}: {e}"));
                chunk_leaves.push((id, digest));
                self.objects.entry(id).or_insert_with(|| {
                    std::fs::write(
                        base.join(format!(".syncup/chunks/{}", to_hex(&id.0))),
                        &data,
                    )
                    .expect("failed to write chunk");
                    Object::Chunk(Chunk)
                });
            }

            let list_id = build_fanout_list(self, &chunk_leaves);
            let bid = blob_object_id(list_id);
            self.objects.insert(
                bid,
                Object::Blob(Blob {
                    chunks: list_id,
                    created_time: metadata.created().ok(),
                    modified_time: mtime,
                    accessed_time: metadata.accessed().ok(),
                    mode: 0,
                }),
            );
            tree_files.insert(rel_path, bid);
        }

        let tid = map_object_id(&tree_files);
        self.objects
            .insert(tid, Object::Map(Map { entries: tree_files }));

        let snap = Snapshot {
            parents: if self.head.0.iter().all(|x| *x == 0) { vec![] } else { vec![self.head]},
            tree: tid,
            message,
            date: SystemTime::now(),
        };
        let sid = snapshot_object_id(&snap);
        self.objects.insert(sid, Object::Snapshot(snap));
        self.head = sid;

        println!("Snapshot: {}", to_hex(&sid.0));
        self.save(base);
    }

    pub fn save(&self, base: &Path) {
        let bytes = postcard::to_allocvec(self).expect("failed to serialize repository");
        std::fs::write(base.join(".syncup/repository"), bytes)
            .expect("failed to write .syncup/repository");
    }

    pub fn load(base: &Path) -> Self {
        let bytes = std::fs::read(base.join(".syncup/repository"))
            .expect("failed to read .syncup/repository");
        postcard::from_bytes(&bytes).expect("failed to deserialize repository")
    }
}

fn chunk_file(path: &Path) {
    let file = std::fs::File::open(path).expect("failed to open file");
    let mut offset = 0usize;

    let size = file.metadata().unwrap().len() as usize;
    let chunks = size / rollsum::AVERAGE_CHUNK_SIZE;
    for chunk in tqdm!(
        split_chunks(file),
        desc = "Chunking",
        total = chunks,
        position = 1
    ) {
        let (_id, data, _digest) = chunk.expect("failed to read file");
        let end = offset + data.len();
        //println!("chunk [{offset}, {end}), len={}", data.len());
        offset = end;
    }
}

struct DebugStatusClient {
    base: PathBuf,
    reverse_buffers: BTreeMap<russh::ChannelId, Vec<u8>>,
}

impl DebugStatusClient {
    fn new(base: PathBuf) -> Self {
        Self {
            base,
            reverse_buffers: BTreeMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl russh::client::Handler for DebugStatusClient {
    type Error = anyhow::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &russh::keys::key::PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }

    async fn server_channel_open_session(
        &mut self,
        channel: russh::ChannelId,
        _session: &mut russh::client::Session,
    ) -> Result<(), Self::Error> {
        self.reverse_buffers.entry(channel).or_default();
        Ok(())
    }

    async fn data(
        &mut self,
        channel: russh::ChannelId,
        data: &[u8],
        _session: &mut russh::client::Session,
    ) -> Result<(), Self::Error> {
        if let Some(buf) = self.reverse_buffers.get_mut(&channel) {
            buf.extend_from_slice(data);
        }
        Ok(())
    }

    async fn channel_eof(
        &mut self,
        channel: russh::ChannelId,
        session: &mut russh::client::Session,
    ) -> Result<(), Self::Error> {
        let Some(buf) = self.reverse_buffers.remove(&channel) else {
            return Ok(());
        };

        let req: protocol::Request = postcard::from_bytes(&buf)?;
        let response = pull::local_pull_response(&self.base, req);
        let bytes = postcard::to_allocvec(&response)?;
        let _ = session.data(channel, bytes.into());
        let _ = session.close(channel);
        Ok(())
    }
}

pub(crate) async fn connect_and_auth(
    host: &scan::ScannedHost,
    base: &Path,
) -> anyhow::Result<russh::client::Handle<DebugStatusClient>> {
    let addr = *host
        .addrs
        .first()
        .ok_or_else(|| anyhow::anyhow!("host has no address: {}", host.fullname))?;

    let config = Arc::new(russh::client::Config {
        inactivity_timeout: Some(std::time::Duration::from_secs(5)),
        ..Default::default()
    });

    let mut session =
        russh::client::connect(config, (addr, host.port), DebugStatusClient::new(base.to_path_buf()))
            .await?;
    let auth_ok = session
        .authenticate_publickey(
            "syncup",
            Arc::new(
                russh_keys::key::KeyPair::generate_ed25519()
                    .ok_or_else(|| anyhow::anyhow!("failed to generate client key"))?,
            ),
        )
        .await?;
    if !auth_ok {
        anyhow::bail!("authentication failed");
    }

    Ok(session)
}

pub(crate) async fn rpc(
    host: &scan::ScannedHost,
    command: &str,
    request: Option<&protocol::Request>,
    base: &Path,
) -> anyhow::Result<protocol::Response> {
    let session = connect_and_auth(host, base).await?;

    let mut channel = session.channel_open_session().await?;
    channel.exec(false, command).await?;

    if let Some(req) = request {
        let bytes = postcard::to_allocvec(req)?;
        channel.data(bytes.as_slice()).await?;
        channel.eof().await?;
    }

    let mut raw = Vec::new();
    while let Some(msg) = channel.wait().await {
        if let russh::ChannelMsg::Data { data } = msg {
            raw.extend_from_slice(&data);
        }
    }
    let _ = channel.close().await;

    let _ = session
        .disconnect(russh::Disconnect::ByApplication, "", "English")
        .await;

    if raw.is_empty() {
        anyhow::bail!("empty response from host");
    }

    Ok(postcard::from_bytes(&raw)?)
}

pub async fn debug_status(host_id: &str) -> anyhow::Result<()> {
    let host = scan::resolve_host(host_id, 3)?;

    let response = rpc(&host, "status", None, Path::new(".")).await?;
    match response {
        protocol::Response::Status { head } => {
            println!("- {} status: head={}", host.fullname, to_hex(&head.0));
        }
        protocol::Response::Error(err) => {
            println!("- {} error: {}", host.fullname, err);
        }
        _ => {
            println!("- {} returned an unexpected response", host.fullname);
        }
    }

    Ok(())
}

pub async fn push_all(base: &Path) -> anyhow::Result<()> {
    let local = Repository::load(base);
    let hosts = scan::scan_hosts(3)?;

    for host in hosts {
        if !host.repo_uuids.iter().any(|id| id == &local.repo_uuid) {
            continue;
        }

        let response = rpc(
            &host,
            "push",
            Some(&protocol::Request::Push {
                repo_uuid: local.repo_uuid,
            }),
            base,
        )
        .await;

        match response {
            Ok(protocol::Response::PushComplete) => {
                println!("- pushed to {}", host.fullname)
            }
            Ok(protocol::Response::Error(err)) => {
                println!("- push to {} failed: {}", host.fullname, err)
            }
            Ok(_) => println!("- {} returned unexpected response to push", host.fullname),
            Err(err) => println!("- push to {} failed: {}", host.fullname, err),
        }
    }

    Ok(())
}

pub fn scan(timeout_secs: u64) -> anyhow::Result<()> {
    scan::scan(timeout_secs)
}

pub async fn serve_on(port: u16) -> anyhow::Result<()> {
    serve::serve(port).await
}

pub fn debug_chunk_file(path: &Path) {
    chunk_file(path);
}
