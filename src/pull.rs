use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use crate::{Blob, List, Object, ObjectId, Repository, RepositoryId, protocol, scan};
use anyhow::Context;
use kdam::{Bar, BarExt};
use log::{debug, info};
use relative_path::{Component as RelativeComponent, RelativePath};

pub(crate) fn object_refs(obj: &Object) -> Vec<ObjectId> {
    match obj {
        Object::Chunk(_) => vec![],
        Object::Blob(b) => vec![b.chunks],
        Object::Map(m) => m.entries.values().copied().collect(),
        Object::List(l) => l.entries.clone(),
        Object::Snapshot(s) => {
            let mut out = Vec::with_capacity(1 + s.parents.len());
            out.push(s.tree);
            out.extend_from_slice(&s.parents);
            out
        }
    }
}

pub(crate) fn local_pull_response(base: &Path, req: protocol::Request) -> protocol::Response {
    debug!("handling local pull request from {:?}: {:?}", base, req);
    let repo = Repository::load(base);
    let repo_uuid = repo.repo_uuid;

    let ensure = |requested: RepositoryId| -> Result<Repository, String> {
        if requested != repo_uuid {
            Err(format!(
                "unknown repo_uuid: requested={}, available={}",
                requested, repo_uuid
            ))
        } else {
            Ok(repo.clone())
        }
    };

    match req {
        protocol::Request::Version => protocol::Response::Version {
            version: protocol::PROTOCOL_VERSION,
        },
        protocol::Request::PullSnapshotIds { repo_uuid } => match ensure(repo_uuid) {
            Ok(repo) => {
                let snapshot_ids = repo
                    .objects
                    .iter()
                    .filter_map(|(id, obj)| match obj {
                        Object::Snapshot(_) => Some(*id),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                debug!(
                    "local pull snapshot ids: head={}, count={}",
                    repo.head.to_hex(),
                    snapshot_ids.len()
                );
                protocol::Response::PullSnapshotIds {
                    head: repo.head,
                    snapshot_ids,
                }
            }
            Err(err) => protocol::Response::Error(err),
        },
        protocol::Request::PullObjects {
            repo_uuid,
            object_ids,
        } => match ensure(repo_uuid) {
            Ok(repo) => {
                debug!("local pull objects requested: {} ids", object_ids.len());
                let mut objects = Vec::new();
                let mut chunks = Vec::new();
                for id in object_ids {
                    if let Some(obj) = repo.objects.get(&id) {
                        objects.push((id, obj.clone()));
                        if matches!(obj, Object::Chunk(_)) {
                            let path = base.join(format!("{}/{}", crate::CHUNKS_DIR, id.to_hex()));
                            if let Ok(bytes) = std::fs::read(path) {
                                chunks.push((id, bytes));
                            }
                        }
                    }
                }
                debug!(
                    "local pull objects response: objects={}, chunks={}",
                    objects.len(),
                    chunks.len()
                );
                protocol::Response::PullObjects { objects, chunks }
            }
            Err(err) => protocol::Response::Error(err),
        },
        _ => protocol::Response::Error("unsupported local pull request".into()),
    }
}

pub(crate) trait RequestSender: Send {
    async fn send(&mut self, request: protocol::Request) -> anyhow::Result<protocol::Response>;
}

struct ChannelRequestSender<'a> {
    channel: &'a mut russh::Channel<russh::client::Msg>,
}

impl RequestSender for ChannelRequestSender<'_> {
    async fn send(&mut self, request: protocol::Request) -> anyhow::Result<protocol::Response> {
        crate::rpc(self.channel, &request).await
    }
}

async fn fetch_remote_objects<S>(
    repo_uuid: RepositoryId,
    local_object_ids: &BTreeSet<ObjectId>,
    total_objects_hint: Option<usize>,
    max_object_ids_per_pull: usize,
    sender: &mut S,
) -> anyhow::Result<(
    ObjectId,
    BTreeMap<ObjectId, Object>,
    BTreeMap<ObjectId, Vec<u8>>,
)>
where
    S: RequestSender,
{
    let response = sender
        .send(protocol::Request::PullSnapshotIds { repo_uuid })
        .await?;

    let (remote_head, snapshot_ids) = match response {
        protocol::Response::PullSnapshotIds { head, snapshot_ids } => (head, snapshot_ids),
        protocol::Response::Error(err) => anyhow::bail!("snapshot id pull failed: {err}"),
        _ => anyhow::bail!("unexpected response to PullSnapshotIds"),
    };

    debug!(
        "received snapshot ids: remote_head={}, snapshot_count={}",
        remote_head.to_hex(),
        snapshot_ids.len()
    );

    let mut need: BTreeSet<ObjectId> = snapshot_ids
        .into_iter()
        .filter(|id| !local_object_ids.contains(id))
        .collect();

    let mut pulled_count = 0usize;
    let hinted_missing = total_objects_hint
        .map(|total| total.saturating_sub(local_object_ids.len()))
        .unwrap_or(0);
    let mut pull_bar = Bar::new(need.len().max(hinted_missing).max(1));
    pull_bar.set_description("Pulling objects");

    let mut fetched_objects: BTreeMap<ObjectId, Object> = BTreeMap::new();
    let mut fetched_chunks: BTreeMap<ObjectId, Vec<u8>> = BTreeMap::new();

    while !need.is_empty() {
        let req_ids: Vec<ObjectId> = need.iter().take(max_object_ids_per_pull).copied().collect();
        for id in &req_ids {
            need.remove(id);
        }
        debug!("requesting {} objects", req_ids.len());

        let response = sender
            .send(protocol::Request::PullObjects {
                repo_uuid,
                object_ids: req_ids,
            })
            .await?;

        let (objects, chunks) = match response {
            protocol::Response::PullObjects { objects, chunks } => (objects, chunks),
            protocol::Response::Error(err) => anyhow::bail!("object pull failed: {err}"),
            _ => anyhow::bail!("unexpected response to PullObjects"),
        };

        debug!(
            "received object batch: objects={}, chunks={}",
            objects.len(),
            chunks.len()
        );

        for (id, data) in chunks {
            fetched_chunks.entry(id).or_insert(data);
        }

        for (id, obj) in objects {
            if local_object_ids.contains(&id) || fetched_objects.contains_key(&id) {
                continue;
            }
            for child in object_refs(&obj) {
                if !local_object_ids.contains(&child) && !fetched_objects.contains_key(&child) {
                    need.insert(child);
                }
            }
            fetched_objects.insert(id, obj);
            pulled_count += 1;
            let _ = pull_bar.update(1);
        }

        let expected_total = pulled_count + need.len();
        if expected_total > pull_bar.total {
            pull_bar.total = expected_total;
        }
    }

    if pull_bar.counter < pull_bar.total {
        let _ = pull_bar.update(pull_bar.total - pull_bar.counter);
    }

    debug!(
        "finished pull graph walk: fetched_objects={}, fetched_chunks={}",
        fetched_objects.len(),
        fetched_chunks.len()
    );

    Ok((remote_head, fetched_objects, fetched_chunks))
}

pub(crate) async fn fetch_and_merge_from_host(
    base: &Path,
    host: &scan::ScannedHost,
) -> anyhow::Result<()> {
    debug!("fetching and merging from host {}", host.fullname);

    let session = crate::connect_and_auth(host, base).await?;
    let mut channel = session.channel_open_session().await?;

    let total_objects_hint = match crate::rpc(&mut channel, &protocol::Request::Status).await? {
        protocol::Response::Status { object_count, .. } => Some(object_count),
        _ => None,
    };

    let local = Repository::load(base);
    let local_object_ids: BTreeSet<ObjectId> = local.objects.keys().copied().collect();

    let mut sender = ChannelRequestSender {
        channel: &mut channel,
    };
    let (remote_head, fetched_objects, fetched_chunks) = fetch_remote_objects(
        local.repo_uuid,
        &local_object_ids,
        total_objects_hint,
        2048,
        &mut sender,
    )
    .await?;

    merge_fetched_into_local(base, remote_head, fetched_objects, fetched_chunks)?;

    let _ = channel.eof().await;
    let _ = channel.close().await;
    let _ = session
        .disconnect(russh::Disconnect::ByApplication, "", "English")
        .await;

    Ok(())
}

pub(crate) async fn fetch_and_merge_with<S>(
    base: &Path,
    total_objects_hint: Option<usize>,
    sender: &mut S,
) -> anyhow::Result<()>
where
    S: RequestSender,
{
    let local = Repository::load(base);
    let local_object_ids: BTreeSet<ObjectId> = local.objects.keys().copied().collect();
    debug!(
        "starting fetch-and-merge: local_head={}, local_objects={}",
        local.head.to_hex(),
        local_object_ids.len()
    );

    let (remote_head, fetched_objects, fetched_chunks) = fetch_remote_objects(
        local.repo_uuid,
        &local_object_ids,
        total_objects_hint,
        64,
        sender,
    )
    .await?;

    merge_fetched_into_local(base, remote_head, fetched_objects, fetched_chunks)?;
    Ok(())
}

fn merge_fetched_into_local(
    base: &Path,
    remote_head: ObjectId,
    fetched_objects: BTreeMap<ObjectId, Object>,
    fetched_chunks: BTreeMap<ObjectId, Vec<u8>>,
) -> anyhow::Result<()> {
    let local = Repository::load(base);

    std::fs::create_dir_all(base.join(crate::CHUNKS_DIR))
        .expect("failed to create chunks directory");
    for (id, data) in fetched_chunks {
        let path = base.join(format!("{}/{}", crate::CHUNKS_DIR, id.to_hex()));
        if !path.exists() {
            std::fs::write(path, data).expect("failed to write chunk");
        }
    }

    let mut remote_repo = local.clone();
    for (id, obj) in fetched_objects {
        remote_repo.objects.insert(id, obj);
    }
    remote_repo.head = remote_head;

    let mut merged = Repository::load(base);
    merged.merge(remote_repo);
    merged.save(base);
    debug!("merge complete, repository saved");

    Ok(())
}

fn collect_chunk_ids(
    repo: &Repository,
    list_id: ObjectId,
    out: &mut Vec<ObjectId>,
) -> anyhow::Result<()> {
    let list = match repo.objects.get(&list_id) {
        Some(Object::List(List { entries })) => entries,
        _ => anyhow::bail!("missing list object {}", list_id.to_hex()),
    };

    for entry in list {
        match repo.objects.get(entry) {
            Some(Object::Chunk(_)) => out.push(*entry),
            Some(Object::List(_)) => collect_chunk_ids(repo, *entry, out)?,
            _ => anyhow::bail!("list entry is neither chunk nor list: {}", entry.to_hex()),
        }
    }

    Ok(())
}

fn blob_bytes(repo: &Repository, base: &Path, blob_id: ObjectId) -> anyhow::Result<Vec<u8>> {
    let blob = match repo.objects.get(&blob_id) {
        Some(Object::Blob(Blob { chunks, .. })) => *chunks,
        _ => anyhow::bail!("missing blob object {}", blob_id.to_hex()),
    };

    let mut chunk_ids = Vec::new();
    collect_chunk_ids(repo, blob, &mut chunk_ids)?;

    let mut out = Vec::new();
    for id in chunk_ids {
        let bytes = std::fs::read(base.join(format!("{}/{}", crate::CHUNKS_DIR, id.to_hex())))
            .with_context(|| format!("missing chunk {}", id.to_hex()))?;
        out.extend_from_slice(&bytes);
    }

    Ok(out)
}

fn normalized_repo_path(raw_path: &str) -> Option<String> {
    let normalized = RelativePath::new(raw_path).normalize().into_string();
    if normalized.is_empty() || normalized == "." {
        return None;
    }

    if RelativePath::new(&normalized)
        .components()
        .any(|c| matches!(c, RelativeComponent::ParentDir))
    {
        return None;
    }

    if normalized == crate::REPO_DIR_NAME || normalized.starts_with(crate::REPO_DIR_PREFIX) {
        return None;
    }

    Some(normalized)
}

fn snapshot_file_paths(
    repo: &Repository,
    snapshot_id: ObjectId,
) -> anyhow::Result<BTreeSet<String>> {
    let snap = match repo.objects.get(&snapshot_id) {
        Some(Object::Snapshot(s)) => s,
        _ => anyhow::bail!(
            "requested object is not a snapshot: {}",
            snapshot_id.to_hex()
        ),
    };

    let tree = match repo.objects.get(&snap.tree) {
        Some(Object::Map(m)) => m,
        _ => anyhow::bail!("snapshot tree is not a map"),
    };

    let mut paths = BTreeSet::new();
    for raw_path in tree.entries.keys() {
        if let Some(rel) = normalized_repo_path(raw_path) {
            paths.insert(rel);
        }
    }

    Ok(paths)
}

fn remove_file_and_empty_parents(base: &Path, rel_path: &str) -> anyhow::Result<()> {
    let full_path = RelativePath::new(rel_path).to_path(base);
    if let Ok(meta) = std::fs::symlink_metadata(&full_path) {
        if meta.file_type().is_file() || meta.file_type().is_symlink() {
            std::fs::remove_file(&full_path)?;
        }
    }

    let mut current = full_path.parent();
    while let Some(dir) = current {
        if dir == base {
            break;
        }

        match std::fs::remove_dir(dir) {
            Ok(()) => current = dir.parent(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => current = dir.parent(),
            Err(e) if e.kind() == std::io::ErrorKind::DirectoryNotEmpty => break,
            Err(_) => break,
        }
    }

    Ok(())
}

fn checkout_snapshot_from_repo(
    base: &Path,
    repo: &Repository,
    snapshot_id: ObjectId,
) -> anyhow::Result<()> {
    let snap = match repo.objects.get(&snapshot_id) {
        Some(Object::Snapshot(s)) => s,
        _ => anyhow::bail!(
            "requested object is not a snapshot: {}",
            snapshot_id.to_hex()
        ),
    };

    let tree = match repo.objects.get(&snap.tree) {
        Some(Object::Map(m)) => m,
        _ => anyhow::bail!("snapshot tree is not a map"),
    };

    for (raw_path, blob_id) in &tree.entries {
        let Some(rel_path) = normalized_repo_path(raw_path) else {
            continue;
        };

        let full_path = RelativePath::new(&rel_path).to_path(base);
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let data = blob_bytes(repo, base, *blob_id)?;
        std::fs::write(&full_path, data)?;
    }

    Ok(())
}

pub fn checkout(base: &Path, snapshot_hash: &str) -> anyhow::Result<()> {
    let trimmed = snapshot_hash.trim();
    if trimmed.len() != 64 {
        anyhow::bail!("snapshot hash must be 64 hex characters");
    }

    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&trimmed[i * 2..i * 2 + 2], 16)
            .map_err(|_| anyhow::anyhow!("invalid snapshot hash: non-hex characters"))?;
    }
    let snapshot_id = ObjectId(out);

    let mut repo = Repository::load(base);
    let previous_paths = snapshot_file_paths(&repo, repo.head)?;
    let target_paths = snapshot_file_paths(&repo, snapshot_id)?;

    for rel_path in previous_paths.difference(&target_paths) {
        remove_file_and_empty_parents(base, rel_path)?;
    }

    checkout_snapshot_from_repo(base, &repo, snapshot_id)?;
    repo.head = snapshot_id;
    repo.save(base);
    Ok(())
}

pub fn checkout_head(base: &Path) -> anyhow::Result<()> {
    let repo = Repository::load(base);
    let head_hash = repo.head.to_hex();
    checkout(base, &head_hash)
}

pub async fn clone_from_resolved_host(
    base: &Path,
    host: &scan::ScannedHost,
    repo_selector: &str,
    bare: bool,
) -> anyhow::Result<()> {
    let repo = if let Some(repo) = host.repos.iter().find(|repo| repo.root == repo_selector) {
        repo.clone()
    } else {
        let selector_lc = repo_selector.to_ascii_lowercase();
        host.repos
            .iter()
            .find(|repo| {
                let id = repo.repo_uuid.to_hex();
                id.eq_ignore_ascii_case(&selector_lc) || id.starts_with(&selector_lc)
            })
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "repo not found on host {} for selector `{}`",
                    host.fullname,
                    repo_selector
                )
            })?
    };

    let dest_name = if repo.root.is_empty() || repo.root == "<unknown>" {
        format!("repo-{}", repo.repo_uuid.to_short_hex())
    } else {
        repo.root.clone()
    };
    let dest = base.join(dest_name);
    if dest.exists() {
        let mut entries = std::fs::read_dir(&dest)?;
        if entries.next().transpose()?.is_some() {
            anyhow::bail!(
                "destination already exists and is not empty: {}",
                dest.display()
            );
        }
    } else {
        std::fs::create_dir_all(&dest)?;
    }

    info!(
        "cloning repo {} ({}) from {} into {}{}",
        repo.root,
        repo.repo_uuid,
        host.fullname,
        dest.display(),
        if bare { " [bare]" } else { "" }
    );

    let local_object_ids = BTreeSet::new();
    let session = crate::connect_and_auth(host, Path::new(".")).await?;
    let mut channel = session.channel_open_session().await?;

    let total_objects_hint = match crate::rpc(&mut channel, &protocol::Request::Status).await? {
        protocol::Response::Status { object_count, .. } => Some(object_count),
        _ => None,
    };

    let mut sender = ChannelRequestSender {
        channel: &mut channel,
    };
    let (remote_head, fetched_objects, fetched_chunks) = fetch_remote_objects(
        repo.repo_uuid,
        &local_object_ids,
        total_objects_hint,
        2048,
        &mut sender,
    )
    .await?;

    let _ = channel.eof().await;
    let _ = channel.close().await;
    let _ = session
        .disconnect(russh::Disconnect::ByApplication, "", "English")
        .await;

    std::fs::create_dir_all(dest.join(crate::CHUNKS_DIR))?;
    for (id, data) in fetched_chunks {
        std::fs::write(dest.join(format!("{}/{}", crate::CHUNKS_DIR, id.to_hex())), data)?;
    }

    let repo_data = Repository {
        repo_uuid: repo.repo_uuid,
        objects: fetched_objects,
        head: remote_head,
    };
    repo_data.save(&dest);

    if !bare {
        let head_hash = repo_data.head.to_hex();
        checkout(&dest, &head_hash)?;
    }

    info!(
        "clone complete{}: {}",
        if bare { " (bare)" } else { "" },
        dest.display()
    );
    Ok(())
}

pub async fn fetch_all(base: &Path) -> anyhow::Result<()> {
    let local = Repository::load(base);
    let hosts = scan::scan_hosts(5)?;

    for host in hosts {
        if !host
            .repos
            .iter()
            .any(|repo| repo.repo_uuid == local.repo_uuid)
        {
            continue;
        }

        match fetch_and_merge_from_host(base, &host).await {
            Ok(()) => info!("- fetched from {}", host.fullname),
            Err(err) => info!("- fetch from {} failed: {}", host.fullname, err),
        }
    }

    Ok(())
}

pub async fn pull_all(base: &Path) -> anyhow::Result<()> {
    fetch_all(base).await?;
    checkout_head(base)
}
