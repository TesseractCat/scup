use log::info;
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::OnceLock,
};

mod chunk;
mod repository;
mod rollsum;
mod storage;
mod transport;

mod model;
pub use model::{
    Blob, Chunk, List, Map, Object, ObjectId, Repository, RepositoryId, Snapshot, to_hex,
};
pub use session::RepositorySession;

mod protocol;
mod pull;
mod scan;
mod serve;
mod session;

pub(crate) use pull::fetch_and_merge_with;
pub use pull::{checkout, checkout_head, fetch_all, pull_all};
pub(crate) use transport::ssh::{connect_and_auth, rpc};

pub const CRATE_NAME: &str = env!("CARGO_PKG_NAME");
pub const REPO_DIR_NAME: &str = concat!(".", env!("CARGO_PKG_NAME"));
pub const REPO_DIR_PREFIX: &str = concat!(".", env!("CARGO_PKG_NAME"), "/");
pub const REPOSITORY_FILE: &str = concat!(".", env!("CARGO_PKG_NAME"), "/repository");
pub const CHUNKS_DIR: &str = concat!(".", env!("CARGO_PKG_NAME"), "/chunks");
pub const MDNS_SERVICE_TYPE: &str = concat!("_", env!("CARGO_PKG_NAME"), "._tcp.local.");

static KEY_OVERRIDE: OnceLock<PathBuf> = OnceLock::new();

pub fn set_key_override(path: PathBuf) {
    let _ = KEY_OVERRIDE.set(path);
}

pub(crate) fn resolve_key_path() -> Option<PathBuf> {
    if let Some(p) = KEY_OVERRIDE.get() {
        return Some(p.clone());
    }

    let home = std::env::var("HOME").ok()?;
    let path = PathBuf::from(home).join(".ssh/id_ed25519");
    if path.exists() { Some(path) } else { None }
}

pub async fn debug_status(host_id: &str) -> anyhow::Result<()> {
    let host = scan::resolve_host(host_id, 3)?;

    let session = connect_and_auth(&host, Path::new(".")).await?;
    let mut channel = session.channel_open_session().await?;
    let response = rpc(&mut channel, &protocol::Request::Status).await?;
    let _ = channel.eof().await;
    let _ = channel.close().await;
    let _ = session
        .disconnect(russh::Disconnect::ByApplication, "", "English")
        .await;
    match response {
        protocol::Response::Status { head, object_count } => {
            info!(
                "- {} status: head={} objects={}",
                host.fullname,
                head.to_hex(),
                object_count
            );
        }
        protocol::Response::Error(err) => {
            info!("- {} error: {}", host.fullname, err);
        }
        _ => {
            info!("- {} returned an unexpected response", host.fullname);
        }
    }

    Ok(())
}

pub async fn push_all(base: &Path) -> anyhow::Result<()> {
    let local = RepositorySession::load(base)?.repository;
    let hosts = scan::scan_hosts(5)?;

    for host in hosts {
        if !host
            .repos
            .iter()
            .any(|repo| repo.repo_uuid == local.repo_uuid)
        {
            continue;
        }

        let response = async {
            let session = connect_and_auth(&host, base).await?;
            let mut channel = session.channel_open_session().await?;
            let response = rpc(
                &mut channel,
                &protocol::Request::Push {
                    repo_uuid: local.repo_uuid,
                },
            )
            .await;
            let _ = channel.eof().await;
            let _ = channel.close().await;
            let _ = session
                .disconnect(russh::Disconnect::ByApplication, "", "English")
                .await;
            response
        }
        .await;

        match response {
            Ok(protocol::Response::PushComplete) => {
                info!("- pushed to {}", host.fullname)
            }
            Ok(protocol::Response::Error(err)) => {
                info!("- push to {} failed: {}", host.fullname, err)
            }
            Ok(_) => info!("- {} returned unexpected response to push", host.fullname),
            Err(err) => info!("- push to {} failed: {}", host.fullname, err),
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

pub async fn clone_from(host_id: &str, repo_selector: &str, bare: bool) -> anyhow::Result<()> {
    let host = scan::resolve_host(host_id, 3)?;
    pull::clone_from_resolved_host(Path::new("."), &host, repo_selector, bare).await
}

pub fn debug_chunk_file(path: &Path) {
    chunk::debug_chunk_file(path);
}

pub async fn status(base: &Path) -> anyhow::Result<()> {
    use ignore::Walk;
    use relative_path::RelativePath;

    let session = RepositorySession::load(base)?;
    let repo = &session.repository;
    info!("head: {}", repo.head.to_hex());

    let tracked: std::collections::BTreeMap<String, ObjectId> = match repo.objects.get(&repo.head) {
        Some(Object::Snapshot(snap)) => match repo.objects.get(&snap.tree) {
            Some(Object::Map(map)) => map.entries.clone(),
            _ => std::collections::BTreeMap::new(),
        },
        _ => std::collections::BTreeMap::new(),
    };

    let mut seen = BTreeSet::new();
    let mut changed = Vec::new();
    let mut untracked = Vec::new();

    for entry in Walk::new(base).flatten().filter(|e| e.file_type().map_or(false, |t| t.is_file())) {
        let path = entry.path();
        let Some(rel) = path.strip_prefix(base).ok() else { continue; };
        let Some(rel_path) = RelativePath::from_path(rel).ok().map(|p| p.normalize().into_string()) else { continue; };
        if rel_path == REPO_DIR_NAME || rel_path.starts_with(REPO_DIR_PREFIX) {
            continue;
        }
        seen.insert(rel_path.clone());

        match tracked.get(&rel_path) {
            Some(blob_id) => {
                let on_disk_mtime = std::fs::metadata(path).ok().and_then(|m| m.modified().ok());
                let tracked_mtime = match repo.objects.get(blob_id) {
                    Some(Object::Blob(blob)) => blob.modified_time,
                    _ => None,
                };
                if tracked_mtime.is_some() && tracked_mtime != on_disk_mtime {
                    changed.push(rel_path);
                }
            }
            None => untracked.push(rel_path),
        }
    }

    for path in tracked.keys() {
        if !seen.contains(path) {
            changed.push(path.clone());
        }
    }

    changed.sort();
    changed.dedup();
    untracked.sort();
    untracked.dedup();

    if changed.is_empty() && untracked.is_empty() {
        info!("working tree clean");
    } else {
        for p in &changed {
            info!("changed: {}", p);
        }
        for p in &untracked {
            info!("untracked: {}", p);
        }
    }

    let hosts = scan::scan_hosts(3)?;
    if hosts.is_empty() {
        info!("no remote servers found");
        return Ok(());
    }

    for host in hosts {
        if !host.repos.iter().any(|r| r.repo_uuid == repo.repo_uuid) {
            continue;
        }

        let remote = async {
            let ssh = connect_and_auth(&host, base).await?;
            let mut channel = ssh.channel_open_session().await?;
            let response = rpc(&mut channel, &protocol::Request::Status).await;
            let _ = channel.eof().await;
            let _ = channel.close().await;
            let _ = ssh
                .disconnect(russh::Disconnect::ByApplication, "", "English")
                .await;
            response
        }
        .await;

        match remote {
            Ok(protocol::Response::Status { head, .. }) => {
                let state = if head == repo.head { "up-to-date" } else { "out-of-date" };
                info!("remote {}: {} (head={})", host.fullname, state, head.to_hex());
            }
            Ok(protocol::Response::Error(err)) => {
                info!("remote {}: error {}", host.fullname, err);
            }
            Ok(_) => info!("remote {}: unexpected response", host.fullname),
            Err(err) => info!("remote {}: unreachable ({})", host.fullname, err),
        }
    }

    Ok(())
}
