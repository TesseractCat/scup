use log::info;
use std::{
    path::{Path, PathBuf},
    sync::OnceLock,
};

mod chunk;
mod repository;
mod rollsum;
mod transport;

mod model;
pub use model::{
    Blob, Chunk, List, Map, Object, ObjectId, Repository, RepositoryId, Snapshot, to_hex,
};

mod protocol;
mod pull;
mod scan;
mod serve;

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
