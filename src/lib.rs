use log::{info, warn};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

mod chunk;
mod repository;
mod rollsum;

mod model;
pub use model::{Blob, Chunk, List, Map, Object, ObjectId, Repository, Snapshot, to_hex};

mod protocol;
mod pull;
mod scan;
mod serve;

pub(crate) use pull::fetch_and_merge_with;
pub use pull::{checkout, checkout_head, fetch_all, pull_all};

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

struct ReversePullHandler {
    base: PathBuf,
    reverse_buffers: BTreeMap<russh::ChannelId, Vec<u8>>,
}

impl ReversePullHandler {
    fn new(base: PathBuf) -> Self {
        Self {
            base,
            reverse_buffers: BTreeMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl russh::client::Handler for ReversePullHandler {
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
        session: &mut russh::client::Session,
    ) -> Result<(), Self::Error> {
        if let Some(buf) = self.reverse_buffers.get_mut(&channel) {
            buf.extend_from_slice(data);

            while let Some(frame) = protocol::pop_framed_message(buf) {
                let req = match protocol::decode_request(&frame) {
                    Ok(req) => req,
                    Err(err) => {
                        let resp = protocol::Response::Error(err.to_string());
                        let bytes = protocol::encode_response(&resp)?;
                        for chunk in bytes.chunks(32 * 1024) {
                            let _ = session.data(channel, chunk.to_vec().into());
                        }
                        continue;
                    }
                };

                let response = pull::local_pull_response(&self.base, req);
                let bytes = protocol::encode_response(&response)?;
                for chunk in bytes.chunks(32 * 1024) {
                    let _ = session.data(channel, chunk.to_vec().into());
                }
            }
        }
        Ok(())
    }

    async fn channel_eof(
        &mut self,
        channel: russh::ChannelId,
        session: &mut russh::client::Session,
    ) -> Result<(), Self::Error> {
        if let Some(buf) = self.reverse_buffers.get_mut(&channel) {
            if !buf.is_empty() {
                let resp = protocol::Response::Error("incomplete framed request at EOF".into());
                let bytes = protocol::encode_response(&resp)?;
                for chunk in bytes.chunks(32 * 1024) {
                    let _ = session.data(channel, chunk.to_vec().into());
                }
                buf.clear();
            }
        }
        let _ = session.eof(channel);
        let _ = session.close(channel);
        let _ = self.reverse_buffers.remove(&channel);
        Ok(())
    }
}

pub(crate) async fn connect_and_auth(
    host: &scan::ScannedHost,
    base: &Path,
) -> anyhow::Result<russh::client::Handle<ReversePullHandler>> {
    let addr = *host
        .addrs
        .first()
        .ok_or_else(|| anyhow::anyhow!("host has no address: {}", host.fullname))?;

    let config = Arc::new(russh::client::Config {
        inactivity_timeout: Some(std::time::Duration::from_secs(5)),
        ..Default::default()
    });

    let mut session = russh::client::connect(
        config,
        (addr, host.port),
        ReversePullHandler::new(base.to_path_buf()),
    )
    .await?;

    let client_key = if let Some(path) = resolve_key_path() {
        russh_keys::load_secret_key(&path, None)
            .map_err(|e| anyhow::anyhow!("failed to load client key {}: {e}", path.display()))?
    } else {
        warn!("no SSH key found; using ephemeral client key");
        russh_keys::key::KeyPair::generate_ed25519()
            .ok_or_else(|| anyhow::anyhow!("failed to generate client key"))?
    };

    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "syncup".to_string());
    let auth_ok = session
        .authenticate_publickey(hostname, Arc::new(client_key))
        .await?;
    if !auth_ok {
        anyhow::bail!("authentication failed");
    }

    Ok(session)
}

pub(crate) async fn rpc<M>(
    channel: &mut russh::Channel<M>,
    request: &protocol::Request,
) -> anyhow::Result<protocol::Response>
where
    M: From<(russh::ChannelId, russh::ChannelMsg)> + Send + Sync + 'static,
{
    let bytes = protocol::encode_request(request)?;
    channel.data(bytes.as_slice()).await?;

    let mut raw = Vec::new();
    loop {
        if let Some(frame) = protocol::pop_framed_message(&mut raw) {
            return Ok(protocol::decode_response(&frame)?);
        }

        match channel.wait().await {
            Some(russh::ChannelMsg::Data { data }) => raw.extend_from_slice(&data),
            Some(russh::ChannelMsg::Eof) => anyhow::bail!("peer EOF before full response frame"),
            Some(_) => {}
            None => anyhow::bail!("channel closed before response"),
        }
    }
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
                to_hex(&head.0),
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
    let hosts = scan::scan_hosts(3)?;

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
