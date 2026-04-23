use anyhow::{Context, Result};
use std::collections::HashMap;
use async_trait::async_trait;
use log::{debug, error, info, warn};
use mdns_sd::{ServiceDaemon, ServiceInfo};
use russh::ChannelId;
use russh::server::{Auth, Handle, Handler, Server, Session};
use russh_keys::key::KeyPair;
use std::{path::PathBuf, sync::Arc};

use crate::protocol::{Request, Response};
use crate::{Object, ObjectId, Repository, to_hex};

// ── Server / handler boilerplate ─────────────────────────────────────────────

struct SyncupServer;

impl Server for SyncupServer {
    type Handler = ConnectionHandler;
    fn new_client(&mut self, _addr: Option<std::net::SocketAddr>) -> Self::Handler {
        ConnectionHandler {
            pending_request: None,
            pending_command: None,
        }
    }
}

/// Per-connection state. `pending_request` is `Some` while accumulating request payload bytes.
struct ConnectionHandler {
    pending_request: Option<Vec<u8>>,
    pending_command: Option<String>,
}

#[async_trait]
impl Handler for ConnectionHandler {
    type Error = anyhow::Error;

    async fn auth_publickey(
        &mut self,
        _user: &str,
        _public_key: &russh_keys::key::PublicKey,
    ) -> Result<Auth> {
info!("credentials: {_user}, {_public_key:?}");
        // Accept any key that completes the SSH handshake.
        // Add authorized-keys enforcement here when needed.
        Ok(Auth::Accept)
    }

    async fn channel_open_session(
        &mut self,
        _channel: russh::Channel<russh::server::Msg>,
        _session: &mut Session,
    ) -> Result<bool> {
        Ok(true)
    }

    /// Dispatches `status`, `pull`, and `push` SSH exec commands.
    /// Status replies immediately; pull/push wait for `channel_eof` so we can parse request payload.
    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<()> {
        let command = std::str::from_utf8(data)?;
        debug!("exec request on channel {:?}: {}", channel, command);
        match command {
            "status" => send_response(channel, session.handle(), &handle_status()?).await?,
            "pull" | "push" => {
                self.pending_command = Some(command.to_string());
                self.pending_request = Some(Vec::new());
                if data.is_empty() {
                    return Ok(());
                }
            }
            cmd => {
                send_response(
                    channel,
                    session.handle(),
                    &Response::Error(format!("unknown command: {cmd}")),
                ).await?;
            }
        }
        Ok(())
    }

    /// Accumulates incoming push data.
    async fn data(
        &mut self,
        _channel: ChannelId,
        data: &[u8],
        _session: &mut Session,
    ) -> Result<()> {
        if let Some(buf) = &mut self.pending_request {
            buf.extend_from_slice(data);
            debug!("buffered {} bytes of request payload", data.len());
        }
        Ok(())
    }

    /// Client closed its write side - process any buffered request payload.
    async fn channel_eof(&mut self, channel: ChannelId, session: &mut Session) -> Result<()> {
        if let Some(buf) = self.pending_request.take() {
            if buf.is_empty() {
                return Ok(());
            }

            let command = self.pending_command.take().unwrap_or_default();
            debug!(
                "channel eof on {:?}: command={}, payload_bytes={}",
                channel,
                command,
                buf.len()
            );
            match crate::protocol::decode_request(&buf)? {
                Request::Push { repo_uuid } if command == "push" => {
                    let handle = session.handle();
                    tokio::spawn(async move {
                        let response = match handle_push_by_pulling(repo_uuid, handle.clone()).await {
                            Ok(r) => r,
                            Err(e) => Response::Error(e.to_string()),
                        };
                        let _ = send_response(channel, handle, &response).await;
                    });
                }
                Request::PullSnapshotIds { repo_uuid } if command == "pull" => {
                    let response = handle_pull_snapshot_ids(repo_uuid)?;
                    let handle = session.handle();
                    tokio::spawn(async move {
                        let _ = send_response(channel, handle, &response).await;
                    });
                }
                Request::PullObjects {
                    repo_uuid,
                    object_ids,
                } if command == "pull" => {
                    let response = handle_pull_objects(repo_uuid, &object_ids)?;
                    let handle = session.handle();
                    tokio::spawn(async move {
                        let _ = send_response(channel, handle, &response).await;
                    });
                }
                _ => {
                    let response = Response::Error("unexpected request for channel command".into());
                    let handle = session.handle();
                    tokio::spawn(async move {
                        let _ = send_response(channel, handle, &response).await;
                    });
                }
            }
        }
        Ok(())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn send_response(channel: ChannelId, handle: Handle, response: &Response) -> Result<()> {
    let bytes = crate::protocol::encode_response(response)?;
    for chunk in bytes.chunks(32 * 1024) {
        let _ = handle.data(channel, chunk.to_vec().into()).await;
    }
    let _ = handle.close(channel).await;
    Ok(())
}

fn load_repository() -> Result<Repository> {
    let bytes = std::fs::read(".syncup/repository")
        .context("repository not found — run `syncup init` first")?;
    Ok(postcard::from_bytes(&bytes)?)
}

// ── Request handlers ──────────────────────────────────────────────────────────

fn handle_status() -> Result<Response> {
    Ok(Response::Status {
        head: load_repository()?.head,
    })
}

fn ensure_repo(repo_uuid: [u8; 32]) -> Result<Repository> {
    let repo = load_repository()?;
    if repo.repo_uuid != repo_uuid {
        anyhow::bail!(
            "unknown repo_uuid: requested={}, available={}",
            to_hex(&repo_uuid),
            to_hex(&repo.repo_uuid)
        );
    }
    Ok(repo)
}

fn handle_pull_snapshot_ids(repo_uuid: [u8; 32]) -> Result<Response> {
    debug!("serve: pull snapshot ids for repo {}", to_hex(&repo_uuid));
    let repo = ensure_repo(repo_uuid)?;
    let snapshot_ids = repo
        .objects
        .iter()
        .filter_map(|(id, obj)| match obj {
            Object::Snapshot(_) => Some(*id),
            _ => None,
        })
        .collect::<Vec<_>>();

    debug!(
        "serve: snapshot ids response head={}, count={}",
        to_hex(&repo.head.0),
        snapshot_ids.len()
    );
    Ok(Response::PullSnapshotIds {
        head: repo.head,
        snapshot_ids,
    })
}

fn handle_pull_objects(repo_uuid: [u8; 32], object_ids: &[ObjectId]) -> Result<Response> {
    debug!(
        "serve: pull objects for repo {}, requested={}",
        to_hex(&repo_uuid),
        object_ids.len()
    );
    let repo = ensure_repo(repo_uuid)?;
    let mut objects = Vec::new();
    let mut chunks = Vec::new();

    for id in object_ids {
        if let Some(obj) = repo.objects.get(id) {
            objects.push((*id, obj.clone()));
            if matches!(obj, Object::Chunk(_)) {
                let data = std::fs::read(format!(".syncup/chunks/{}", to_hex(&id.0)))
                    .with_context(|| format!("missing chunk {}", to_hex(&id.0)))?;
                chunks.push((*id, data));
            }
        }
    }

    debug!(
        "serve: pull objects response objects={}, chunks={}",
        objects.len(),
        chunks.len()
    );
    Ok(Response::PullObjects { objects, chunks })
}

async fn handle_push_by_pulling(repo_uuid: [u8; 32], handle: Handle) -> Result<Response> {
    let _local = ensure_repo(repo_uuid)?;
    let base = std::path::Path::new(".");

    info!("push-triggered pull for repo {}", to_hex(&repo_uuid));
    crate::pull_and_merge_with(base, |req| {
        let handle = handle.clone();
        async move { crate::rpc(&handle, req).await }
    })
    .await?;
    info!("push-triggered pull complete for repo {}", to_hex(&repo_uuid));

    Ok(Response::PushComplete)
}

// ── Key loading ───────────────────────────────────────────────────────────────

fn load_host_key() -> Result<KeyPair> {
    // Prefer the user's own SSH identity key so the host fingerprint is stable.
    let home = std::env::var("HOME").context("HOME not set")?;
    let ssh_path = PathBuf::from(home).join(".ssh/id_ed25519");
    if ssh_path.exists() {
        return russh_keys::load_secret_key(&ssh_path, None)
            .context("failed to load ~/.ssh/id_ed25519");
    }

    // russh-keys has no key-write API, so generate an ephemeral key and warn.
    warn!(
        "~/.ssh/id_ed25519 not found; using an ephemeral host key (fingerprint changes on restart)"
    );
    KeyPair::generate_ed25519().context("key generation failed")
}

// ── mDNS server scan ────────────────────────────────────────────────────────

const MDNS_SERVICE_TYPE: &str = "_syncup._tcp.local.";

fn advertise_mdns(
    port: u16,
    repo_uuids: &[[u8; 32]],
    repo_roots: &[String],
) -> Result<ServiceDaemon> {
    let mdns = ServiceDaemon::new().context("failed to start mDNS daemon")?;

    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "syncup".to_string());
    let instance_name = format!("syncup-{hostname}");
    let host_name = format!("{hostname}.local.");

    let mut properties = HashMap::new();
    let repo_list = repo_uuids
        .iter()
        .map(|id| to_hex(id))
        .collect::<Vec<_>>()
        .join(",");
    properties.insert("repos".to_string(), repo_list);
    properties.insert("roots".to_string(), repo_roots.join(","));

    let service = ServiceInfo::new(
        MDNS_SERVICE_TYPE,
        &instance_name,
        &host_name,
        "",
        port,
        properties,
    )
    .context("failed to build mDNS service info")?
    .enable_addr_auto();

    mdns.register(service)
        .context("failed to register mDNS service")?;

    info!(
        "mDNS advertised: {instance_name}.{MDNS_SERVICE_TYPE} -> {host_name}:{port} repos={} roots={}",
        repo_uuids.len(),
        repo_roots.join(",")
    );
    Ok(mdns)
}

// ── Entry point ───────────────────────────────────────────────────────────────

fn current_repo_root_name() -> Option<String> {
    std::env::current_dir()
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
}

pub async fn serve(port: u16) -> Result<()> {
    let config = Arc::new(russh::server::Config {
        // RFC 4253: identification string must start with "SSH-2.0-".
        server_id: russh::SshId::Standard("SSH-2.0-syncup-ssh".into()),
        keys: vec![load_host_key()?],
        ..Default::default()
    });

    let repo_uuids = load_repository()
        .map(|r| vec![r.repo_uuid])
        .unwrap_or_else(|_| Vec::new());
    let repo_roots = if repo_uuids.is_empty() {
        Vec::new()
    } else {
        vec![current_repo_root_name().unwrap_or_else(|| "<unknown>".to_string())]
    };

    let _mdns = advertise_mdns(port, &repo_uuids, &repo_roots)?;

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    info!("Listening on 0.0.0.0:{port}");

    let mut server = SyncupServer;
    loop {
        let (stream, addr) = listener.accept().await?;
        debug!("accepted TCP connection from {addr}");
        let config = config.clone();
        let handler = server.new_client(Some(addr));
        tokio::spawn(async move {
            let session = match russh::server::run_stream(config, stream, handler).await {
                Ok(s) => s,
                Err(e) => {
                    error!("Handshake error [{addr}]: {e:#}");
                    return;
                }
            };
            if let Err(e) = session.await {
                error!("Session error [{addr}]: {e:#}");
            }
        });
    }
}
