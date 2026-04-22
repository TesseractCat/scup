use anyhow::{Context, Result};
use std::collections::HashMap;
use async_trait::async_trait;
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
        println!("credentials: {_user}, {_public_key:?}");
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
        match std::str::from_utf8(data)? {
            "status" => send_response(channel, session, &handle_status()?)?,
            "pull" | "push" => {
                self.pending_command = Some(std::str::from_utf8(data)?.to_string());
                self.pending_request = Some(Vec::new());
                if data.is_empty() {
                    return Ok(());
                }
            }
            cmd => {
                send_response(
                    channel,
                    session,
                    &Response::Error(format!("unknown command: {cmd}")),
                )?;
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
        }
        Ok(())
    }

    /// Client closed its write side — process any buffered request payload.
    async fn channel_eof(&mut self, channel: ChannelId, session: &mut Session) -> Result<()> {
        if let Some(buf) = self.pending_request.take() {
            if buf.is_empty() {
                return Ok(());
            }

            let command = self.pending_command.take().unwrap_or_default();
            match postcard::from_bytes::<Request>(&buf)? {
                Request::Push { repo_uuid } if command == "push" => {
                    let handle = session.handle();
                    tokio::spawn(async move {
                        let response = match handle_push_by_pulling(repo_uuid, handle.clone()).await {
                            Ok(r) => r,
                            Err(e) => Response::Error(e.to_string()),
                        };
                        let _ = send_response_handle(channel, handle, &response).await;
                    });
                }
                Request::PullSnapshotIds { repo_uuid } if command == "pull" => {
                    let response = handle_pull_snapshot_ids(repo_uuid)?;
                    send_response(channel, session, &response)?;
                }
                Request::PullObjects {
                    repo_uuid,
                    object_ids,
                } if command == "pull" => {
                    let response = handle_pull_objects(repo_uuid, &object_ids)?;
                    send_response(channel, session, &response)?;
                }
                _ => {
                    let response = Response::Error("unexpected request for channel command".into());
                    send_response(channel, session, &response)?;
                }
            }
        }
        Ok(())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn send_response(channel: ChannelId, session: &mut Session, response: &Response) -> Result<()> {
    let bytes = postcard::to_allocvec(response)?;
    session.data(channel, bytes.into());
    session.close(channel);
    Ok(())
}

async fn send_response_handle(channel: ChannelId, handle: Handle, response: &Response) -> Result<()> {
    let bytes = postcard::to_allocvec(response)?;
    let _ = handle.data(channel, bytes.into()).await;
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
    let repo = ensure_repo(repo_uuid)?;
    let snapshot_ids = repo
        .objects
        .iter()
        .filter_map(|(id, obj)| match obj {
            Object::Snapshot(_) => Some(*id),
            _ => None,
        })
        .collect::<Vec<_>>();

    Ok(Response::PullSnapshotIds {
        head: repo.head,
        snapshot_ids,
    })
}

fn handle_pull_objects(repo_uuid: [u8; 32], object_ids: &[ObjectId]) -> Result<Response> {
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

    Ok(Response::PullObjects { objects, chunks })
}

async fn rpc_to_connected_client(handle: &Handle, request: Request) -> Result<Response> {
    let mut channel = handle.channel_open_session().await?;

    let bytes = postcard::to_allocvec(&request)?;
    channel.data(bytes.as_slice()).await?;
    channel.eof().await?;

    let mut raw = Vec::new();
    while let Some(msg) = channel.wait().await {
        if let russh::ChannelMsg::Data { data } = msg {
            raw.extend_from_slice(&data);
        }
    }

    if raw.is_empty() {
        anyhow::bail!("empty response from connected client");
    }

    Ok(postcard::from_bytes(&raw)?)
}

async fn handle_push_by_pulling(repo_uuid: [u8; 32], handle: Handle) -> Result<Response> {
    let _local = ensure_repo(repo_uuid)?;
    let base = std::path::Path::new(".");

    println!("push-triggered pull for repo {}", to_hex(&repo_uuid));
    crate::pull_and_merge_with(base, |req| {
        let handle = handle.clone();
        async move { rpc_to_connected_client(&handle, req).await }
    })
    .await?;
    println!("push-triggered pull complete for repo {}", to_hex(&repo_uuid));

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
    eprintln!(
        "warning: ~/.ssh/id_ed25519 not found; using an ephemeral host key (fingerprint changes on restart)"
    );
    KeyPair::generate_ed25519().context("key generation failed")
}

// ── mDNS server scan ────────────────────────────────────────────────────────

const MDNS_SERVICE_TYPE: &str = "_syncup._tcp.local.";

fn advertise_mdns(port: u16, repo_uuids: &[[u8; 32]]) -> Result<ServiceDaemon> {
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

    println!("mDNS advertised: {instance_name}.{MDNS_SERVICE_TYPE} -> {host_name}:{port}");
    Ok(mdns)
}

// ── Entry point ───────────────────────────────────────────────────────────────

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

    let _mdns = advertise_mdns(port, &repo_uuids)?;

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    println!("Listening on 0.0.0.0:{port}");

    let mut server = SyncupServer;
    loop {
        let (stream, addr) = listener.accept().await?;
        let config = config.clone();
        let handler = server.new_client(Some(addr));
        tokio::spawn(async move {
            let session = match russh::server::run_stream(config, stream, handler).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Handshake error [{addr}]: {e:#}");
                    return;
                }
            };
            if let Err(e) = session.await {
                eprintln!("Session error [{addr}]: {e:#}");
            }
        });
    }
}
