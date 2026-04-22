use anyhow::{Context, Result};
use std::collections::HashMap;
use async_trait::async_trait;
use mdns_sd::{ServiceDaemon, ServiceInfo};
use russh::ChannelId;
use russh::server::{Auth, Handler, Server, Session};
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
        }
    }
}

enum PendingRequest {
    Pull(Vec<u8>),
    Push(Vec<u8>),
}

/// Per-connection state. `pending_request` is `Some` while accumulating request payload bytes.
struct ConnectionHandler {
    pending_request: Option<PendingRequest>,
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
            "pull" => self.pending_request = Some(PendingRequest::Pull(Vec::new())),
            "push" => self.pending_request = Some(PendingRequest::Push(Vec::new())),
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
        if let Some(pending) = &mut self.pending_request {
            match pending {
                PendingRequest::Pull(buf) | PendingRequest::Push(buf) => buf.extend_from_slice(data),
            }
        }
        Ok(())
    }

    /// Client closed its write side — process any buffered request payload.
    async fn channel_eof(&mut self, channel: ChannelId, session: &mut Session) -> Result<()> {
        if let Some(pending) = self.pending_request.take() {
            let response = match pending {
                PendingRequest::Pull(buf) => match postcard::from_bytes::<Request>(&buf)? {
                    Request::Pull { repo_uuid } => handle_pull(repo_uuid)?,
                    _ => Response::Error("expected pull request".into()),
                },
                PendingRequest::Push(buf) => match postcard::from_bytes::<Request>(&buf)? {
                    Request::Push {
                        repo_uuid,
                        repository,
                        chunks,
                    } => handle_push(repo_uuid, repository, chunks)?,
                    _ => Response::Error("expected push request".into()),
                },
            };
            send_response(channel, session, &response)?;
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

fn handle_pull(repo_uuid: [u8; 32]) -> Result<Response> {
    let repo = load_repository()?;
    if repo.repo_uuid != repo_uuid {
        return Ok(Response::Error(format!(
            "unknown repo_uuid: requested={}, available={}",
            to_hex(&repo_uuid),
            to_hex(&repo.repo_uuid)
        )));
    }
    let mut chunks = Vec::new();
    for (id, obj) in &repo.objects {
        if !matches!(obj, Object::Chunk(_)) {
            continue;
        }
        let data = std::fs::read(format!(".syncup/chunks/{}", to_hex(&id.0)))
            .with_context(|| format!("missing chunk {}", to_hex(&id.0)))?;
        chunks.push((*id, data));
    }
    Ok(Response::Pull {
        repository: repo,
        chunks,
    })
}

fn handle_push(
    repo_uuid: [u8; 32],
    repository: Repository,
    chunks: Vec<(ObjectId, Vec<u8>)>,
) -> Result<Response> {
    if repository.repo_uuid != repo_uuid {
        return Ok(Response::Error(format!(
            "push repo_uuid mismatch: request={}, payload={}",
            to_hex(&repo_uuid),
            to_hex(&repository.repo_uuid)
        )));
    }

    let base = std::path::Path::new(".");
    let current = Repository::load(base);
    if current.repo_uuid != repo_uuid {
        return Ok(Response::Error(format!(
            "unknown repo_uuid: requested={}, available={}",
            to_hex(&repo_uuid),
            to_hex(&current.repo_uuid)
        )));
    }

    std::fs::create_dir_all(base.join(".syncup/chunks"))?;
    for (id, data) in &chunks {
        let path = base.join(format!(".syncup/chunks/{}", to_hex(&id.0)));
        if !path.exists() {
            std::fs::write(path, data)?;
        }
    }

    let mut local = Repository::load(base);
    local.merge(repository);
    local.save(base);
    Ok(Response::PushOk)
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
