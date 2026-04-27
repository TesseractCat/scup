use anyhow::{Context, Result};
use async_trait::async_trait;
use log::{debug, error, info, warn};
use mdns_sd::{ServiceDaemon, ServiceInfo};
use russh::ChannelId;
use russh::server::{Auth, Handle, Handler, Server, Session};
use russh_keys::key::KeyPair;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;

use crate::protocol::{Request, Response};
use crate::pull::RequestSender;
use crate::{Object, ObjectId, Repository, to_hex};

// ── Server / handler boilerplate ─────────────────────────────────────────────

struct SyncupServer {
    repo_cache: Arc<RwLock<Option<Repository>>>,
    allowed_client_key: Option<russh_keys::key::PublicKey>,
}

impl Server for SyncupServer {
    type Handler = ConnectionHandler;

    fn new_client(&mut self, _addr: Option<std::net::SocketAddr>) -> Self::Handler {
        ConnectionHandler {
            pending_request: Vec::new(),
            repo_cache: self.repo_cache.clone(),
            allowed_client_key: self.allowed_client_key.clone(),
        }
    }
}

/// Per-connection state for a single request/response exchange.
struct ConnectionHandler {
    pending_request: Vec<u8>,
    repo_cache: Arc<RwLock<Option<Repository>>>,
    allowed_client_key: Option<russh_keys::key::PublicKey>,
}

#[async_trait]
impl Handler for ConnectionHandler {
    type Error = anyhow::Error;

    async fn auth_publickey(
        &mut self,
        user: &str,
        public_key: &russh_keys::key::PublicKey,
    ) -> Result<Auth> {
        if let Some(expected) = &self.allowed_client_key {
            if public_key != expected {
                info!("credentials rejected: {user}");
                return Ok(Auth::Reject {
                    proceed_with_methods: None,
                });
            }
        }
        info!("credentials accepted: {user}");
        Ok(Auth::Accept)
    }

    async fn channel_open_session(
        &mut self,
        _channel: russh::Channel<russh::server::Msg>,
        _session: &mut Session,
    ) -> Result<bool> {
        Ok(true)
    }

    /// Accumulates request data and responds as soon as a full framed message is available.
    async fn data(&mut self, channel: ChannelId, data: &[u8], session: &mut Session) -> Result<()> {
        self.pending_request.extend_from_slice(data);
        debug!("buffered {} bytes of request payload", data.len());

        while let Some(frame) = crate::protocol::pop_framed_message(&mut self.pending_request) {
            let repo_cache = self.repo_cache.clone();
            let handle = session.handle();
            tokio::spawn(async move {
                let response = match crate::protocol::decode_request(&frame) {
                    Ok(request) => dispatch_request(request, &repo_cache, handle.clone()).await,
                    Err(err) => Response::Error(err.to_string()),
                };
                let _ = send_response_frame(channel, handle, &response).await;
            });
        }

        Ok(())
    }

    /// Client closed write side: close response side after flushing framed replies.
    async fn channel_eof(&mut self, channel: ChannelId, session: &mut Session) -> Result<()> {
        debug!(
            "channel eof on {:?}: trailing buffered bytes={}",
            channel,
            self.pending_request.len()
        );

        if !self.pending_request.is_empty() {
            let response = Response::Error("incomplete framed request at EOF".into());
            let _ = send_response_frame(channel, session.handle(), &response).await;
            self.pending_request.clear();
        }

        let _ = session.handle().eof(channel).await;
        let _ = session.handle().close(channel).await;
        Ok(())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn send_response_frame(
    channel: ChannelId,
    handle: Handle,
    response: &Response,
) -> Result<()> {
    let bytes = crate::protocol::encode_response(response)?;
    for chunk in bytes.chunks(32 * 1024) {
        let _ = handle.data(channel, chunk.to_vec().into()).await;
    }
    Ok(())
}

async fn dispatch_request(
    request: Request,
    repo_cache: &Arc<RwLock<Option<Repository>>>,
    handle: Handle,
) -> Response {
    info!("request: {request:?}");
    match request {
        Request::Version => Ok(Response::Version {
            version: crate::protocol::PROTOCOL_VERSION,
        }),
        Request::Status => handle_status(repo_cache).await,
        Request::Push { repo_uuid } => {
            handle_push_by_pulling(repo_uuid, handle, repo_cache.clone()).await
        }
        Request::PullSnapshotIds { repo_uuid } => {
            handle_pull_snapshot_ids(repo_uuid, repo_cache).await
        }
        Request::PullObjects {
            repo_uuid,
            object_ids,
        } => handle_pull_objects(repo_uuid, &object_ids, repo_cache).await,
    }
    .unwrap_or_else(|e| Response::Error(e.to_string()))
}

fn load_repository(base: &Path) -> Result<Repository> {
    let bytes = std::fs::read(base.join(".syncup/repository"))
        .context("repository not found — run `syncup init` first")?;
    Ok(postcard::from_bytes(&bytes)?)
}

async fn reload_repository_cache(
    base: &Path,
    repo_cache: &Arc<RwLock<Option<Repository>>>,
) -> Result<()> {
    let repo = load_repository(base)?;
    let head = to_hex(&repo.head.0);
    let object_count = repo.objects.len();

    let mut cache = repo_cache.write().await;
    *cache = Some(repo);

    info!("repository cache reloaded: head={head}, objects={object_count}");
    Ok(())
}

async fn repository_from_cache(repo_cache: &Arc<RwLock<Option<Repository>>>) -> Result<Repository> {
    repo_cache
        .read()
        .await
        .clone()
        .ok_or_else(|| anyhow::anyhow!("repository not found — run `syncup init` first"))
}

fn repository_fingerprint(path: &Path) -> Option<(SystemTime, u64)> {
    let meta = std::fs::metadata(path).ok()?;
    let modified = meta.modified().ok()?;
    Some((modified, meta.len()))
}

async fn watch_repository_file(base: PathBuf, repo_cache: Arc<RwLock<Option<Repository>>>) {
    let repo_path = base.join(".syncup/repository");
    let mut last_seen = repository_fingerprint(&repo_path);

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let current = repository_fingerprint(&repo_path);
        if current == last_seen {
            continue;
        }
        last_seen = current;

        match reload_repository_cache(&base, &repo_cache).await {
            Ok(()) => {
                info!("detected repository file change: {}", repo_path.display());
            }
            Err(err) => {
                warn!(
                    "failed to reload repository after change {}: {err:#}",
                    repo_path.display()
                );
            }
        }
    }
}

// ── Request handlers ──────────────────────────────────────────────────────────

async fn handle_status(repo_cache: &Arc<RwLock<Option<Repository>>>) -> Result<Response> {
    let repo = repository_from_cache(repo_cache).await?;
    Ok(Response::Status {
        head: repo.head,
        object_count: repo.objects.len(),
    })
}

async fn ensure_repo(
    repo_uuid: [u8; 32],
    repo_cache: &Arc<RwLock<Option<Repository>>>,
) -> Result<Repository> {
    let repo = repository_from_cache(repo_cache).await?;
    if repo.repo_uuid != repo_uuid {
        anyhow::bail!(
            "unknown repo_uuid: requested={}, available={}",
            to_hex(&repo_uuid),
            to_hex(&repo.repo_uuid)
        );
    }
    Ok(repo)
}

async fn handle_pull_snapshot_ids(
    repo_uuid: [u8; 32],
    repo_cache: &Arc<RwLock<Option<Repository>>>,
) -> Result<Response> {
    debug!("serve: pull snapshot ids for repo {}", to_hex(&repo_uuid));
    let repo = ensure_repo(repo_uuid, repo_cache).await?;
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

async fn handle_pull_objects(
    repo_uuid: [u8; 32],
    object_ids: &[ObjectId],
    repo_cache: &Arc<RwLock<Option<Repository>>>,
) -> Result<Response> {
    debug!(
        "serve: pull objects for repo {}, requested={}",
        to_hex(&repo_uuid),
        object_ids.len()
    );
    let repo = ensure_repo(repo_uuid, repo_cache).await?;
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

async fn rpc_via_handle(handle: &Handle, request: &Request) -> Result<Response> {
    let mut channel = handle.channel_open_session().await?;
    let response = crate::rpc(&mut channel, request).await?;
    let _ = channel.eof().await;
    let _ = channel.close().await;
    Ok(response)
}

struct HandleRequestSender {
    handle: Handle,
}

impl RequestSender for HandleRequestSender {
    async fn send(&mut self, request: Request) -> Result<Response> {
        rpc_via_handle(&self.handle, &request).await
    }
}

async fn handle_push_by_pulling(
    repo_uuid: [u8; 32],
    handle: Handle,
    repo_cache: Arc<RwLock<Option<Repository>>>,
) -> Result<Response> {
    let _local = ensure_repo(repo_uuid, &repo_cache).await?;
    let base = Path::new(".");

    info!("push-triggered pull for repo {}", to_hex(&repo_uuid));
    let mut sender = HandleRequestSender {
        handle: handle.clone(),
    };
    crate::fetch_and_merge_with(base, None, &mut sender).await?;
    crate::checkout_head(base)?;
    info!(
        "push-triggered pull complete for repo {}",
        to_hex(&repo_uuid)
    );

    if let Err(err) = reload_repository_cache(base, &repo_cache).await {
        warn!("push completed, but failed to refresh in-memory repository cache: {err:#}");
    }

    Ok(Response::PushComplete)
}

// ── Key loading ───────────────────────────────────────────────────────────────

fn load_server_keys() -> Result<(KeyPair, Option<russh_keys::key::PublicKey>)> {
    if let Some(path) = crate::resolve_key_path() {
        let key = russh_keys::load_secret_key(&path, None)
            .with_context(|| format!("failed to load SSH key {}", path.display()))?;
        let allowed = Some(
            key.clone_public_key()
                .context("failed to derive public key")?,
        );
        return Ok((key, allowed));
    }

    // russh-keys has no key-write API, so generate an ephemeral key and allow any client key.
    warn!("no SSH key found; using ephemeral host key and accepting any client key");
    let key = KeyPair::generate_ed25519().context("key generation failed")?;
    Ok((key, None))
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
    let (host_key, allowed_client_key) = load_server_keys()?;

    let config = Arc::new(russh::server::Config {
        // RFC 4253: identification string must start with "SSH-2.0-".
        server_id: russh::SshId::Standard("SSH-2.0-syncup-ssh".into()),
        keys: vec![host_key],
        ..Default::default()
    });

    let base = PathBuf::from(".");
    let initial_repo = load_repository(&base).ok();
    let repo_cache = Arc::new(RwLock::new(initial_repo.clone()));

    tokio::spawn(watch_repository_file(base.clone(), repo_cache.clone()));

    let repo_uuids = initial_repo
        .as_ref()
        .map(|r| vec![r.repo_uuid])
        .unwrap_or_else(Vec::new);
    let repo_roots = if repo_uuids.is_empty() {
        Vec::new()
    } else {
        vec![current_repo_root_name().unwrap_or_else(|| "<unknown>".to_string())]
    };

    let _mdns = advertise_mdns(port, &repo_uuids, &repo_roots)?;

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    info!("Listening on 0.0.0.0:{port}");

    let mut server = SyncupServer {
        repo_cache,
        allowed_client_key,
    };
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
                let msg = e.to_string();
                if msg.to_ascii_lowercase().contains("early eof") {
                    debug!("Session disconnected [{addr}]: {e:#}");
                } else {
                    error!("Session error [{addr}]: {e:#}");
                }
            }
        });
    }
}
