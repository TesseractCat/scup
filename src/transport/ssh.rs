use log::warn;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{protocol, pull, resolve_key_path, scan};

pub(crate) struct ReversePullHandler {
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
