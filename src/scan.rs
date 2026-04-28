use anyhow::{Context, Result, anyhow};
use log::{debug, info};
use mdns_sd::{ResolvedService, ServiceDaemon, ServiceEvent};
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::time::{Duration, Instant};

use crate::RepositoryId;

const MDNS_SERVICE_TYPE: &str = "_syncup._tcp.local.";

#[derive(Clone, Debug)]
pub struct ScannedRepo {
    pub repo_uuid: RepositoryId,
    pub root: String,
}

impl ScannedRepo {
    fn from_properties(repos: Option<&str>, roots: Option<&str>) -> Vec<Self> {
        let repo_ids = repos
            .unwrap_or("")
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .filter_map(RepositoryId::from_hex)
            .collect::<Vec<_>>();

        let repo_roots = roots
            .unwrap_or("")
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();

        repo_ids
            .into_iter()
            .enumerate()
            .map(|(i, repo_uuid)| ScannedRepo {
                repo_uuid,
                root: repo_roots
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| "<unknown>".to_string()),
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct ScannedHost {
    pub fullname: String,
    pub addrs: Vec<IpAddr>,
    pub port: u16,
    pub repos: Vec<ScannedRepo>,
}

impl ScannedHost {
    fn from_resolved_info(info: &ResolvedService) -> Self {
        let mut addrs: Vec<_> = info
            .get_addresses()
            .iter()
            .map(|ip| ip.to_ip_addr())
            .collect();
        addrs.sort();

        let repos = ScannedRepo::from_properties(
            info.get_property_val_str("repos"),
            info.get_property_val_str("roots"),
        );

        Self {
            fullname: info.get_fullname().to_string(),
            addrs,
            port: info.get_port(),
            repos,
        }
    }
}

pub fn scan_hosts(timeout_secs: u64) -> Result<Vec<ScannedHost>> {
    let mdns = ServiceDaemon::new().context("failed to start mDNS daemon")?;
    let receiver = mdns
        .browse(MDNS_SERVICE_TYPE)
        .context("failed to browse mDNS service type")?;

    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    let mut seen = BTreeSet::new();
    let mut hosts = Vec::new();

    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }

        let remaining = deadline.saturating_duration_since(now);
        let Ok(event) = receiver.recv_timeout(remaining) else {
            break;
        };

        if let ServiceEvent::ServiceResolved(info) = event {
            let host = ScannedHost::from_resolved_info(&info);
            if seen.insert(host.fullname.clone()) {
                debug!(
                    "resolved host {}:{} repos={}",
                    host.fullname,
                    host.port,
                    host.repos.len(),
                );
                hosts.push(host);
            }
        }
    }

    let _ = mdns.stop_browse(MDNS_SERVICE_TYPE);
    mdns.shutdown().context("failed to shutdown mDNS daemon")?;

    Ok(hosts)
}

pub fn scan(timeout_secs: u64) -> Result<()> {
    info!("Browsing for {MDNS_SERVICE_TYPE} for {timeout_secs}s...");
    let hosts = scan_hosts(timeout_secs)?;

    for host in &hosts {
        let mut addrs: Vec<_> = host.addrs.iter().map(ToString::to_string).collect();
        addrs.sort();
        let addrs = if addrs.is_empty() {
            "<no-address>".to_string()
        } else {
            addrs.join(",")
        };

        let repos = if host.repos.is_empty() {
            "<none>".to_string()
        } else {
            host.repos
                .iter()
                .map(|repo| format!("{}:{}", repo.repo_uuid, repo.root))
                .collect::<Vec<_>>()
                .join(",")
        };

        info!("- {} at {addrs}:{} repos={repos}", host.fullname, host.port);
    }

    if hosts.is_empty() {
        info!("No syncup servers found.");
    } else {
        info!("Found {} server(s).", hosts.len());
    }

    Ok(())
}

pub fn resolve_host(host_id: &str, timeout_secs: u64) -> Result<ScannedHost> {
    let mdns = ServiceDaemon::new().context("failed to start mDNS daemon")?;
    let receiver = mdns
        .browse(MDNS_SERVICE_TYPE)
        .context("failed to browse mDNS service type")?;

    let deadline = Instant::now() + Duration::from_secs(timeout_secs);

    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }

        let remaining = deadline.saturating_duration_since(now);
        let Ok(event) = receiver.recv_timeout(remaining) else {
            break;
        };

        if let ServiceEvent::ServiceResolved(info) = event {
            let host = ScannedHost::from_resolved_info(&info);
            if host.fullname == host_id {
                let _ = mdns.stop_browse(MDNS_SERVICE_TYPE);
                mdns.shutdown().context("failed to shutdown mDNS daemon")?;
                return Ok(host);
            }
        }
    }

    let _ = mdns.stop_browse(MDNS_SERVICE_TYPE);
    mdns.shutdown().context("failed to shutdown mDNS daemon")?;

    Err(anyhow!("host id not found: {host_id}"))
}
