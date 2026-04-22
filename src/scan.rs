use anyhow::{Context, Result, anyhow};
use mdns_sd::{ServiceDaemon, ServiceEvent};
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::time::{Duration, Instant};

const MDNS_SERVICE_TYPE: &str = "_syncup._tcp.local.";

fn parse_repo_list(value: Option<&str>) -> Vec<[u8; 32]> {
    fn from_hex_32(s: &str) -> Option<[u8; 32]> {
        if s.len() != 64 {
            return None;
        }
        let mut out = [0u8; 32];
        for i in 0..32 {
            let b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
            out[i] = b;
        }
        Some(out)
    }

    value
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(from_hex_32)
        .collect()
}

#[derive(Clone, Debug)]
pub struct ScannedHost {
    pub fullname: String,
    pub addrs: Vec<IpAddr>,
    pub port: u16,
    pub repo_uuids: Vec<[u8; 32]>,
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
            let fullname = info.get_fullname().to_string();
            if seen.insert(fullname.clone()) {
                let mut addrs: Vec<_> = info
                    .get_addresses()
                    .iter()
                    .map(|ip| ip.to_ip_addr())
                    .collect();
                addrs.sort();

                hosts.push(ScannedHost {
                    fullname,
                    addrs,
                    port: info.get_port(),
                    repo_uuids: parse_repo_list(info.get_property_val_str("repos")),
                });
            }
        }
    }

    let _ = mdns.stop_browse(MDNS_SERVICE_TYPE);
    mdns.shutdown().context("failed to shutdown mDNS daemon")?;

    Ok(hosts)
}

pub fn scan(timeout_secs: u64) -> Result<()> {
    println!("Browsing for {MDNS_SERVICE_TYPE} for {timeout_secs}s...");
    let hosts = scan_hosts(timeout_secs)?;

    for host in &hosts {
        let mut addrs: Vec<_> = host.addrs.iter().map(ToString::to_string).collect();
        addrs.sort();
        let addrs = if addrs.is_empty() {
            "<no-address>".to_string()
        } else {
            addrs.join(",")
        };

        let repos = if host.repo_uuids.is_empty() {
            "<none>".to_string()
        } else {
            host.repo_uuids
                .iter()
                .map(|id| id.iter().map(|b| format!("{b:02x}")).collect::<String>())
                .collect::<Vec<_>>()
                .join(",")
        };

        println!("- {} at {addrs}:{} repos={repos}", host.fullname, host.port);
    }

    if hosts.is_empty() {
        println!("No syncup servers found.");
    } else {
        println!("Found {} server(s).", hosts.len());
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
            let fullname = info.get_fullname().to_string();
            if fullname == host_id {
                let mut addrs: Vec<_> = info
                    .get_addresses()
                    .iter()
                    .map(|ip| ip.to_ip_addr())
                    .collect();
                addrs.sort();

                let host = ScannedHost {
                    fullname,
                    addrs,
                    port: info.get_port(),
                    repo_uuids: parse_repo_list(info.get_property_val_str("repos")),
                };

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
