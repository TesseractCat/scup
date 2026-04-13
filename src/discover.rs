use anyhow::{Context, Result};
use mdns_sd::{ServiceDaemon, ServiceEvent};
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

const MDNS_SERVICE_TYPE: &str = "_syncup._tcp.local.";

pub fn discover(timeout_secs: u64) -> Result<()> {
    let mdns = ServiceDaemon::new().context("failed to start mDNS daemon")?;
    let receiver = mdns
        .browse(MDNS_SERVICE_TYPE)
        .context("failed to browse mDNS service type")?;

    println!("Browsing for {MDNS_SERVICE_TYPE} for {timeout_secs}s...");

    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    let mut seen = BTreeSet::new();

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
                let mut addrs: Vec<_> = info.get_addresses().iter().map(|ip| ip.to_string()).collect();
                addrs.sort();
                let addrs = if addrs.is_empty() {
                    "<no-address>".to_string()
                } else {
                    addrs.join(",")
                };

                println!("- {fullname} at {addrs}:{}", info.get_port());
            }
        }
    }

    let _ = mdns.stop_browse(MDNS_SERVICE_TYPE);
    mdns.shutdown().context("failed to shutdown mDNS daemon")?;

    if seen.is_empty() {
        println!("No syncup servers found.");
    } else {
        println!("Found {} server(s).", seen.len());
    }

    Ok(())
}
