use crossterm::cursor::MoveToColumn;
use crossterm::execute;
use crossterm::style::Stylize;
use crossterm::terminal::{Clear, ClearType};
use log::info;
use std::{
    collections::BTreeSet,
    io::{Write, stdout},
    path::{Path, PathBuf},
    time::Duration,
};

mod cli;
use cli::cli;

fn init_logger(verbosity: u8) {
    let default_level = if verbosity >= 2 {
        "debug".to_string()
    } else if verbosity >= 1 {
        format!("info,{}=debug", scup::CRATE_NAME)
    } else {
        "info".to_string()
    };

    let mut builder =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(default_level));
    builder.target(env_logger::Target::Stdout);
    if verbosity == 0 {
        builder.format(|buf, record| writeln!(buf, "{}", record.args()));
    }
    builder.init();
}

fn print_local_status(base: &Path) -> anyhow::Result<Option<scup::RepositorySession>> {
    use ignore::Walk;
    use relative_path::RelativePath;

    let session = match scup::RepositorySession::load(base) {
        Ok(s) => s,
        Err(_) => {
            println!(
                "{}",
                format!(
                    "error: not a {} repository ({} directory not found)",
                    scup::CRATE_NAME,
                    scup::REPO_DIR_NAME
                )
                .red()
            );
            println!();
            return Ok(None);
        }
    };
    let repo = &session.repository;

    println!("On snapshot {}", repo.head.to_hex());
    println!();

    let tracked: std::collections::BTreeMap<String, scup::ObjectId> = match repo.objects.get(&repo.head) {
        Some(scup::Object::Snapshot(snap)) => match repo.objects.get(&snap.tree) {
            Some(scup::Object::Map(map)) => map.entries.clone(),
            _ => std::collections::BTreeMap::new(),
        },
        _ => std::collections::BTreeMap::new(),
    };

    let mut seen = BTreeSet::new();
    let mut modified = Vec::new();
    let mut created = Vec::new();
    let mut deleted = Vec::new();

    for entry in Walk::new(base)
        .flatten()
        .filter(|e| e.file_type().map_or(false, |t| t.is_file()))
    {
        let path = entry.path();
        let Some(rel) = path.strip_prefix(base).ok() else {
            continue;
        };
        let Some(rel_path) = RelativePath::from_path(rel)
            .ok()
            .map(|p| p.normalize().into_string())
        else {
            continue;
        };
        if rel_path == scup::REPO_DIR_NAME || rel_path.starts_with(scup::REPO_DIR_PREFIX) {
            continue;
        }
        seen.insert(rel_path.clone());

        match tracked.get(&rel_path) {
            Some(blob_id) => {
                let on_disk_mtime = std::fs::metadata(path).ok().and_then(|m| m.modified().ok());
                let tracked_mtime = match repo.objects.get(blob_id) {
                    Some(scup::Object::Blob(blob)) => blob.modified_time,
                    _ => None,
                };
                if tracked_mtime.is_some() && tracked_mtime != on_disk_mtime {
                    modified.push(rel_path);
                }
            }
            None => created.push(rel_path),
        }
    }

    for path in tracked.keys() {
        if !seen.contains(path) {
            deleted.push(path.clone());
        }
    }

    modified.sort();
    modified.dedup();
    created.sort();
    created.dedup();
    deleted.sort();
    deleted.dedup();

    if modified.is_empty() && created.is_empty() && deleted.is_empty() {
        println!("Working tree clean");
    } else {
        println!("Unsnapshotted changes:");
        println!("  (use \"scup snapshot\" to record these changes)");
        for p in &modified {
            println!("     {}: {}", "modified".yellow(), p.as_str().yellow());
        }
        for p in &created {
            println!("     {}: {}", "new".yellow(), p.as_str().yellow());
        }
        for p in &deleted {
            println!("     {}: {}", "deleted".red(), p.as_str().red());
        }
    }
    println!();

    Ok(Some(session))
}

async fn print_remote_status(
    base: &Path,
    timeout: u64,
    local: Option<&scup::RepositorySession>,
    minimal: bool,
 ) -> anyhow::Result<()> {
    let spinner = ['/', '-', '\\', '|'];
    let mut idx = 0usize;
    let scan_task = tokio::task::spawn_blocking(move || scup::scan_hosts(timeout));
    while !scan_task.is_finished() {
        print!("\r{} Scanning servers...", spinner[idx % spinner.len()]);
        stdout().flush()?;
        idx += 1;
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    let hosts = scan_task.await??;
    execute!(stdout(), MoveToColumn(0), Clear(ClearType::CurrentLine))?;

    let remotes: Vec<_> = match local {
        Some(s) if minimal => hosts
            .into_iter()
            .filter(|h| h.repos.iter().any(|r| r.repo_uuid == s.repository.repo_uuid))
            .collect(),
        _ => hosts,
    };

    if remotes.is_empty() {
        println!("No remotes found!");
        if minimal {
            println!("  (use \"scup scan\" to get more info)");
        }
        return Ok(());
    }

    println!("Remote status:");
    if minimal {
        println!("  (use \"scup scan\" to get more info)");
    }

    for host in remotes {
        let ip = host
            .addrs
            .first()
            .map(ToString::to_string)
            .unwrap_or_else(|| "<no-address>".to_string());
        let endpoint = format!("{}:{}", ip, host.port);

        if minimal {
            let remote = scup::remote_head(base, &host).await;
            match (remote, local) {
                (Ok(head), Some(s)) => {
                    let status = if head == s.repository.head {
                        "up-to-date".green().to_string()
                    } else {
                        "out-of-date".red().to_string()
                    };
                    println!(
                        "     {} ({}): {} (head={})",
                        host.id.as_str().bold(),
                        endpoint,
                        status,
                        head.to_hex()
                    );
                }
                (Ok(head), None) => {
                    println!("     {} ({}): head={}", host.id.as_str().bold(), endpoint, head.to_hex());
                }
                (Err(err), _) => println!(
                    "     {} ({}): {}",
                    host.id.as_str().bold(),
                    endpoint,
                    format!("unreachable ({err})").red()
                ),
            }
            continue;
        }

        println!("     {} ({})", host.id.as_str().bold(), endpoint);
        if host.repos.is_empty() {
            println!("       - <no repositories>");
            continue;
        }

        for repo in &host.repos {
            let head_result = scup::remote_repo_head(base, &host, repo.repo_uuid).await;
            match head_result {
                Ok(head) => {
                    let status_suffix = match local {
                        Some(s) if s.repository.repo_uuid == repo.repo_uuid => {
                            if s.repository.head == head {
                                format!(" {}", "up-to-date".green())
                            } else {
                                format!(" {}", "out-of-date".red())
                            }
                        }
                        _ => String::new(),
                    };
                    println!(
                        "       - {} ({}) head={}{}",
                        repo.root,
                        repo.repo_uuid.to_short_hex(),
                        head.to_hex(),
                        status_suffix
                    );
                }
                Err(err) => {
                    println!(
                        "       - {} ({}) {}",
                        repo.root,
                        repo.repo_uuid.to_short_hex(),
                        format!("(unreachable: {err})").red()
                    );
                }
            }
        }
    }

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let matches = cli().get_matches();
    init_logger(matches.get_count("verbose"));
    let timeout = *matches
        .get_one::<u64>("timeout")
        .expect("timeout has default in clap");

    if let Some(key_path) = matches.get_one::<PathBuf>("key") {
        scup::set_key_override(key_path.clone());
    }

    match matches.subcommand() {
        Some(("init", _)) => {
            let _ = scup::RepositorySession::init(Path::new("."))?;
        }
        Some(("snapshot", sub)) => {
            let message = sub.get_one::<String>("message").cloned();
            let base = Path::new(".");
            let mut session = scup::RepositorySession::load(base)?;
            session.snapshot(message);
        }
        Some(("debug", sub)) => match sub.subcommand() {
            Some(("chunk", sub)) => {
                let path = sub
                    .get_one::<PathBuf>("PATH")
                    .expect("PATH is required by clap");
                scup::debug_chunk_file(path);
            }
            Some(("print-repo", _)) => {
                let session = scup::RepositorySession::load(Path::new("."))?;
                info!("{:#?}", session.repository);
            }
            Some(("status", sub)) => {
                let host_id = sub
                    .get_one::<String>("HOST")
                    .expect("HOST is required by clap");
                tokio::runtime::Runtime::new()?.block_on(scup::debug_status(host_id))?;
            }
            _ => unreachable!(),
        },
        Some(("scan", _)) => {
            let local = scup::RepositorySession::load(Path::new(".")).ok();
            tokio::runtime::Runtime::new()?.block_on(print_remote_status(
                Path::new("."),
                timeout,
                local.as_ref(),
                false,
            ))?;
        }
        Some(("status", _)) => {
            let local = print_local_status(Path::new("."))?;
            tokio::runtime::Runtime::new()?.block_on(print_remote_status(
                Path::new("."),
                timeout,
                local.as_ref(),
                true,
            ))?;
        }
        Some(("push", _)) => {
            tokio::runtime::Runtime::new()?.block_on(scup::push_all(Path::new(".")))?;
        }
        Some(("pull", sub)) => {
            let fetch_only = sub.get_flag("fetch");
            if fetch_only {
                tokio::runtime::Runtime::new()?.block_on(scup::fetch_all(Path::new(".")))?;
            } else {
                tokio::runtime::Runtime::new()?.block_on(scup::pull_all(Path::new(".")))?;
            }
        }
        Some(("clone", sub)) => {
            let host_id = sub
                .get_one::<String>("HOST")
                .expect("HOST is required by clap");
            let repo = sub
                .get_one::<String>("REPO")
                .expect("REPO is required by clap");
            let bare = sub.get_flag("bare");
            tokio::runtime::Runtime::new()?.block_on(scup::clone_from(host_id, repo, bare))?;
        }
        Some(("checkout", sub)) => {
            let snapshot = sub
                .get_one::<String>("SNAPSHOT")
                .expect("SNAPSHOT is required by clap");
            scup::checkout(Path::new("."), snapshot)?;
        }
        Some(("serve", sub)) => {
            let port = *sub
                .get_one::<u16>("port")
                .expect("port has default in clap");
            tokio::runtime::Runtime::new()?.block_on(scup::serve_on(port))?;
        }
        _ => {}
    }

    Ok(())
}
