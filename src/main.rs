use log::info;
use std::path::{Path, PathBuf};

mod cli;
use cli::cli;

fn init_logger(verbosity: u8) {
    let default_level = if verbosity >= 2 {
        "debug"
    } else if verbosity >= 1 {
        "info,syncup=debug"
    } else {
        "info"
    };

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(default_level))
        .target(env_logger::Target::Stdout)
        .init();
}

fn main() -> anyhow::Result<()> {
    let matches = cli().get_matches();
    init_logger(matches.get_count("verbose"));

    if let Some(key_path) = matches.get_one::<PathBuf>("key") {
        syncup::set_key_override(key_path.clone());
    }

    match matches.subcommand() {
        Some(("init", _)) => {
            syncup::Repository::init(Path::new("."));
        }
        Some(("snapshot", sub)) => {
            let message = sub.get_one::<String>("message").cloned();
            let base = Path::new(".");
            let mut repo = syncup::Repository::load(base);
            repo.snapshot(base, message);
        }
        Some(("debug", sub)) => match sub.subcommand() {
            Some(("chunk", sub)) => {
                let path = sub
                    .get_one::<PathBuf>("PATH")
                    .expect("PATH is required by clap");
                syncup::debug_chunk_file(path);
            }
            Some(("print-repo", _)) => {
                let repo: syncup::Repository = syncup::Repository::load(Path::new("."));
                info!("{:#?}", repo);
            }
            Some(("status", sub)) => {
                let host_id = sub
                    .get_one::<String>("HOST")
                    .expect("HOST is required by clap");
                tokio::runtime::Runtime::new()?.block_on(syncup::debug_status(host_id))?;
            }
            _ => unreachable!(),
        },
        Some(("scan", sub)) => {
            let timeout = *sub
                .get_one::<u64>("timeout")
                .expect("timeout has default in clap");
            syncup::scan(timeout)?;
        }
        Some(("push", _)) => {
            tokio::runtime::Runtime::new()?.block_on(syncup::push_all(Path::new(".")))?;
        }
        Some(("pull", sub)) => {
            let fetch_only = sub.get_flag("fetch");
            if fetch_only {
                tokio::runtime::Runtime::new()?.block_on(syncup::fetch_all(Path::new(".")))?;
            } else {
                tokio::runtime::Runtime::new()?.block_on(syncup::pull_all(Path::new(".")))?;
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
            tokio::runtime::Runtime::new()?.block_on(syncup::clone_from(host_id, repo, bare))?;
        }
        Some(("checkout", sub)) => {
            let snapshot = sub
                .get_one::<String>("SNAPSHOT")
                .expect("SNAPSHOT is required by clap");
            syncup::checkout(Path::new("."), snapshot)?;
        }
        Some(("serve", sub)) => {
            let port = *sub
                .get_one::<u16>("port")
                .expect("port has default in clap");
            tokio::runtime::Runtime::new()?.block_on(syncup::serve_on(port))?;
        }
        _ => {}
    }

    Ok(())
}
