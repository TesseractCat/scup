use std::path::PathBuf;

use clap::{Arg, ArgAction, Command, arg, value_parser};

pub fn cli() -> Command {
    Command::new(syncup::CRATE_NAME)
        .about("File synchronization and backup program")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .help(format!(
                    "Enable verbose logging for {} (use -vv for all logs)",
                    syncup::CRATE_NAME
                ))
                .global(true)
                .action(ArgAction::Count),
        )
        .arg(
            arg!(--key <PATH> "SSH private key path (overrides auto discovery)")
                .global(true)
                .value_parser(value_parser!(PathBuf)),
        )
        .subcommand(Command::new("init").about("Initialize repository"))
        .subcommand(
            Command::new("debug")
                .about("Debug utilities")
                .subcommand_required(true)
                .arg_required_else_help(true)
                .subcommand(
                    Command::new("chunk")
                        .about("Print chunk boundaries of a file")
                        .arg(arg!(<PATH> "File to chunk").value_parser(value_parser!(PathBuf))),
                )
                .subcommand(
                    Command::new("print-repo")
                        .about("Print the debug representation of the repository"),
                )
                .subcommand(
                    Command::new("status")
                        .about("Query the status endpoint of a scanned host")
                        .arg(arg!(<HOST> "Host id as printed by `scan`")),
                ),
        )
        .subcommand(
            Command::new("snapshot")
                .about("Create a new snapshot of the current directory")
                .arg(arg!(-m --message <MESSAGE> "Snapshot message").required(false)),
        )
        .subcommand(Command::new("push").about("Scan and push this repository to matching hosts"))
        .subcommand(
            Command::new("pull")
                .about("Scan and pull this repository from matching hosts")
                .arg(
                    arg!(--fetch "Fetch+merge only; do not update working tree")
                        .required(false)
                        .action(ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("clone")
                .about("Clone a repository from a scanned host")
                .arg(arg!(<HOST> "Host id as printed by `scan`"))
                .arg(arg!(<REPO> "Repo root name or repo id"))
                .arg(
                    arg!(--bare "Clone metadata/objects only; do not check out files")
                        .required(false)
                        .action(ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("checkout")
                .about("Check out a snapshot hash into the working tree")
                .arg(arg!(<SNAPSHOT> "64-char snapshot object hash")),
        )
        .subcommand(
            Command::new("scan")
                .about("Scan for sync servers on the local network via mDNS")
                .arg(
                    arg!(--timeout <SECONDS> "How long to browse for services")
                        .default_value("3")
                        .value_parser(value_parser!(u64)),
                ),
        )
        .subcommand(
            Command::new("serve").about("Start sync server").arg(
                arg!(--port <PORT> "Port to listen on")
                    .default_value("6767")
                    .value_parser(value_parser!(u16)),
            ),
        )
}
