use std::path::PathBuf;

use clap::{ArgAction, Command, arg, value_parser};

pub fn cli() -> Command {
    Command::new("syncup")
        .about("File synchronization and backup program")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .arg(
            arg!(-v --verbose "Enable verbose logging (debug level)")
                .global(true)
                .action(ArgAction::SetTrue),
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
        .subcommand(Command::new("pull").about("Scan and pull this repository from matching hosts"))
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
