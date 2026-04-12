use clap::{Command, arg};

pub fn cli() -> Command {
    Command::new("syncup")
        .about("File synchronization and backup program")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .subcommand(
            Command::new("init")
            .about("Initialize repository")
        )
        .subcommand(
            Command::new("chunk")
            .about("(Debug) Chunk a file")
            .arg(arg!(<FILE> "File to chunk"))
        )
}