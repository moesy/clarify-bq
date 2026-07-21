mod cli;
mod config;
mod lock;
mod plan;
mod spool;
mod tables;

use clap::Parser;

fn main() {
    let cli = cli::Cli::parse();
    let level = match (cli.verbose, cli.quiet) {
        (_, q) if q > 0 => "warn",
        (0, _) => "info",
        (1, _) => "debug",
        _ => "trace",
    };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level));
    match cli.log_format {
        cli::Format::Json => tracing_subscriber::fmt().with_env_filter(filter).json().init(),
        cli::Format::Text => tracing_subscriber::fmt().with_env_filter(filter).init(),
    }
    // Subcommand dispatch lands with the orchestrator; parse-only for now.
    let _ = &cli.command;
    std::process::exit(cli::ExitCode::Complete.code());
}
