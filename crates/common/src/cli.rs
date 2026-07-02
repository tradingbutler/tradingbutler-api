use clap::{Parser, Subcommand};

/// Shared CLI shape for every service binary — parsed with
/// `common::cli::parse()` before touching Redis, logging, or the HTTP server,
/// so `healthcheck`/`version` stay cheap and dependency-free.
#[derive(Parser)]
#[command(version = crate::VERSION)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Command {
    /// Probe the service's own `/healthz` endpoint and exit 0/1.
    ///
    /// Meant to be invoked as the process itself (e.g.
    /// `docker HEALTHCHECK CMD ["/app/service", "healthcheck"]`) in images
    /// that have no shell or curl to probe with.
    Healthcheck,
    /// Print the service version and exit.
    Version,
    /// Start the service. This is the normal, long-running mode.
    Start,
}

pub fn parse() -> Cli {
    Cli::parse()
}
