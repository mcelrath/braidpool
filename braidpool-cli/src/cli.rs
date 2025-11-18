use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug, Clone)]
#[command(name = "braidpool-cli")]
#[command(about = "Command-line interface for Braidpool RPC server", long_about = None)]
#[command(version)]
pub struct Cli {
    /// RPC server address (default: 127.0.0.1:6682)
    #[arg(long, short = 's', default_value = "127.0.0.1:6682")]
    pub server_addr: String,

    /// Output format
    #[arg(long, short = 'f', value_enum, default_value_t = OutputFormat::Pretty)]
    pub format: OutputFormat,

    /// Request timeout in seconds (default: 30)
    #[arg(long, short = 't', default_value = "30")]
    pub timeout: u64,

    /// Enable verbose output
    #[arg(long, short = 'v')]
    pub verbose: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    /// Get a bead by hash
    GetBead {
        /// The bead hash (as a hex string)
        bead_hash: String,
    },
    /// Add a bead via JSON data
    AddBead {
        /// JSON-formatted bead data or path to JSON file
        bead_data: String,
    },
    /// Get current DAG tips
    GetTips,
    /// Get total number of beads
    GetBeadCount,
    /// Get total number of cohorts
    GetCohortCount,
}

#[derive(Clone, Debug, ValueEnum)]
pub enum OutputFormat {
    /// Pretty-printed human readable format
    Pretty,
    /// JSON format (default for programmatic use)
    Json,
    /// Compact output with minimal formatting
    Compact,
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Pretty => write!(f, "pretty"),
            OutputFormat::Json => write!(f, "json"),
            OutputFormat::Compact => write!(f, "compact"),
        }
    }
}