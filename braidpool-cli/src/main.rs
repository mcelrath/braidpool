mod cli;
mod commands;
mod error;
mod output;
mod rpc_client;

use clap::Parser;
use cli::{Cli, Commands};
use error::Result;
use output::{print_error, print_info, print_warning};
use rpc_client::RpcClient;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging based on verbosity
    let log_level = if cli.verbose {
        tracing::Level::DEBUG
    } else {
        tracing::Level::INFO
    };

    tracing_subscriber::fmt()
        .with_max_level(log_level)
        .with_target(false)
        .init();

    // Create RPC client
    let client = match RpcClient::new(&cli.server_addr, cli.timeout) {
        Ok(client) => {
            print_info(&format!("Connected to RPC server at {}", client.server_addr()));
            client
        }
        Err(e) => {
            print_error(&format!("Failed to create RPC client: {}", e));
            return Err(e);
        }
    };

    // Test connection with timeout
    print_info("Testing connection...");
    let ping_result = timeout(Duration::from_secs(cli.timeout), client.ping()).await;
    match ping_result {
        Ok(Ok(true)) => print_success("Connection successful"),
        Ok(Ok(false)) => {
            print_warning("Server responded but connection test failed");
        }
        Ok(Err(e)) => {
            print_error(&format!("Connection test failed: {}", e));
            return Err(e);
        }
        Err(_) => {
            print_error(&format!("Connection timed out after {} seconds", cli.timeout));
            return Err(error::BraidCliError::TimeoutError(cli.timeout));
        }
    }

    // Execute command
    let result = match cli.command {
        Commands::GetBead { bead_hash } => {
            commands::getbead::handle_get_bead(&client, &bead_hash, &cli.format).await
        }
        Commands::AddBead { bead_data } => {
            commands::addbead::handle_add_bead(&client, &bead_data, &cli.format).await
        }
        Commands::GetTips => {
            commands::gettips::handle_get_tips(&client, &cli.format).await
        }
        Commands::GetBeadCount => {
            commands::getbeadcount::handle_get_bead_count(&client, &cli.format).await
        }
        Commands::GetCohortCount => {
            commands::getcohortcount::handle_get_cohort_count(&client, &cli.format).await
        }
    };

    match result {
        Ok(_) => {
            print_info("Command completed successfully");
            Ok(())
        }
        Err(e) => {
            print_error(&format!("Command failed: {}", e));
            Err(e)
        }
    }
}

fn print_success(message: &str) {
    output::print_success(message);
}