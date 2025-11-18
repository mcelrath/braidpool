use crate::cli::OutputFormat;
use crate::error::Result;
use crate::output::{format_output, print_error, print_info};
use crate::rpc_client::RpcClient;

pub async fn handle_get_tips(client: &RpcClient, format: &OutputFormat) -> Result<()> {
    print_info("Fetching current DAG tips...");

    match client.get_tips().await {
        Ok(response) => {
            let formatted = format_output(&response, format)?;
            println!("{}", formatted);
            Ok(())
        }
        Err(e) => {
            print_error(&format!("Failed to get tips: {}", e));
            Err(e)
        }
    }
}