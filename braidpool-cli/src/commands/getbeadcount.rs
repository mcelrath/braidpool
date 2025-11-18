use crate::cli::OutputFormat;
use crate::error::Result;
use crate::output::{format_output, print_error, print_info};
use crate::rpc_client::RpcClient;

pub async fn handle_get_bead_count(client: &RpcClient, format: &OutputFormat) -> Result<()> {
    print_info("Fetching total bead count...");

    match client.get_bead_count().await {
        Ok(response) => {
            let formatted = format_output(&response, format)?;
            println!("{}", formatted);
            Ok(())
        }
        Err(e) => {
            print_error(&format!("Failed to get bead count: {}", e));
            Err(e)
        }
    }
}