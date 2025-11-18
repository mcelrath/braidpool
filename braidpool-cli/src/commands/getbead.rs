use crate::cli::OutputFormat;
use crate::error::Result;
use crate::output::{format_output, print_error, print_info};
use crate::rpc_client::RpcClient;

pub async fn handle_get_bead(
    client: &RpcClient,
    bead_hash: &str,
    format: &OutputFormat,
) -> Result<()> {
    print_info(&format!("Fetching bead with hash: {}", bead_hash));

    match client.get_bead(bead_hash).await {
        Ok(response) => {
            let formatted = format_output(&response, format)?;
            println!("{}", formatted);
            Ok(())
        }
        Err(e) => {
            print_error(&format!("Failed to get bead: {}", e));
            Err(e)
        }
    }
}