use crate::cli::OutputFormat;
use crate::error::{BraidCliError, Result};
use crate::output::{format_output, print_error, print_info, print_success};
use crate::rpc_client::RpcClient;
use std::fs;

pub async fn handle_add_bead(
    client: &RpcClient,
    bead_data: &str,
    format: &OutputFormat,
) -> Result<()> {
    let data = if bead_data == "-" {
        // Read from stdin
        use std::io::Read;
        let mut buffer = String::new();
        std::io::stdin()
            .read_to_string(&mut buffer)
            .map_err(BraidCliError::IoError)?;
        buffer
    } else if bead_data.starts_with('{') {
        // Assume it's JSON data directly
        bead_data.to_string()
    } else {
        // Assume it's a file path
        print_info(&format!("Reading bead data from file: {}", bead_data));
        fs::read_to_string(bead_data).map_err(BraidCliError::IoError)?
    };

    // Validate that it's valid JSON
    serde_json::from_str::<serde_json::Value>(&data)
        .map_err(BraidCliError::SerializationError)?;

    print_info("Adding bead to braid...");

    match client.add_bead(&data).await {
        Ok(response) => {
            let formatted = format_output(&response, format)?;
            print_success("Bead added successfully!");
            println!("{}", formatted);
            Ok(())
        }
        Err(e) => {
            print_error(&format!("Failed to add bead: {}", e));
            Err(e)
        }
    }
}