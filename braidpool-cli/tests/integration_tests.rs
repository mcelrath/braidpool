use braidpool_cli::cli::{Cli, Commands};
use braidpool_cli::error::BraidCliError;
use braidpool_cli::output::format_output;
use braidpool_cli::rpc_client::RpcClient;
use clap::Parser;
use serde_json::json;

#[test]
fn test_cli_parsing() {
    // Test basic command parsing
    let cli = Cli::parse_from(&["braidpool-cli", "get-bead-count"]);
    assert!(matches!(cli.command, Commands::GetBeadCount));

    // Test with server address
    let cli = Cli::parse_from(&["braidpool-cli", "--server-addr", "127.0.0.1:9999", "get-tips"]);
    assert_eq!(cli.server_addr, "127.0.0.1:9999");
    assert!(matches!(cli.command, Commands::GetTips));

    // Test with format
    let cli = Cli::parse_from(&["braidpool-cli", "--format", "json", "get-cohort-count"]);
    assert!(matches!(cli.command, Commands::GetCohortCount));

    // Test get-bead command
    let cli = Cli::parse_from(&[
        "braidpool-cli",
        "get-bead",
        "00000000e61c695daae1f94c2c79b5e195f9e064f96f9bd72b132a86df067c5a"
    ]);
    if let Commands::GetBead { bead_hash } = cli.command {
        assert_eq!(
            bead_hash,
            "00000000e61c695daae1f94c2c79b5e195f9e064f96f9bd72b132a86df067c5a"
        );
    } else {
        panic!("Expected GetBead command");
    }
}

#[test]
fn test_rpc_client_creation() {
    // Test valid server address
    let client = RpcClient::new("127.0.0.1:6682", 30);
    assert!(client.is_ok());

    // Test invalid server address
    let client = RpcClient::new("invalid-address", 30);
    assert!(client.is_err());
}

#[test]
fn test_output_formatting() {
    let test_json = json!({
        "test": "value",
        "number": 42,
        "array": [1, 2, 3]
    });

    let json_str = test_json.to_string();

    // Test JSON format
    let formatted = format_output(&json_str, &braidpool_cli::cli::OutputFormat::Json).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&formatted).unwrap();
    assert_eq!(parsed, test_json);

    // Test pretty format
    let pretty_formatted = format_output(&json_str, &braidpool_cli::cli::OutputFormat::Pretty).unwrap();
    assert!(!pretty_formatted.is_empty());

    // Test compact format
    let compact_formatted = format_output(&json_str, &braidpool_cli::cli::OutputFormat::Compact).unwrap();
    assert!(compact_formatted.lines().count() <= 1); // Should be on one line for JSON
}

#[test]
fn test_error_creation() {
    // Test various error types
    let io_error = BraidCliError::IoError(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "File not found"
    ));
    assert!(io_error.to_string().contains("IO error"));

    // Test by parsing invalid JSON to get a serialization error
    let invalid_json = "{ invalid json }";
    let parse_result: Result<serde_json::Value, _> = serde_json::from_str(invalid_json);
    if let Err(json_err) = parse_result {
        let serialization_error = BraidCliError::SerializationError(json_err);
        assert!(serialization_error.to_string().contains("Serialization error"));
    } else {
        panic!("Expected JSON parsing to fail");
    }

    let connection_error = BraidCliError::ConnectionError("127.0.0.1:9999".to_string());
    assert!(connection_error.to_string().contains("Could not connect to"));
}

#[tokio::test]
async fn test_rpc_client_methods() {
    // Create client (this will fail if no server is running, but we're testing the method structure)
    let client = RpcClient::new("127.0.0.1:9999", 5).unwrap();

    // Test that methods exist and have correct signatures
    // These will fail due to no server, but verify the interface is correct
    let bead_result = client.get_bead("test_hash").await;
    assert!(bead_result.is_err());

    let tips_result = client.get_tips().await;
    assert!(tips_result.is_err());

    let count_result = client.get_bead_count().await;
    assert!(count_result.is_err());

    let cohort_result = client.get_cohort_count().await;
    assert!(cohort_result.is_err());
}

#[test]
fn test_invalid_bead_data_validation() {
    // Test invalid JSON
    let invalid_json = "{ invalid json }";
    let parsed: Result<serde_json::Value, _> = serde_json::from_str(invalid_json);
    assert!(parsed.is_err());

    // Test valid JSON
    let valid_json = r#"{"test": "value"}"#;
    let parsed: Result<serde_json::Value, _> = serde_json::from_str(valid_json);
    assert!(parsed.is_ok());
}