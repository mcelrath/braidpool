use thiserror::Error;

#[derive(Error, Debug)]
pub enum BraidCliError {
    #[error("RPC client error: {0}")]
    RpcClientError(#[from] jsonrpsee::core::ClientError),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Connection error: Could not connect to {0}")]
    ConnectionError(String),

    #[error("Timeout error: Request timed out after {0} seconds")]
    TimeoutError(u64),

    #[error("Invalid response from server: {0}")]
    InvalidResponse(String),

    #[error("Command not found: {0}")]
    CommandNotFound(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Server error: {0}")]
    ServerError(String),
}

pub type Result<T> = std::result::Result<T, BraidCliError>;