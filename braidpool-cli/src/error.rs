use thiserror::Error;

#[derive(Error, Debug)]
pub enum BraidCliError {
    #[error("RPC client error: {0}")]
    RpcClientError(#[from] jsonrpsee::core::ClientError),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Connection error: Could not connect to {0}")]
    ConnectionError(String),

    #[error("Timeout error: Request timed out after {0} seconds")]
    TimeoutError(u64),
}

pub type Result<T> = std::result::Result<T, BraidCliError>;