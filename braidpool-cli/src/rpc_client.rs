use crate::error::{BraidCliError, Result};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::core::params::ArrayParams;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use std::time::Duration;

pub struct RpcClient {
    client: HttpClient,
    server_addr: String,
}

impl RpcClient {
    pub fn new(server_addr: &str, timeout: u64) -> Result<Self> {
        let target_uri = format!("http://{}", server_addr);

        // Validate server address format
        let _socket_addr: std::net::SocketAddr = server_addr
            .parse()
            .map_err(|_| BraidCliError::ConnectionError(server_addr.to_string()))?;

        let client = HttpClientBuilder::default()
            .request_timeout(Duration::from_secs(timeout))
            .build(target_uri)
            .map_err(|e| BraidCliError::RpcClientError(e.into()))?;

        Ok(Self {
            client,
            server_addr: server_addr.to_string(),
        })
    }

    pub fn server_addr(&self) -> &str {
        &self.server_addr
    }

    pub async fn get_bead(&self, bead_hash: &str) -> Result<String> {
        let mut params = ArrayParams::new();
        params.insert(bead_hash)?;

        let response = self
            .client
            .request::<String, _>("getbead", params)
            .await?;

        Ok(response)
    }

    pub async fn add_bead(&self, bead_data: &str) -> Result<String> {
        let mut params = ArrayParams::new();
        params.insert(bead_data)?;

        let response = self
            .client
            .request::<String, _>("addbead", params)
            .await?;

        Ok(response)
    }

    pub async fn get_tips(&self) -> Result<String> {
        let params = ArrayParams::new();

        let response = self
            .client
            .request::<String, _>("gettips", params)
            .await?;

        Ok(response)
    }

    pub async fn get_bead_count(&self) -> Result<String> {
        let params = ArrayParams::new();

        let response = self
            .client
            .request::<String, _>("getbeadcount", params)
            .await?;

        Ok(response)
    }

    pub async fn get_cohort_count(&self) -> Result<String> {
        let params = ArrayParams::new();

        let response = self
            .client
            .request::<String, _>("getcohortcount", params)
            .await?;

        Ok(response)
    }

    pub async fn ping(&self) -> Result<bool> {
        // Try to get bead count as a simple ping
        match self.get_bead_count().await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}