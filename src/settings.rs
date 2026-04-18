use serde::Deserialize;
use storage::types::StorageConfig;

#[derive(Debug, Deserialize, Clone)]
pub struct ServiceConfig {
    pub host: String,
    pub http_port: u16,
    pub enable_tls: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TlsConfig {
    pub cert: String,
    pub key: String,
    pub ca_cert: Option<String>,
    #[serde(default = "default_tls_cert_ttl")]
    pub cert_ttl: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub log_level: String,
    pub log_dir: String,
    pub data_dir: String,
    pub service: ServiceConfig,
    pub storage_config: StorageConfig,
    pub tls: Option<TlsConfig>,
}

impl Default for Settings {
    fn default() -> Self {
        let service = ServiceConfig {
            host: "0.0.0.0".to_string(),
            http_port: 9876,
            enable_tls: false,
        };
        
        Self {
            log_level: "info".to_string(),
            log_dir: "./logs".to_string(),
            data_dir: "./data".into(),
            service,
            storage_config: StorageConfig::default(),
            tls: None,
        }
    }

}

#[allow(clippy::unnecessary_wraps)] // Used as serde default
const fn default_tls_cert_ttl() -> Option<u64> {
    // Default one hour
    Some(3600)
}