use std::fs::File;
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
    #[serde(rename = "storage")]
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

impl Settings {
    pub fn new(config_path: Option<String>) -> Result<Self, String> {
        let mut errors = Vec::new();
        let config_exists = |path| File::open(path).is_ok();

        if let Some(path) = &config_path
            && !config_exists(path)
        {
            errors.push(format!(
                "Config file via --config-path is not found: {path}"
            ));
        }

        Ok(Settings::default())
    }
}

#[allow(clippy::unnecessary_wraps)] // Used as serde default
const fn default_tls_cert_ttl() -> Option<u64> {
    // Default one hour
    Some(3600)
}