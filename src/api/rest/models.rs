use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct VersionInfo {
    pub title: String,
    pub version: String,
}

impl Default for VersionInfo {
    fn default() -> Self {
        VersionInfo {
            title: "RavenTraces - traces storage engine".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}


