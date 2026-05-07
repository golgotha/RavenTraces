use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemtableConfig {
    pub max_size_bytes: usize,
    pub max_age_seconds: u64,
    // Pre-allocate the
    pub initial_capacity: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub max_block_size: usize,
    pub mem_table: MemtableConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_block_size: 16 * 1024 * 1024,
            mem_table: MemtableConfig::default(),
        }
    }
}

impl Default for MemtableConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 128 * 1024 * 1024,
            max_age_seconds: 10,
            initial_capacity: 10_000,
        }
    }
}
