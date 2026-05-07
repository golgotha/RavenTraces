use serde::{Deserialize};

#[derive(Debug, Clone, Deserialize)]
pub struct MemtableConfig {
    pub max_size_bytes: usize,
    pub max_age_seconds: u64,
    // Pre-allocate the
    pub initial_capacity: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BloomFilterConfig {
    pub cache: BloomFilterCacheConfig
}

#[derive(Debug, Clone, Deserialize)]
pub struct BloomFilterCacheConfig {
    pub capacity: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    pub max_block_size: usize,
    pub mem_table: MemtableConfig,
    pub bloom_filter: BloomFilterConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_block_size: 16 * 1024 * 1024,
            mem_table: MemtableConfig::default(),
            bloom_filter: BloomFilterConfig::default(),
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

impl Default for BloomFilterConfig {
    fn default() -> Self {
        Self {
            cache: Default::default()
        }
    }
}

impl Default for BloomFilterCacheConfig {
    fn default() -> Self {
        Self { capacity: 10_000 }
    }
}