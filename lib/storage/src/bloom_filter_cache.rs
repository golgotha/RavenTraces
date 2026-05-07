use crate::block::BlockId;
use crate::bloom::bloom_filter::{BloomFilter, BloomFilterImpl};
use crate::errors::StorageError;
use crate::span::TraceId;
use crate::sstable_reader::SStableReader;
use std::sync::{Arc, RwLock};
use indexmap::IndexMap;

type TraceBloomFilter = dyn BloomFilter<TraceId> + Send + Sync + 'static;

pub trait BloomFilterCache: Send + Sync {
    fn put(&self, block_id: BlockId, bloom_filter: Arc<TraceBloomFilter>);

    fn get(&self, block_id: &BlockId) -> Option<Arc<TraceBloomFilter>>;

    fn might_contain(&self, block_id: &BlockId, trace_id: &TraceId) -> Option<bool>;

}

pub trait BloomFilterProvider: Send + Sync {
    /// Loads a block bloom filter from storage
    fn load_bloom_filter(&self, block_id: &BlockId) -> Result<BloomFilterImpl, StorageError>;
}

pub struct LruBloomFilterCache {
    capacity: usize,
    inner: RwLock<CacheInnerState>,
}

struct CacheInnerState {
    entries: IndexMap<BlockId, Arc<TraceBloomFilter>>,
}

pub struct BloomCacheAccessor {
    provider: Arc<dyn BloomFilterProvider>,
    cache: Arc<dyn BloomFilterCache>,
}


pub struct LocalBloomFilterProvider {
    storage_reader: Arc<Box<dyn SStableReader + Send + Sync>>
}

impl LruBloomFilterCache {
    fn new(capacity: usize) -> LruBloomFilterCache {
        LruBloomFilterCache {
            capacity,
            inner: RwLock::new(CacheInnerState {
                entries: IndexMap::new(),
            }),
        }
    }

    fn evict_if_needed(&self, entries: &mut IndexMap<BlockId, Arc<TraceBloomFilter>>) {
        while entries.len() > self.capacity {
            entries.shift_remove_index(0);
        }
    }
}

impl BloomFilterCache for LruBloomFilterCache {

    fn put(&self, block_id: BlockId, bloom_filter: Arc<TraceBloomFilter>) {
        let mut state = self.inner.write().unwrap();
        state.entries.shift_remove(&block_id);
        state.entries.insert(block_id, bloom_filter);
        self.evict_if_needed(&mut state.entries);

    }

    fn get(&self, block_id: &BlockId) -> Option<Arc<TraceBloomFilter>> {
        let inner = self.inner.read().unwrap();
        let bloom_filter = inner.entries.get(block_id).cloned();
        bloom_filter
    }

    fn might_contain(&self, block_id: &BlockId, trace_id: &TraceId) -> Option<bool> {
        let state = self.inner.read().unwrap();
        state
            .entries
            .get(block_id)
            .map(|bloom_filter| bloom_filter.might_contain(trace_id))
    }
}

impl LocalBloomFilterProvider {
    fn new(storage_reader: Arc<Box<dyn SStableReader + Send + Sync>>) -> LocalBloomFilterProvider {
        Self { storage_reader }
    }
}

impl BloomFilterProvider for LocalBloomFilterProvider {

    fn load_bloom_filter(&self, block_id: &BlockId) -> Result<BloomFilterImpl, StorageError> {
        let result = self.storage_reader.read_bloom_filter(block_id)?;
        Ok(result)
    }
}


/// The implementation of facade to Bloom filter cache access
impl BloomCacheAccessor {
    pub fn new(provider: Arc<dyn BloomFilterProvider>, cache: Arc<dyn BloomFilterCache>) -> Self {
        Self { provider, cache }
    }

    /// Creates a local Bloom filter cache accessor backed by an [`SStableReader`].
    ///
    /// This constructor creates:
    ///
    /// - a [`LocalBloomFilterProvider`] for loading Bloom filters from local SSTable storage;
    /// - a [`LruBloomFilterCache`] with the provided capacity.
    ///
    /// The `capacity` argument defines the maximum number of Bloom filters that
    /// should be kept in memory by the cache.
    ///
    /// This is the default constructor for query/search components that use
    /// local SSTable storage.
    pub fn new_local_accessor(storage_reader: Arc<Box<dyn SStableReader + Send + Sync>>, capacity: usize) -> BloomCacheAccessor {
        let provider = Arc::new(LocalBloomFilterProvider::new(storage_reader));
        let cache = Arc::new(LruBloomFilterCache::new(capacity));
        BloomCacheAccessor {
            provider,
            cache
        }
    }

    /// Checks whether the block may contain the given [`TraceId`].
    ///
    /// The method first checks the in-memory Bloom filter cache. If the Bloom
    /// filter for the block is already cached, the cached filter is used.
    ///
    /// If the Bloom filter is not cached, it is loaded from storage through
    /// [`BloomFilterProvider`], checked against the provided trace id, and then
    /// inserted into the cache for future lookups.
    ///
    /// Bloom filter semantics:
    ///
    /// - `Ok(false)` means the block definitely does not contain the trace;
    /// - `Ok(true)` means the block may contain the trace and should be checked further;
    /// - `Err(...)` means the Bloom filter could not be loaded or evaluated.
    ///
    /// Because Bloom filters can produce false positives, a `true` result does
    /// not guarantee that the trace exists in the block. The caller should still
    /// verify the block index or block data.
    pub fn might_contain(
        &self,
        block_id: &BlockId,
        trace_id: &TraceId,
    ) -> Result<bool, StorageError> {
        if let Some(result) = self.cache.might_contain(block_id, trace_id) {
            return Ok(result);
        }

        let bloom_filter = self.provider.load_bloom_filter(block_id)?;

        let result = bloom_filter.might_contain(trace_id);

        self.cache.put(block_id.clone(), Arc::new(bloom_filter));

        Ok(result)
    }
}