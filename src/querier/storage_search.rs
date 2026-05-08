use log::{debug, error, trace};
use std::sync::Arc;
use storage::block::{BlockId, BlockMeta};
use storage::bloom_filter_cache::BloomCacheAccessor;
use storage::errors::StorageError;
use storage::search_request::SearchRequest;
use storage::span::{AttributeValue, Span, SERVICE_NAME_ATTRIBUTE};
use storage::sstable_reader::SStableReader;
use storage::types::StorageConfig;

type SearchResult<T> = Result<T, StorageError>;

pub trait BlockStorageSearch: Send + Sync {
    fn search(&self, query: &SearchRequest) -> Result<Vec<Span>, StorageError>;

    fn list_services(&self) -> Result<Vec<String>, StorageError>;
}

pub struct LocalStorageSearch {
    bloom_accessor: Arc<BloomCacheAccessor>,
    storage: Arc<Box<dyn SStableReader + Send + Sync>>,
    block_repository: Box<dyn BlockRepository>,
}

impl LocalStorageSearch {
    pub fn new(
        storage: Box<dyn SStableReader + Send + Sync>,
        storage_config: StorageConfig,
    ) -> Self {
        let storage: Arc<Box<dyn SStableReader + Send + Sync>> = Arc::from(storage);

        let bloom_filter_capacity = storage_config.bloom_filter.cache.capacity;
        let bloom_accessor = Arc::new(BloomCacheAccessor::new_local_accessor(
            Arc::clone(&storage),
            bloom_filter_capacity,
        ));

        let block_repository = Box::new(LocalBlockRepository::new(
            Arc::clone(&storage),
            Arc::clone(&bloom_accessor),
        ));

        Self {
            storage,
            bloom_accessor,
            block_repository,
        }
    }
}

impl BlockStorageSearch for LocalStorageSearch {
    fn search(&self, query: &SearchRequest) -> SearchResult<Vec<Span>> {
        let span_name = query.span_name.clone();
        let service_name = query.service_name.clone();
        let limit = query.limit.unwrap_or(usize::MAX);
        let candidates = self.block_repository.find_blocks(query)?;

        let mut trace_spans: Vec<Span> = Vec::new();

        for candidate in candidates {
            let block_id = candidate.id;
            let block_iterator = if query.trace_id.is_some() {
                let block_index = self.storage.read_block_index(&block_id)?;
                let Some(entry) = block_index.find_trace_id(&query.trace_id.unwrap()) else {
                    continue;
                };

                self.storage.read_block_slice_iter(
                    &block_id,
                    entry.offset(),
                    entry.length() as u64,
                )?
            } else {
                self.storage.read_block_iter(&block_id, 0)?
            };

            // let spans = block_iterator
            //     .map(|entry| Span::deserialize(&entry.payload()))
            //     .take(limit)
            //     .collect::<Result<Vec<Span>, _>>()?;


            for entry in block_iterator {
                if trace_spans.len() >= limit {
                    break;
                }
                let span = Span::deserialize(&entry?.payload());

                if let Some(svc) = service_name.as_deref() {
                    let span_service_name = span
                        .attributes
                        .get(SERVICE_NAME_ATTRIBUTE)
                        .and_then(AttributeValue::as_str);

                    if span_service_name != Some(svc) {
                        continue;
                    }
                }

                if let Some(name) = &span_name {
                    if span.name != name.as_str() {
                        continue;
                    }
                }
                trace_spans.push(span);
            }
            
        }

        /*for block in storage_meta.blocks {
            let block_id = BlockId::new(block);
            let block_iterator = match query.trace_id {
                Some(trace_id) => {
                    let might_exists = self.bloom_accessor.might_contain(&block_id, &trace_id)?;

                    if might_exists {
                        debug!("Trace found in bloom filter in block {}", block_id.id);
                        let block_index = self.storage.read_block_index(&block_id)?;
                        let Some(entry) = block_index.find_trace_id(&trace_id) else {
                            continue;
                        };

                        self.storage.read_block_slice_iter(
                            &block_id,
                            entry.offset(),
                            entry.length() as u64,
                        )?
                    } else {
                        // no trace in block, go to the next block
                        trace!(
                            "No trace found in bloom filter in block {}. Move to the next block",
                            block_id.id
                        );
                        continue;
                    }
                }
                None => self.storage.read_block_iter(&block_id, 0)?,
            };

            for entry in block_iterator {
                if trace_spans.len() >= limit {
                    break;
                }
                let span = Span::deserialize(&entry?.payload());

                if let Some(svc) = service_name.as_deref() {
                    let span_service_name = span
                        .attributes
                        .get("service.name")
                        .and_then(AttributeValue::as_str);

                    if span_service_name != Some(svc) {
                        continue;
                    }
                }

                if let Some(name) = &span_name {
                    if span.name != name.as_str() {
                        continue;
                    }
                }
                trace_spans.push(span);
            }
        }*/

        Ok(trace_spans.iter().take(limit).cloned().collect())
    }

    fn list_services(&self) -> Result<Vec<String>, StorageError> {
        Ok(Vec::new())
    }
}

trait BlockRepository: Send + Sync {
    fn find_blocks(&self, query: &SearchRequest) -> Result<Vec<BlockMeta>, StorageError>;
}

struct LocalBlockRepository {
    bloom_accessor: Arc<BloomCacheAccessor>,
    storage: Arc<Box<dyn SStableReader + Send + Sync>>,
}

impl LocalBlockRepository {
    fn new(
        storage: Arc<Box<dyn SStableReader + Send + Sync>>,
        bloom_accessor: Arc<BloomCacheAccessor>,
    ) -> Self {
        Self {
            storage,
            bloom_accessor,
        }
    }
}

impl BlockRepository for LocalBlockRepository {
    fn find_blocks(&self, query: &SearchRequest) -> Result<Vec<BlockMeta>, StorageError> {
        let storage_meta = match self.storage.read_blocks_meta() {
            Ok(meta) => meta,
            Err(_) => {
                error!("Error occurred while reading storage metadata");
                return Ok(vec![]);
            }
        };

        let mut candidates = Vec::new();
        for block in storage_meta.blocks {
            let block_id = BlockId::new(block);

            if let Some(trace_id) = query.trace_id {
                let might_exists = self.bloom_accessor.might_contain(&block_id, &trace_id)?;
                if might_exists {
                    let block_meta = self.storage.read_block_meta(&block_id)?;
                    candidates.push(block_meta);
                }
            } else {
                let block_meta = self.storage.read_block_meta(&block_id)?;
                candidates.push(block_meta);
            }
        }

        Ok(candidates)
    }
}
