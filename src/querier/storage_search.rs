use log::{debug, error, trace};
use storage::block::BlockId;
use storage::bloom::bloom_filter::BloomFilter;
use storage::errors::StorageError;
use storage::search_request::SearchRequest;
use storage::span::{AttributeValue, Span};
use storage::sstable_reader::{SStableReader};

type SearchResult<T> = Result<T, StorageError>;

pub trait BlockStorageSearch {

    fn search(&self, query: &SearchRequest) -> Result<Vec<Span>, StorageError>;

}

pub struct LocalStorageSearch {
    storage: Box<dyn SStableReader + Send + Sync>,
}

impl LocalStorageSearch {

    pub fn new(storage: Box<dyn SStableReader + Send + Sync>) -> Self {
        Self { storage }
    }
}

impl BlockStorageSearch for LocalStorageSearch {

    fn search(&self, query: &SearchRequest) -> SearchResult<Vec<Span>> {
        let storage_meta = match self.storage.read_blocks_meta() {
            Ok(meta) => meta,
            Err(_) => {
                error!("Error occurred while reading storage metadata");
                return Ok(vec![]);
            }
        };

        let span_name = query.span_name.clone();
        let service_name = query.service_name.clone();
        let limit = query.limit.unwrap_or(usize::MAX);

        let mut trace_spans: Vec<Span> = Vec::new();
        for block in storage_meta.blocks {
            let block_id = BlockId::new(block);

            let block_iterator = match query.trace_id {
                Some(trace_id) => {
                    let bloom_filter = self.storage.read_bloom_filter(&block_id)?;

                    if bloom_filter.might_contain(&trace_id) {
                        debug!("Trace found in bloom filter in block {}", block_id.id);
                        let block_index = self.storage.read_block_index(&block_id)?;
                        let Some(entry) = block_index.find_trace_id(&trace_id) else {
                            continue;
                        };

                        self.storage.read_block_slice_iter(&block_id, entry.offset(), entry.length() as u64)?
                    } else {
                        // no trace in block, go to the next block
                        trace!("No trace found in bloom filter in block {}. Move to the next block", block_id.id);
                        continue
                    }
                },
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
        }

        Ok(trace_spans.iter().take(limit).cloned().collect())
    }
}