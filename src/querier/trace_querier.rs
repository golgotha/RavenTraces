use crate::querier::storage_search::{BlockStorageSearch, LocalStorageSearch};
use log::{debug, error, info};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use storage::block::BlockId;
use storage::corvus_engine::CorvusEngine;
use storage::errors::StorageError;
use storage::search_request::SearchRequest;
use storage::span::{Span, TraceId};
use storage::sstable_reader::{SStableReader, SStableReaderImpl};
use storage::types::StorageConfig;

pub struct BlockRef {
    block_id: BlockId,
    offset: u64,
    length: u32,
}

pub struct TraceQuerier {
    corvus_engine: Arc<dyn CorvusEngine>,
    storage: Box<dyn SStableReader + Send + Sync>,
    storage_search: Box<dyn BlockStorageSearch>,
    block_index: HashMap<TraceId, BlockRef>,
}

impl TraceQuerier {
    pub fn new(
        base_dir: PathBuf,
        corvus_engine: Arc<dyn CorvusEngine>,
        storage_config: StorageConfig,
    ) -> TraceQuerier {
        let sstable_reader = Box::new(SStableReaderImpl::new(base_dir.as_path()));
        Self {
            corvus_engine,
            storage: Box::new(SStableReaderImpl::new(base_dir.as_path())),
            block_index: HashMap::new(),
            storage_search: Box::new(LocalStorageSearch::new(sstable_reader, storage_config)),
        }
    }

    pub fn load_blocks_index(&mut self) -> Result<(), StorageError> {
        // let blocks = self.storage.list_blocks()?;
        let blocks = Vec::new();
        info!("Loading blocks {} into index", blocks.len().clone());

        for block in &blocks {
            let block_index = match self.storage.read_block_index(&block) {
                Ok(index) => index,
                Err(err) => {
                    // you can choose to continue or fail hard
                    error!("Failed to read block index {}: {:?}", block, err);
                    continue;
                }
            };

            for entry in block_index.entries().values() {
                let block_ref = BlockRef::new(block.clone(), entry.offset(), entry.length());
                self.block_index.insert(entry.trace_id(), block_ref);
            }
        }

        Ok(())
    }

    pub fn get_trace_ref(&self, trace_id: &TraceId) -> Option<&BlockRef> {
        self.block_index.get(trace_id)
    }

    pub fn get_trace(&self, trace_id: &TraceId) -> Vec<Span> {
        let mem_spans = self.corvus_engine.fetch_trace(trace_id);
        let block_storage_spans: Vec<Span> = self
            .storage_search
            .search(&SearchRequest::for_trace_id(trace_id))
            .expect("An error occurred. Can not search for block storage");

        let spans = merge_spans(mem_spans, block_storage_spans);
        spans
    }

    pub fn get_services(&self) -> Vec<String> {
        let services = self.corvus_engine.fetch_services();
        services
    }

    pub fn search(&self, search_request: SearchRequest) -> Result<Vec<Span>, StorageError> {
        let mut spans_result: Vec<Span> = Vec::new();
        let limit = search_request.limit.unwrap_or(usize::MAX);
        let spans = self.corvus_engine.search(&search_request);

        spans_result.extend(spans);

        if spans_result.len() < limit {
            debug!(
                "Spans in memtable {} less than limit {}. Search in block storage.",
                spans_result.len(),
                limit
            );
            let storage_request = SearchRequest {
                trace_id: search_request.trace_id.clone(),
                service_name: search_request.service_name,
                span_name: search_request.span_name,
                limit: Some(limit - spans_result.len()),
                end_ts: search_request.end_ts,
                lookback: search_request.lookback,
            };
            let storage_spans = self.storage_search.search(&storage_request)?;
            spans_result.extend(storage_spans);
        }

        Ok(spans_result)
    }

    pub fn query_by_time(&self, start_ts: u64, end_ts: u64) -> Result<Vec<Span>, StorageError> {
        let spans = self.corvus_engine.fetch_by_time(start_ts, end_ts);
        Ok(spans)
    }
}

fn merge_spans(mem_table_spans: Vec<Span>, block_storage_spans: Vec<Span>) -> Vec<Span> {
    let mut merged = mem_table_spans
        .into_iter()
        .chain(block_storage_spans)
        .collect::<Vec<_>>();

    merged.sort_by_key(|span| span.timestamp());
    merged
}

impl BlockRef {
    pub fn new(block_id: BlockId, offset: u64, length: u32) -> Self {
        Self {
            block_id,
            offset,
            length,
        }
    }
}
