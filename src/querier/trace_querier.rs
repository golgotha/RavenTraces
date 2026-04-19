use crate::querier::model::SearchRequest;
use common::binary_readers::{read_n_bytes, read_u32};
use log::{error, info, warn};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use storage::block::BlockId;
use storage::block_index;
use storage::block_index::BlockIndexEntry;
use storage::errors::StorageError;
use storage::memtable::{Entry, Memtable};
use storage::span::{Span, TraceId};
use storage::sstable_reader::{SStableReader, SStableReaderImpl};

pub struct BlockRef {
    block_id: BlockId,
    offset: u64,
    length: u32,
}

pub struct TraceQuerier {
    mem_table: Arc<RwLock<Memtable>>,
    storage: Box<dyn SStableReader + Send + Sync>,
    block_index: HashMap<TraceId, BlockRef>,
}

impl TraceQuerier {
    pub fn new(base_dir: PathBuf, mem_table: Arc<RwLock<Memtable>>) -> TraceQuerier {
        Self {
            mem_table,
            storage: Box::new(SStableReaderImpl::new(base_dir)),
            block_index: HashMap::new(),
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
        let read_mem_table = match self.mem_table.read() {
            Ok(guard) => guard,
            Err(_) => {
                error!("Can not aquire a read lock for mem_table");
                return vec![];
            },
        };
        let mem_table_spans = read_mem_table.get_index(trace_id);
        let block_storage_spans: Vec<Span> = self.get_trace_spans_from_storage(trace_id)
            .unwrap_or(Vec::new());

        let spans = merge_spans(mem_table_spans, block_storage_spans);
        spans
    }

    pub fn get_services(&self) -> Vec<String> {
        self.mem_table.read().unwrap().services()
    }

    pub fn search(&self, search_request: &SearchRequest) -> Result<Vec<Span>, StorageError> {
        let mut spans_result: Vec<Span> = Vec::new();
        let service_name = search_request.service_name.clone();
        let span_name = search_request.span_name.clone();
        let limit = search_request.limit;

        let spans = match &service_name {
            Some(service_name) => self.mem_table
                .read()
                .unwrap()
                .get_spans_by_service(&service_name, limit.unwrap_or(usize::MAX))
                .unwrap_or_default(),
            None =>  self.mem_table
                .read()
                .unwrap()
                .entries()
                .iter()
                .map(|entry: &Entry| entry.get_span())
                .take(limit.unwrap_or(usize::MAX))
                .cloned()
                .collect()
        };

        let spans = spans
            .into_iter()
            .filter(|span| match &span_name {
                Some(name) => span.name == name.as_str(),
                None => true,
            })
            .collect::<Vec<Span>>();

        spans_result.extend(spans);

        Ok(spans_result)
    }

    pub fn query_by_time(&self, start_ts: u64, end_ts: u64) -> Result<Vec<Span>, StorageError> {
        let spans = self
            .mem_table
            .read()
            .unwrap()
            .query_by_time(start_ts, end_ts);

        Ok(spans)
    }

    fn get_trace_spans_from_storage(&self, trace_id: &TraceId) -> Result<Vec<Span>, StorageError> {
        let storage_meta = match self.storage.read_blocks_meta() {
            Ok(meta) => meta,
            Err(_) => {
                error!("Error occurred while reading storage metadata");
                return Ok(vec![]);
            }
        };

        let trace_spans = Vec::new();
        for block in storage_meta.blocks {
            let block_id = BlockId::new(block);
            let block_index = self.storage.read_block_index(&block_id)?;

            let Some(entry) = block_index.find_trace_id(trace_id) else {
                continue;
            };

            let block_data = self.storage.read_block_slice(&block_id, entry.offset(), entry.length())?;
            let block_spans = block_data.spans();
            let spans = block_spans.iter()
                .map(|entry| Span::deserialize(entry.payload().to_vec()))
                .collect();

            return Ok(spans);
        }
        Ok(trace_spans)
    }
}


fn merge_spans(
    mem_table_spans: Vec<Span>,
    block_storage_spans: Vec<Span>,
) -> Vec<Span> {
    let mut merged = mem_table_spans
        .into_iter()
        .chain(block_storage_spans)
        .collect::<Vec<_>>();

    merged.sort_by_key(|span| span.timestamp());
    merged
}

fn read_block_entries(data: &Vec<u8>) -> Vec<Span> {
    let mut offset = 0;
    let mut spans = Vec::<Span>::new();
    while offset < data.len() {
        let payload_size = read_u32(data, &mut offset).unwrap();
        let payload = read_n_bytes(data, &mut offset, payload_size as usize);
        let span = Span::deserialize(payload);
        spans.push(span);
    }

    spans
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
