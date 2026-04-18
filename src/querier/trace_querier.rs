use crate::querier::model::SearchRequest;
use common::binary_readers::{read_n_bytes, read_u32};
use log::{error, info};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use storage::block::BlockId;
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

            for entry in block_index.entries() {
                let block_ref = BlockRef::new(block.clone(), entry.offset(), entry.length());
                self.block_index.insert(entry.trace_id(), block_ref);
            }
        }

        Ok(())
    }

    pub fn get_trace_ref(&self, trace_id: &TraceId) -> Option<&BlockRef> {
        self.block_index.get(trace_id)
    }

    pub fn get_trace(&self, trace_id: &TraceId) -> Option<Vec<Span>> {
        let read_mem_table = match self.mem_table.read() {
            Ok(guard) => guard,
            Err(e) => {
                error!("Can not aquire a read lock for mem_table");
                None
            }?,
        };
        let mem_table_spans = read_mem_table.get_index(trace_id);
        let block_storage_spans: Option<Vec<Span>> = None;
        let spans = merge_spans(mem_table_spans, block_storage_spans);
        // self.get_trace_ref(trace_id)
        //     .map(|block_ref| {
        //         let block_id = &block_ref.block_id;
        //         let offset = block_ref.offset;
        //         let length = block_ref.length;
        //         let block_data = self.storage.read_block_at(block_id, offset, length)
        //             .unwrap();
        //         return read_block_entries(&block_data);
        //     })
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

        let spans = match (&service_name) {
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
}

fn merge_spans(
    mem_table_spans: Option<Vec<Span>>,
    block_storage_spans: Option<Vec<Span>>,
) -> Option<Vec<Span>> {
    mem_table_spans
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
