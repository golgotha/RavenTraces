use crate::block::{BloomFilterBlock, DataBlock};
use crate::block_index::{BlockIndex, BlockIndexEntry};
use crate::errors::StorageError;
use crate::memtable::{Memtable};
use crate::span::{Span, TraceId};
use crate::sstable_writer::{SStableWriter, SStableWriterImpl};
use log::{debug, info};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;
use wal::wal::WAL;
use metrics::metrics;
use crate::bloom::bloom_filter::{BloomFilter, BloomFilterImpl};

pub trait FlushWorker: Send + Sync {

    fn flush(&mut self, wal: &mut WAL, memtable: &mut Memtable) -> Result<(), StorageError>;

}

pub enum FlushResult {
    Flushed,
}

pub struct DiskFlushWorker {
    table_writer: SStableWriterImpl,
    max_block_size: usize,
    mutex: Mutex<u32>,
}

impl DiskFlushWorker {
    pub fn new(table_writer: SStableWriterImpl, max_block_size: usize) -> Self {
        Self {
            table_writer,
            max_block_size,
            mutex: Mutex::new(0),
        }
    }

    fn flush_block(&mut self, block: &mut DataBlock, block_index: &BlockIndex) -> Result<(), StorageError> {
        self.table_writer
            .write_block(&block)
            .expect("Error while writing a block");

        self.table_writer
            .flush_index(block.get_block_meta(), &block_index)
            .expect("Error while flushing index");

        let num_traces = block_index.entries().len();
        let mut bloom_filter = BloomFilterImpl::new(num_traces, 0.01);
        for trace_id in block_index.entries().keys() {
            bloom_filter.add(trace_id);
        }

        let bloom_filter_block = BloomFilterBlock::from_bloom_filter(bloom_filter);
        self.table_writer
            .flush_bloom_filter(block.get_block_meta(), &bloom_filter_block)?;
        Ok(())
    }
}

impl FlushWorker for DiskFlushWorker {

    fn flush(&mut self, wal: &mut WAL, memtable: &mut Memtable) -> Result<(), StorageError> {
        // let _guard = self.mutex.lock().expect("Error while locking mutex");
        info!("Flushing memtable generation: {}", memtable.generation());
        let flush_start_time = Instant::now();
        let max_segment_id = memtable.max_segment_id();
        let entries = memtable.entries();

        let mut grouped: HashMap<TraceId, Vec<&Span>> = HashMap::new();
        for entry in entries {
            let span = entry.get_span();
            let trace_id = span.trace_id;

            grouped
                .entry(trace_id)
                .or_insert_with(Vec::new)
                .push(span);
        }

        for spans in grouped.values_mut() {
            spans.sort_by_key(|s| s.timestamp);
        }

        let mut block_offset: usize = 0;
        let mut block_index = BlockIndex::new();
        let mut current_block = DataBlock::new(self.max_block_size);

        let mut min_ts = u64::MAX;
        let mut max_ts = 0;
        for (trace_id, spans) in grouped.iter() {
            debug!(
                "Flush trace {:?} ({:?} spans) to block: {}",
                trace_id.to_hex(),
                spans.len(),
                current_block
            );

            for span in spans {
                let timestamp = span.timestamp();
                if timestamp < min_ts {
                    min_ts = timestamp;
                }

                if timestamp > max_ts {
                    max_ts = timestamp;
                }

                current_block.add_span(&span.serialize());
            }

            let block_size = current_block.block_size();
            let spans_block_size = block_size - block_offset;

            debug!(
                "Create index entry: offset: {}, size: {}",
                block_offset, block_size
            );

            let index_entry = create_block_entry(trace_id, block_offset as u64, spans_block_size as u32)
                .expect("Error while building index entry");
            block_index.insert(index_entry);
            block_offset = block_size;

            if current_block.is_full() {
                {
                    let meta = current_block.get_block_meta();
                    meta.set_start_ts(min_ts);
                    meta.set_end_ts(max_ts);
                }

                self.flush_block(&mut current_block, &block_index)?;

                current_block = DataBlock::new(self.max_block_size);
                block_index = BlockIndex::new();
                block_offset = 0;
                min_ts = 0;
                max_ts = 0;
            }
        }

        {
            let meta = current_block.get_block_meta();
            meta.set_start_ts(min_ts);
            meta.set_end_ts(max_ts);
        }

        self.flush_block(&mut current_block, &block_index)?;

        wal.checkpoint(max_segment_id)
            .expect("cannot checkpoint out WAL");
        wal.cleanup()
            .expect("cannot clean up wal");

        metrics::MEMTABLE_ENTRIES.set(0);
        metrics::MEMTABLE_SIZE_BYTES.set(0);
        metrics::MEMTABLE_FLUSHES.inc();
        metrics::MEMTABLE_FLUSH_DURATION_MS.inc_by(flush_start_time.elapsed().as_millis() as u64);
        Ok(())
    }
}

fn create_block_entry(trace_id: &TraceId, block_offset: u64, spans_block_size: u32) -> Result<BlockIndexEntry, String> {
    let index_entry = BlockIndexEntry::builder()
        .trace_id(*trace_id)
        .offset(block_offset as u64)
        .length(spans_block_size as u32)
        .build();
    index_entry
}