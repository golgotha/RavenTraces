use std::sync::Arc;
use crate::block::{BloomFilterBlock, DataBlock};
use crate::errors::StorageError;
use crate::memtable::{Memtable};
use crate::sstable_writer::{SStableWriter, SStableWriterImpl};
use log::{info, trace};
use std::time::Instant;
use metrics::metrics;
use crate::bloom::bloom_filter::{BloomFilter, BloomFilterImpl};
use crate::index::service_name_index::ServiceNameIndex;

pub trait FlushWorker: Send + Sync {

    fn flush(&mut self, memtable: Memtable) -> Result<(), StorageError>;

}

pub enum FlushResult {
    Flushed,
}

pub struct DiskFlushWorker {
    table_writer: SStableWriterImpl,
    service_name_index: Arc<ServiceNameIndex>,
    max_block_size: usize,
}

impl DiskFlushWorker {
    pub fn new(table_writer: SStableWriterImpl, service_name_index: Arc<ServiceNameIndex>, max_block_size: usize) -> Self {
        Self {
            table_writer,
            service_name_index,
            max_block_size,
        }
    }

    fn flush_block(&mut self, block: DataBlock) -> Result<(), StorageError> {
        self.table_writer.write_block(&block)?;

        let block_meta = block.get_block_meta();
        let block_index = block.block_index();
        self.table_writer.flush_index(&block_meta, &block_index)?;

        let num_traces = block_index.entries().len();
        let mut bloom_filter = BloomFilterImpl::new(num_traces, 0.01);
        for trace_id in block_index.entries().keys() {
            bloom_filter.add(trace_id);
        }

        let bloom_filter_block = BloomFilterBlock::from_bloom_filter(bloom_filter);
        self.table_writer.flush_bloom_filter(&block_meta, bloom_filter_block)?;
        self.service_name_index.flush()?;
        Ok(())
    }
}

impl FlushWorker for DiskFlushWorker {

    fn flush(&mut self, memtable: Memtable) -> Result<(), StorageError> {
        info!("Flushing memtable generation: {}", memtable.generation());
        let flush_start_time = Instant::now();
        let entries = memtable.entries();

        let mut current_block = DataBlock::new(self.max_block_size);
        let mut  min_ts = u64::MAX;
        let mut max_ts = u64::MIN;

        for (_trace_key, entry)  in entries {
            let trace_id = entry.trace_id();
            let spans = entry.get_spans();
            trace!(
                "Flush trace {:?} ({:?} spans) to block: {}",
                trace_id.to_hex(),
                spans.len(),
                current_block
            );

            min_ts = min_ts.min(entry.max_ts());
            max_ts = max_ts.max(entry.max_ts());

            for span in spans {
                current_block.add_span(&trace_id, span);
            }

            if current_block.is_full() {
                current_block.set_start_ts(min_ts);
                current_block.set_end_ts(max_ts);
                self.flush_block(current_block)?;

                current_block = DataBlock::new(self.max_block_size);
                min_ts = u64::MAX;
                max_ts = u64::MIN;
            }
        }

        current_block.set_start_ts(min_ts);
        current_block.set_end_ts(max_ts);
        self.flush_block(current_block)?;

        metrics::MEMTABLE_FLUSHES.inc();
        metrics::MEMTABLE_FLUSH_DURATION_MS.inc_by(flush_start_time.elapsed().as_millis() as u64);
        Ok(())
    }
}
