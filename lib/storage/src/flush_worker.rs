use crate::block::{BlockMeta, DataBlock};
use crate::block_index::{BlockIndex, BlockIndexEntry};
use crate::errors::StorageError;
use crate::memtable::{Entry, Memtable};
use crate::span::{Span, TraceId};
use crate::sstable_writer::{SStableWriter, SStableWriterImpl};
use log::{debug, info};
use std::collections::HashMap;
use std::sync::{Mutex};
use wal::wal::WAL;

pub trait FlushWorker: Send + Sync {

    fn flush(&mut self, wal: &mut WAL, memtable: &mut Memtable) -> Result<(), StorageError>;

}

pub enum FlushResult {
    Flushed,
}

pub struct DiskFlushWorker {
    table_writer: SStableWriterImpl,
    max_block_size: usize,
}

impl DiskFlushWorker {
    pub fn new(table_writer: SStableWriterImpl, max_block_size: usize) -> Self {
        Self {
            table_writer,
            max_block_size
        }
    }
}

impl FlushWorker for DiskFlushWorker {

    fn flush(&mut self, wal: &mut WAL, memtable: &mut Memtable) -> Result<(), StorageError> {
        info!("Flushing memtable");
        let max_segment_id = memtable.max_segment_id();
        let mut entries = memtable.entries();
        // spans.sort_by_key(|s| s.timestamp());

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

            let index_entry = BlockIndexEntry::builder()
                .trace_id(*trace_id)
                .offset(block_offset as u64)
                .length(spans_block_size as u32)
                .build()
                .expect("Error while building index entry");
            block_index.insert(index_entry);
            block_offset = block_size;

            if current_block.is_full() {
                {
                    let meta = current_block.get_block_meta();
                    meta.set_start_ts(min_ts);
                    meta.set_end_ts(max_ts);
                }

                self.table_writer
                    .write_block(&current_block)
                    .expect("Error while writing a block");

                self.table_writer
                    .flush_index(current_block.get_block_meta(), &block_index)
                    .expect("Error while flushing index");

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

        self.table_writer
            .write_block(&current_block)
            .expect("Error while writing a block");

        self.table_writer
            .flush_index(current_block.get_block_meta(), &block_index)
            .expect("Error while flushing index");

        wal.checkpoint(max_segment_id)
            .expect("cannot checkpoint out WAL");
        memtable.clear();
        // wal.cleanup().expect("Error occurred during WAL cleanup");
        Ok(())
    }
}
