use std::io::{BufReader, Read};
use std::path::{Path};
use std::sync::Arc;
use common::binary_readers::{read_n_bytes, read_u32};
use common::serialization::Readable;
use crate::block::{BlockEntry, BlockId, BlockMeta, DataBlock, DataBlockBuilder, StorageMeta};
use crate::block_index::BlockIndex;
use crate::block_storage::{BlockStorage, LocalBlockStorage};
use crate::bloom::bloom_filter::BloomFilterImpl;
use crate::errors::StorageError;
use crate::readers::reader_utils::{read_exact_bytes, try_read_u32};

pub type BlockIteratorResult = Result<Box<dyn Iterator<Item = Result<BlockEntry, StorageError>> + Send>, StorageError>;

pub trait SStableReader {

    fn read_block_slice(&self, block_id: &BlockId, offset: u64, size: u32) -> Result<DataBlock, StorageError>;
    
    fn read_block_slice_iter(&self, block_id: &BlockId, offset: u64, size: u64) -> BlockIteratorResult;

    fn read_block(&self, block_id: &BlockId) -> Result<DataBlock, StorageError>;

    fn read_block_iter(&self, block_id: &BlockId, offset: u64) -> BlockIteratorResult;

    fn read_block_index(&self, block_id: &BlockId) -> Result<BlockIndex, StorageError>;

    fn read_block_meta(&self, block_id: &BlockId) -> Result<BlockMeta, StorageError>;
    
    fn read_bloom_filter(&self, block_id: &BlockId) -> Result<BloomFilterImpl, StorageError>;

    fn read_blocks_meta(&self) -> Result<StorageMeta, StorageError>;
}

pub struct BlockEntryIterator<R: Read> {
    reader: BufReader<R>,
    finished: bool,
}

pub struct SStableReaderImpl {
    storage: Box<dyn BlockStorage + Send + Sync>,
}

impl SStableReaderImpl {
    pub fn new<P: AsRef<Path>>(base_dir: P) -> Self {
        Self {
            storage: Box::new(LocalBlockStorage::new(base_dir)),
        }
    }
}

impl<R: Read> BlockEntryIterator<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            finished: false,
        }
    }
}

impl<R: Read> Iterator for BlockEntryIterator<R> {
    type Item = Result<BlockEntry, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let payload_size = match try_read_u32(&mut self.reader) {
            Ok(Some(size)) => size,
            Ok(None) => {
                self.finished = true;
                return None;
            }
            Err(e) => {
                self.finished = true;
                return Some(Err(e));
            }
        };

        match read_exact_bytes(&mut self.reader, payload_size as usize) {
            Ok(payload) => {
                let entry = BlockEntry::new(payload_size, Arc::from(payload));
                Some(Ok(entry))
            }
            Err(e) => {
                self.finished = true;
                Some(Err(e))
            }
        }
    }
}

impl SStableReader for SStableReaderImpl {

    fn read_block_slice(&self, block_id: &BlockId, offset: u64, size: u32) -> Result<DataBlock, StorageError> {
        let block_bytes= self.storage.read_block_at(block_id, offset, size)?;
        let block_entry = BlockEntry::deserialize(&block_bytes)
            .map_err(|e| StorageError::StorageReadError(e.to_string()))?;

        let data_block = DataBlockBuilder::new()
            .block_id(block_id.clone())
            .add_entry(block_entry)
            .build();
        Ok(data_block)
    }

    fn read_block_slice_iter(&self, block_id: &BlockId, offset: u64, size: u64) -> BlockIteratorResult {
        let reader = self.storage.read_block_len(block_id, offset, size)?;
        let iterator = BlockEntryIterator::new(reader);
        Ok(Box::new(iterator))
    }

    fn read_block(&self, block_id: &BlockId) -> Result<DataBlock, StorageError> {
        let block_bytes= self.storage.read_block_bytes(block_id)?;
        let block_size = block_bytes.len();

        let mut offset = 0;
        let mut block_builder = DataBlockBuilder::new()
            .block_id(block_id.clone());
        while offset < block_size {
            let payload_size= read_u32(&block_bytes, &mut offset)
                .map_err(|e| StorageError::StorageReadError(e.to_string()))?;

            let payload = read_n_bytes(&block_bytes, &mut offset, payload_size as usize);
            let block_entry = BlockEntry::new(payload_size, Arc::from(payload));
            block_builder = block_builder.add_entry(block_entry);
        }

        let data_block = block_builder.build();
        Ok(data_block)
    }

    fn read_block_iter(&self, block_id: &BlockId, offset: u64) -> BlockIteratorResult {
        let reader = self.storage.read_block(block_id, offset)?;
        let iterator = BlockEntryIterator::new(reader);
        Ok(Box::new(iterator))
    }

    fn read_block_index(&self, block_id: &BlockId) -> Result<BlockIndex, StorageError> {
        self.storage.read_block_index(block_id)
    }

    fn read_block_meta(&self, block_id: &BlockId) -> Result<BlockMeta, StorageError> {
        self.storage.read_block_meta(block_id)
    }

    fn read_bloom_filter(&self, block_id: &BlockId) -> Result<BloomFilterImpl, StorageError> {
        let bloom_filter_block = self.storage.read_bloom_filter(block_id)?;
        let bloom_filter = bloom_filter_block.get_filter();
        Ok(bloom_filter)
    }

    fn read_blocks_meta(&self) -> Result<StorageMeta, StorageError> {
        let meta = self.storage.read_storage_meta()?;
        Ok(meta)
    }
}