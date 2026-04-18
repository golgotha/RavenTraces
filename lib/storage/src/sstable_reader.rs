use std::path::PathBuf;
use std::sync::Arc;
use common::binary_readers::{read_n_bytes, read_u32};
use common::serialization::Readable;
use crate::block::{BlockEntry, BlockId, DataBlock, DataBlockBuilder, StorageMeta};
use crate::block_index::BlockIndex;
use crate::block_storage::{BlockStorage, LocalBlockStorage};
use crate::errors::StorageError;

pub trait SStableReader {

    fn read_block_slice(&self, block_id: &BlockId, offset: u64, size: u32) -> Result<DataBlock, StorageError>;

    fn read_block(&self, block_id: &BlockId) -> Result<DataBlock, StorageError>;

    fn read_block_index(&self, block_id: &BlockId) -> Result<BlockIndex, StorageError>;

    fn read_blocks_meta(&self) -> Result<StorageMeta, StorageError>;
}

pub struct SStableReaderImpl {
    storage: Box<dyn BlockStorage + Send + Sync>,
}

impl SStableReaderImpl {
    pub fn new(base_dir: PathBuf) -> Self {
        Self {
            storage: Box::new(LocalBlockStorage::new(base_dir)),
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

    fn read_block(&self, block_id: &BlockId) -> Result<DataBlock, StorageError> {
        let block_bytes= self.storage.read_block(block_id)?;
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

    fn read_block_index(&self, block_id: &BlockId) -> Result<BlockIndex, StorageError> {
        self.storage.read_block_index(block_id)
    }

    fn read_blocks_meta(&self) -> Result<StorageMeta, StorageError> {
        let meta = self.storage.read_storage_meta()?;
        Ok(meta)
    }
}