use std::path::PathBuf;
use log::info;
use common::serialization::Writable;
use crate::block::{BlockId, BlockMeta, DataBlock};
use crate::block_index::BlockIndex;
use crate::block_storage::{BlockStorage, LocalBlockStorage};
use crate::errors::StorageError;

pub trait SStableWriter {

    fn write_block(&mut self, block: &DataBlock) -> Result<usize, StorageError>;

    fn flush_index(&self, block_metadata: &BlockMeta, block_index: &BlockIndex) -> Result<(), StorageError>;

}

pub struct SStableWriterImpl {
    storage: Box<dyn BlockStorage + Send + Sync>,
}

impl SStableWriterImpl {
    pub fn new(base_dir: PathBuf) -> Self {
        Self {
            storage: Box::new(LocalBlockStorage::new(base_dir)),
        }
    }

    fn open_block(&mut self, block_id: &BlockId) -> Result<(), StorageError> {
        info!("Creating a new block file: {}", block_id.to_string());
        self.storage.open(block_id)?;
        Ok(())
    }

    fn next_block(&mut self) -> Result<BlockMeta, StorageError>  {
        let block_metadata = BlockMeta::new(4 * 1024);

        let block_id = block_metadata.id.clone();
        info!("Creating a new block file: {}", block_id.to_string());
        self.storage.open(&block_id)?;
        Ok(block_metadata)
    }
}

impl SStableWriter for SStableWriterImpl {

    fn write_block(&mut self, block: &DataBlock) -> Result<usize, StorageError> {
        let block_id = block.id();
        self.open_block(block_id)?;
        let spans = block.spans();

        let mut written_bytes: usize = 0;
        spans.iter()
            .for_each(|entry| {
                let block_data = entry.serialize();
                written_bytes += block_data.len();
                self.storage.write_block(block_id, &block_data).expect("Error occurred while writing a block");
            });

        Ok(written_bytes)
    }

    fn flush_index(&self, block_metadata: &BlockMeta, block_index: &BlockIndex) -> Result<(), StorageError> {
        let block_id = block_metadata.id.clone();
        self.storage.write_block_index(&block_id, block_index)?;
        self.storage.write_block_meta(&block_id, block_metadata)?;
        Ok(())
    }
}