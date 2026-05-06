use std::path::PathBuf;
use log::{info, trace};
use common::serialization::Writable;
use crate::block::{BlockId, BlockMeta, BloomFilterBlock, DataBlock, StorageMeta};
use crate::block_index::BlockIndex;
use crate::block_storage::{BlockStorage, LocalBlockStorage};
use crate::errors::StorageError;

pub trait SStableWriter {

    fn write_block(&mut self, block: &DataBlock) -> Result<usize, StorageError>;

    fn flush_index(&self, block_metadata: &BlockMeta, block_index: &BlockIndex) -> Result<(), StorageError>;
    
    fn flush_bloom_filter(&self, block_metadata: &BlockMeta, bloom_filter: BloomFilterBlock) -> Result<(), StorageError>;

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
        trace!("Creating a new block file: {}", block_id.to_string());
        self.storage.open(block_id)?;
        Ok(())
    }

    fn next_block(&mut self) -> Result<BlockMeta, StorageError>  {
        let block_metadata = BlockMeta::new(4 * 1024);

        let block_id = block_metadata.id.clone();
        trace!("Creating a new block file: {}", block_id.to_string());
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

        let mut storage_meta = match self.storage.read_storage_meta() {
            Ok(meta) => meta,
            Err(StorageError::NotFound(_)) => StorageMeta { blocks: Vec::new() },
            Err(e) => return Err(e),
        };
            
        storage_meta.blocks.push(block_id.to_string());
        self.storage.write_storage_meta(&storage_meta)?;

        Ok(written_bytes)
    }

    fn flush_index(&self, block_metadata: &BlockMeta, block_index: &BlockIndex) -> Result<(), StorageError> {
        let block_id = block_metadata.id.clone();
        self.storage.write_block_index(&block_id, block_index)?;
        self.storage.write_block_meta(&block_id, block_metadata)?;
        Ok(())
    }

    fn flush_bloom_filter(&self, block_metadata: &BlockMeta, bloom_filter: BloomFilterBlock) -> Result<(), StorageError> {
        self.storage.write_bloom_filter(&block_metadata.id, bloom_filter)?;
        Ok(())
    }
}