use crate::block::{BlockId, BlockMeta, BloomFilterBlock, StorageMeta};
use crate::block_index::{BlockIndex, BlockIndexEntry};
use crate::errors::StorageError;
use common::serialization::{Readable, Writable};
use log::{info, trace};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const BLOCKS_DIR_NAME: &str = "blocks";
const INDEX_FILE_NAME: &str = "index.bin";
const BLOOM_FILTER_FILE_NAME: &str = "bloom.bin";
const BLOCK_DATA_FILE_NAME: &str = "data.bin";
const META_FILE_NAME: &str = "meta.json";

type BlockStorageResult<T> = Result<T, StorageError>;

pub trait BlockStorage: Send + Sync {
    fn open(&mut self, id: &BlockId) -> BlockStorageResult<()>;

    fn write_block(&self, id: &BlockId, data: &[u8]) -> BlockStorageResult<()>;

    fn write_block_index(&self, id: &BlockId, index: &BlockIndex) -> BlockStorageResult<()>;

    fn write_block_meta(
        &self,
        block_id: &BlockId,
        block_meta: &BlockMeta,
    ) -> Result<(), StorageError>;

    fn write_storage_meta(&self, meta: &StorageMeta) -> BlockStorageResult<()>;

    fn write_bloom_filter(&self, block_id: &BlockId, bloom_filter: BloomFilterBlock) -> BlockStorageResult<()>;

    fn read_block_at(
        &self,
        block_id: &BlockId,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, StorageError>;

    fn read_block_bytes(&self, block_id: &BlockId) -> BlockStorageResult<Vec<u8>>;

    fn read_block(&self, block_id: &BlockId, offset: u64) -> BlockStorageResult<Box<dyn Read + Send>>;
    
    fn read_block_len(&self, block_id: &BlockId, offset: u64, len: u64) -> BlockStorageResult<Box<dyn Read + Send>>;

    fn read_block_index(&self, id: &BlockId) -> BlockStorageResult<BlockIndex>;

    fn read_bloom_filter(&self, id: &BlockId) -> BlockStorageResult<BloomFilterBlock>;

    fn list_blocks(&self) -> BlockStorageResult<Vec<BlockId>>;

    fn read_storage_meta(&self) -> BlockStorageResult<StorageMeta>;

}

#[derive(Debug)]
struct BlockReference {
    data_file: File,
    bloom_file: File,
    index_file: File,
}

#[derive(Debug)]
pub struct LocalBlockStorage {
    base_dir: PathBuf,
    current_block: Option<Mutex<BlockReference>>,
}

impl LocalBlockStorage {
    pub fn new<P: AsRef<Path>>(dir: P) -> Self {
        LocalBlockStorage {
            base_dir: dir.as_ref().to_path_buf(),
            current_block: None,
        }
    }

    fn create_data_file(&self, dir_path: &PathBuf) -> BlockStorageResult<File> {
        let block_path = dir_path.join(BLOCK_DATA_FILE_NAME);
        let block_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&block_path)?;
        Ok(block_file)
    }

    fn create_bloom_filter_file(&self, dir_path: &PathBuf) -> BlockStorageResult<File> {
        let bloom_path = dir_path.join(BLOOM_FILTER_FILE_NAME);
        let bloom_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&bloom_path)?;
        Ok(bloom_file)
    }

    fn create_index_file(&self, dir_path: &PathBuf) -> BlockStorageResult<File> {
        let index_path = dir_path.join(INDEX_FILE_NAME);
        let index_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&index_path)?;
        Ok(index_file)
    }
}

impl BlockStorage for LocalBlockStorage {
    fn open(&mut self, id: &BlockId) -> BlockStorageResult<()> {
        let block_dir_path = self.base_dir.join(BLOCKS_DIR_NAME).join(id.to_string());

        if !block_dir_path.exists() {
            trace!("Create {:?} directory. ", &block_dir_path);
            fs::create_dir_all(&block_dir_path)?;
        }

        Ok(())
    }

    fn write_block(&self, id: &BlockId, data: &[u8]) -> BlockStorageResult<()> {
        let block_dir_path = self.base_dir.join(BLOCKS_DIR_NAME).join(id.to_string());

        if !block_dir_path.exists() {
            trace!("Create {:?} directory. ", &block_dir_path);
            fs::create_dir_all(&block_dir_path)?;
        }

        let mut data_file = self.create_data_file(&block_dir_path)?;
        data_file.write_all(data)?;
        data_file.flush()?;
        Ok(())
    }

    fn write_block_index(
        &self,
        block_id: &BlockId,
        index: &BlockIndex,
    ) -> Result<(), StorageError> {
        trace!("Writing block index for id {}", block_id.id.to_string());
        let data_vec: Vec<u8> = index
            .entries()
            .values()
            .into_iter()
            .map(|entry| entry.serialize())
            .flatten()
            .collect();

        let block_dir_path = self.base_dir.join(BLOCKS_DIR_NAME).join(block_id.to_string());

        if !block_dir_path.exists() {
            trace!("Create {:?} directory. ", &block_dir_path);
            fs::create_dir_all(&block_dir_path)?;
        }

        let mut index_file = self.create_index_file(&block_dir_path)?;

        index_file.write(&data_vec)?;
        index_file.flush()?;
        Ok(())
    }

    fn write_block_meta(
        &self,
        block_id: &BlockId,
        block_meta: &BlockMeta,
    ) -> Result<(), StorageError> {
        let block_path = self
            .base_dir
            .join(BLOCKS_DIR_NAME)
            .join(block_id.id.to_string());

        if !block_path.exists() {
            return Err(StorageError::NotFound(
                format!("Block {} doesn't exist", block_id.id.to_string()).into(),
            ));
        }

        let meta_file_path = block_path.join(META_FILE_NAME);
        let mut meta_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&meta_file_path)?;

        let json_string = serde_json::to_string(block_meta).unwrap();

        let bytes = json_string.into_bytes();
        meta_file.write(&bytes)?;
        Ok(())
    }

    fn write_storage_meta(&self, meta: &StorageMeta) -> Result<(), StorageError> {
        let meta_path = self.base_dir.join(BLOCKS_DIR_NAME).join("meta.json");

        let mut meta_file: File;
        if !meta_path.exists() {
            info!("Meta storage doesn't exist. Create a new one.");
            meta_file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&meta_path)?;
        } else {
            meta_file = File::create(&meta_path)?
        }

        let json_string = serde_json::to_string(meta).unwrap();
        let bytes = json_string.into_bytes();
        meta_file.write_all(&bytes)?;

        Ok(())
    }

    fn write_bloom_filter(&self, block_id: &BlockId, bloom_filter: BloomFilterBlock) -> BlockStorageResult<()> {
        let block_dir_path = self.base_dir.join(BLOCKS_DIR_NAME).join(block_id.to_string());

        if !block_dir_path.exists() {
            trace!("Create {:?} directory. ", &block_dir_path);
            fs::create_dir_all(&block_dir_path)?;
        }

        let mut bloom_file = self.create_bloom_filter_file(&block_dir_path)?;

        let bloom_vector = bloom_filter.serialize();
        bloom_file.write_all(&bloom_vector)?;
        bloom_file.flush()?;
        Ok(())
    }

    fn read_block_at(
        &self,
        block_id: &BlockId,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, StorageError> {
        let block_path = self
            .base_dir
            .join(BLOCKS_DIR_NAME)
            .join(block_id.id.to_string())
            .join(BLOCK_DATA_FILE_NAME);

        if !block_path.exists() {
            return Err(StorageError::NotFound(
                format!("Block {} doesn't exist", block_id.id.to_string()).into(),
            ));
        }

        let block_file_size = block_path.metadata()?.len();
        if offset + size as u64 > block_file_size {
            return Err(StorageError::CorruptedEntry(
                format!(
                    "Reading more data than block has. Block size: {}",
                    block_file_size
                )
                .into(),
            ));
        }

        let mut file = File::open(&block_path)?;
        let mut buffer = vec![0u8; size as usize];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    fn read_block_bytes(&self, block_id: &BlockId) -> Result<Vec<u8>, StorageError> {
        let block_path = self
            .base_dir
            .join(BLOCKS_DIR_NAME)
            .join(block_id.id.to_string())
            .join(BLOCK_DATA_FILE_NAME);

        if !block_path.exists() {
            return Err(StorageError::NotFound(
                format!("Block {} doesn't exist", block_id.id.to_string()).into(),
            ));
        }

        let block_file_size = block_path.metadata()?.len();

        let mut file = File::open(&block_path)?;
        let mut buffer = vec![0u8; block_file_size as usize];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    fn read_block(&self, block_id: &BlockId, offset: u64) -> BlockStorageResult<Box<dyn Read + Send>> {
        let block_path = self
            .base_dir
            .join(BLOCKS_DIR_NAME)
            .join(block_id.id.to_string())
            .join(BLOCK_DATA_FILE_NAME);

        if !block_path.exists() {
            return Err(StorageError::NotFound(
                format!("Block {} doesn't exist", block_id.id.to_string()).into(),
            ));
        }

        let block_file_size = block_path.metadata()?.len();

        let mut file = File::open(&block_path)?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| StorageError::StorageReadError(e.to_string()))?;

        Ok(Box::new(file.take(block_file_size)))
    }

    fn read_block_len(&self, block_id: &BlockId, offset: u64, len: u64) -> BlockStorageResult<Box<dyn Read + Send>> {
        let block_path = self
            .base_dir
            .join(BLOCKS_DIR_NAME)
            .join(block_id.id.to_string())
            .join(BLOCK_DATA_FILE_NAME);

        if !block_path.exists() {
            return Err(StorageError::NotFound(
                format!("Block {} doesn't exist", block_id.id.to_string()).into(),
            ));
        }

        let block_file_size = block_path.metadata()?.len();
        if len > block_file_size {
            return Err(StorageError::BlockReadError(
                format!("Block size {} is less that len {}", block_file_size, len).into(),
            ));
        }

        let mut file = File::open(&block_path)?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| StorageError::StorageReadError(e.to_string()))?;

        Ok(Box::new(file.take(len)))
    }

    fn read_block_index(&self, block_id: &BlockId) -> Result<BlockIndex, StorageError> {
        let block_path = self
            .base_dir
            .join(BLOCKS_DIR_NAME)
            .join(block_id.id.to_string())
            .join(INDEX_FILE_NAME);

        if !block_path.exists() {
            return Err(StorageError::NotFound(
                format!("Block {} doesn't exist", block_id.id.to_string()).into(),
            ));
        }

        let mut index_file = File::open(&block_path)?;

        let file_size = index_file.metadata()?.len();
        index_file.seek(SeekFrom::Start(0))?;
        let mut offset = 0;
        let block_size = 28;

        let mut block_index = BlockIndex::new();

        while offset + block_size <= file_size {
            let mut buffer = [0u8; 28];
            index_file.read_at(&mut buffer, offset)?;

            let index_entry =
                BlockIndexEntry::deserialize(&buffer).expect("Can not deserialize index entry");
            block_index.insert(index_entry);
            offset += block_size
        }

        Ok(block_index)
    }

    fn read_bloom_filter(&self, block_id: &BlockId) -> BlockStorageResult<BloomFilterBlock> {
        trace!("Reading bloom filter for block {}", block_id.id);
        let bloom_filter_path = self
            .base_dir
            .join(BLOCKS_DIR_NAME)
            .join(block_id.id.to_string())
            .join(BLOOM_FILTER_FILE_NAME);

        if !bloom_filter_path.exists() {
            return Err(StorageError::NotFound(
                format!("Bloom filter for block {} doesn't exist", block_id.id.to_string()).into(),
            ));
        }

        let mut bloom_filter_file = File::open(&bloom_filter_path)?;
        let file_size = bloom_filter_file.metadata()?.len();
        let mut buffer = vec![0u8; file_size as usize];

        bloom_filter_file.seek(SeekFrom::Start(0))?;
        bloom_filter_file.read_exact(&mut buffer)?;
        let bloom_filter_block = BloomFilterBlock::deserialize(&buffer)
            .map_err(|e| StorageError::StorageReadError(e.to_string()))?;

        Ok(bloom_filter_block)
    }

    fn list_blocks(&self) -> Result<Vec<BlockId>, StorageError> {
        let blocks_path = self.base_dir.join(BLOCKS_DIR_NAME);

        let block_dirs: Vec<BlockId> = fs::read_dir(&blocks_path)?
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name();
                let name = name.to_str()?;
                Some(BlockId::new(name.to_string()))
            })
            .collect();

        Ok(block_dirs)
    }

    fn read_storage_meta(&self) -> Result<StorageMeta, StorageError> {
        let meta_path = self.base_dir.join(BLOCKS_DIR_NAME).join("meta.json");

        if !meta_path.exists() {
            return Err(StorageError::NotFound(
                "Storage metadata meta.json doesn't exist".to_string().into(),
            ));
        }

        let file = File::open(meta_path)?;
        let reader = BufReader::new(file);
        let meta: StorageMeta = serde_json::from_reader(reader)
            .map_err(|e| StorageError::CorruptedEntry(format!("Can not open meta file: {}", e.to_string())))?;
        Ok(meta)
    }
}
