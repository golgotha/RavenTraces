use crate::block::{BlockId, BlockMeta, StorageMeta};
use crate::block_index::{BlockIndex, BlockIndexEntry};
use crate::errors::StorageError;
use common::serialization::{Readable, Writable};
use log::info;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const BLOCKS_DIR_NAME: &str = "blocks";
const INDEX_FILE_NAME: &str = "index.bin";
const BLOCK_DATA_FILE_NAME: &str = "data.bin";
const META_FILE_NAME: &str = "meta.json";

pub trait BlockStorage: Send + Sync {
    fn open(&mut self, id: &BlockId) -> Result<(), StorageError>;

    fn write_block(&self, id: &BlockId, data: &[u8]) -> Result<(), StorageError>;

    fn write_block_index(&self, id: &BlockId, index: &BlockIndex) -> Result<(), StorageError>;

    fn write_block_meta(
        &self,
        block_id: &BlockId,
        block_meta: &BlockMeta,
    ) -> Result<(), StorageError>;

    fn read_block_at(
        &self,
        block_id: &BlockId,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, StorageError>;

    fn read_block(&self, block_id: &BlockId) -> Result<Vec<u8>, StorageError>;

    fn read_block_index(&self, id: &BlockId) -> Result<BlockIndex, StorageError>;

    fn list_blocks(&self) -> Result<Vec<BlockId>, StorageError>;

    fn read_storage_meta(&self) -> Result<StorageMeta, StorageError>;

    fn write_storage_meta(&self, meta: &StorageMeta) -> Result<(), StorageError>;
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
    mutex: Mutex<u32>,
}

impl LocalBlockStorage {
    pub fn new<P: AsRef<Path>>(dir: P) -> Self {
        LocalBlockStorage {
            base_dir: dir.as_ref().to_path_buf(),
            current_block: None,
            mutex: Mutex::new(0),
        }
    }

    fn create_data_file(&self, dir_path: &PathBuf) -> Result<File, StorageError> {
        let block_path = dir_path.join(BLOCK_DATA_FILE_NAME);
        let block_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&block_path)?;
        Ok(block_file)
    }

    fn create_bloom_filter_file(&self, dir_path: &PathBuf) -> Result<File, StorageError> {
        let bloom_path = dir_path.join("bloom.bin");
        let bloom_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&bloom_path)?;
        Ok(bloom_file)
    }

    fn create_index_file(&self, dir_path: &PathBuf) -> Result<File, StorageError> {
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
    fn open(&mut self, id: &BlockId) -> Result<(), StorageError> {
        let block_dir_path = self.base_dir.join(BLOCKS_DIR_NAME).join(id.to_string());

        if !block_dir_path.exists() {
            info!("Create {:?} directory. ", &block_dir_path);
            fs::create_dir_all(&block_dir_path)?;
        }

        let data_file = self.create_data_file(&block_dir_path)?;
        let bloom_file = self.create_bloom_filter_file(&block_dir_path)?;
        let index_file = self.create_index_file(&block_dir_path)?;
        let block_ref = BlockReference {
            data_file,
            bloom_file,
            index_file,
        };
        self.current_block = Some(Mutex::new(block_ref));
        Ok(())
    }

    fn write_block(&self, id: &BlockId, data: &[u8]) -> Result<(), StorageError> {
        let _unused = self.mutex.lock().unwrap();
        let mut data_file = &self
            .current_block
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .data_file;
        data_file.write(data)?;
        data_file.flush()?;
        Ok(())
    }

    fn write_block_index(
        &self,
        block_id: &BlockId,
        index: &BlockIndex,
    ) -> Result<(), StorageError> {
        info!("Writing block index for id {}", block_id.id.to_string());
        let _guard = self.mutex.lock().unwrap();

        let data_vec: Vec<u8> = index
            .entries()
            .iter()
            .map(|entry| entry.serialize())
            .flatten()
            .collect();

        let mut data_file = &self
            .current_block
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .index_file;
        data_file.write(&data_vec)?;
        data_file.flush()?;
        Ok(())
    }

    fn write_block_meta(
        &self,
        block_id: &BlockId,
        block_meta: &BlockMeta,
    ) -> Result<(), StorageError> {
        let _unused = self.mutex.lock().unwrap();
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

    fn read_block(&self, block_id: &BlockId) -> Result<Vec<u8>, StorageError> {
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
        meta_file.write(&bytes)?;

        Ok(())
    }
}
