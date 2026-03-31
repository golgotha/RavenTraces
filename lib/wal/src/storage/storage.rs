use std::fs::{File, OpenOptions};
use std::io::{Read, Write, BufWriter, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use log::{info, warn, debug, error};

use crate::errors::WalError;
use crate::storage::{Readable, Storage, Writable};

pub struct FileStorage {
    writer: BufWriter<File>,
    reader: BufReader<File>,
    path: PathBuf,
    file_size: u64,
}

impl FileStorage {

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WalError> {
        let path_buf = path.as_ref().to_path_buf();
        info!("Opening file: {}", path_buf.display());

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path_buf)?;

        let file_size = file.metadata()?.len();
        // Reader handle
        let mut reader = BufReader::new(file.try_clone()?);
        reader.seek(SeekFrom::Start(0))?;

        // Writer handle
        let writer = BufWriter::new(file);

        let storage = Self {
            path: path_buf,
            writer,
            reader,
            file_size
        };

        Ok(storage)
    }

    /// Returns the size of currently open file
    pub fn size(&self) -> u64 {
        self.file_size
    }
}

impl Storage for FileStorage {

    fn write<T: Writable>(&mut self, record: &T) -> Result<usize, WalError> {
        // println!("Write a log entry into the WAL");
        let bytes = record.serialize();
        self.writer.write_all(&bytes)?;
        // println!("Wrote {} bytes to WAL", bytes.len());
        Ok(bytes.len())
    }

    fn read<T: Readable>(&mut self) -> Result<T, WalError> {
        let mut buffer = vec![0u8; T::num_bytes_to_read()];
        let n = self.reader.read(&mut buffer)?;
        T::deserialize(&buffer)
    }

    fn read_at<T: Readable>(&mut self, offset: u64) -> Result<T, WalError> {
        self.reader.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; T::num_bytes_to_read()];
        self.reader.read_exact(&mut buffer)?;
        T::deserialize(&buffer)
    }
}