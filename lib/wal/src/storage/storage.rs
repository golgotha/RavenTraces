use std::fs::{File, OpenOptions};
use std::io::{Read, Write, BufWriter, BufReader, Seek, SeekFrom};
use std::path::{Path};
use log::{info};

use crate::errors::WalError;
use crate::storage::{Readable, Storage, Writable};

pub struct FileStorage {
    writer: BufWriter<File>,
    reader: BufReader<File>,
    file_size: u64,
}

impl FileStorage {

    pub fn open<P: AsRef<Path>>(path: P, append: bool) -> Result<Self, WalError> {
        let path_buf = path.as_ref().to_path_buf();
        info!("Opening file: {}", path_buf.display());

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(append)
            .open(&path_buf)?;

        let file_size = file.metadata()?.len();
        // Reader handle
        let mut reader = BufReader::new(file.try_clone()?);
        reader.seek(SeekFrom::Start(0))?;

        // Writer handle
        let writer = BufWriter::new(file);

        let storage = Self {
            writer,
            reader,
            file_size
        };

        Ok(storage)
    }

    pub fn exists<P: AsRef<Path>>(path: P) -> bool {
        let path = path.as_ref();
        path.exists()
    }

    pub fn delete<P: AsRef<Path>>(path: P) -> Result<(), WalError> {
        let path = path.as_ref();
        info!("Deleting segment file: {}", path.display());
        std::fs::remove_file(path)?;
        Ok(())
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
        self.writer.flush()?;
        // println!("Wrote {} bytes to WAL", bytes.len());
        Ok(bytes.len())
    }

    fn read<T: Readable>(&mut self) -> Result<T, WalError> {
        let mut buffer = vec![0u8; T::num_bytes_to_read()];
        self.reader.read(&mut buffer)?;
        T::deserialize(&buffer)
    }

    fn read_at<T: Readable>(&mut self, offset: u64) -> Result<T, WalError> {
        self.reader.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; T::num_bytes_to_read()];
        self.reader.read_exact(&mut buffer)?;
        T::deserialize(&buffer)
    }

    fn read_bytes_at(&mut self, offset: u64, size: usize) -> Result<Vec<u8>, WalError> {
        self.reader.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; size];
        self.reader.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    fn flush(&mut self) -> Result<(), WalError> {
        Ok(self.writer.flush()?)
    }
}