use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum StorageError {
    IoError(std::io::Error),
    CorruptedEntry(String),
    StorageAppendError(String),
    StorageReadError(String),
    StorageFull(String),
    StorageClosed(String),
    NotFound(String),
    NotAFile(String)
}

#[derive(Debug)]
pub enum EngineError {
    EngineError(String),
}

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        StorageError::IoError(err)
    }
}
impl Display for StorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::IoError(err) => write!(f, "I/O error: {}", err),
            StorageError::CorruptedEntry(msg) => write!(f, "Corrupted entry: {}", msg),
            StorageError::StorageAppendError(msg) => write!(f, "Storage append error: {}", msg),
            StorageError::StorageReadError(msg) => write!(f, "Storage read error: {}", msg),
            StorageError::StorageFull(msg) => write!(f, "Storage full: {}", msg),
            StorageError::StorageClosed(msg) => write!(f, "Storage closed: {}", msg),
            StorageError::NotFound(msg) => write!(f, "Not found: {}", msg),
            StorageError::NotAFile(msg) => write!(f, "Not a file: {}", msg),
        }
    }
}
