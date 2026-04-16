
#[derive(Debug)]
pub enum StorageError {
    IoError(std::io::Error),
    CorruptedEntry(String),
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
