
#[derive(Debug)]
pub enum WalError {
    IoError(std::io::Error),
    CorruptedEntry(String),
    StorageFull(String),
    StorageClosed(String),
    NoCheckpoint(String),
    NotAFile(String)
}

impl From<std::io::Error> for WalError {
    fn from(err: std::io::Error) -> Self {
        WalError::IoError(err)
    }
}