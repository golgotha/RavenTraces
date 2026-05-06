use std::fmt::{Display, Formatter};

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

impl Display for WalError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::IoError(err) => write!(f, "I/O error: {}", err),
            WalError::CorruptedEntry(msg) => write!(f, "Corrupted entry: {}", msg),
            WalError::StorageFull(msg) => write!(f, "Storage full: {}", msg),
            WalError::StorageClosed(msg) => write!(f, "Storage closed: {}", msg),
            WalError::NoCheckpoint(msg) => write!(f, "BNo checkpoint error: {}", msg),
            WalError::NotAFile(msg) => write!(f, "Not a file: {}", msg),
        }
    }
}