use std::io::Read;
use crate::errors::StorageError;

pub fn try_read_u32<R: Read>(reader: &mut R) -> Result<Option<u32>, StorageError> {
    let mut buf = [0u8; 4];

    match reader.read_exact(&mut buf) {
        Ok(_) => Ok(Some(u32::from_le_bytes(buf))),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(StorageError::StorageReadError(e.to_string())),
    }
}

pub fn read_u32<R: Read>(reader: &mut R) -> Result<u32, StorageError> {
    let mut buf = [0u8; 4];

    match reader.read_exact(&mut buf) {
        Ok(_) => Ok(u32::from_le_bytes(buf)),
        Err(e) => Err(StorageError::StorageReadError(e.to_string())),
    }
}

pub fn read_exact_bytes<R: Read>(reader: &mut R, len: usize) -> Result<Vec<u8>, StorageError> {
    let mut buf = vec![0u8; len];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::StorageReadError(e.to_string()))?;
    Ok(buf)
}
