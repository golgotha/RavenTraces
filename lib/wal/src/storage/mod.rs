pub mod storage;

use crate::errors::WalError;

pub trait Storage {

    fn write<T: Writable>(&mut self, record: &T) -> Result<usize, WalError>;

    // fn read<T: Readable>(&mut self, record: &T) -> Result<T, WalError>;
    fn read<T: Readable>(&mut self) -> Result<T, WalError>;

    fn read_at<T: Readable>(&mut self, offset: u64) -> Result<T, WalError>;

    fn read_bytes_at(&mut self, offset: u64, size: usize) -> Result<Vec<u8>, WalError>;

    fn flush(&mut self) -> Result<(), WalError>;
}

pub trait Writable {

    fn serialize(&self) -> Vec<u8>;

    fn serialized_size(&self) -> usize;
}

pub trait Readable {

    fn deserialize(buffer: &[u8]) -> Result<Self, WalError> where Self: Sized;

    fn num_bytes_to_read() -> usize;
}