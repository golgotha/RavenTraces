use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use log::{info, warn, error};
use crate::errors::WalError;
use crate::storage::storage::FileStorage;
use crate::storage::{Readable, Storage, Writable};
use crate::log_entry::LogEntryHeader;
use crate::sequence::Sequence;

const SEGMENT_MAGIC: &[u8; 4] = b"RWAL";
const SEGMENT_PREFIX: &str = "segment_";

#[repr(C, packed)]
pub struct SegmentHeader {
    magic: [u8; 4],
    version: u8,
    segment_id: u32,
    created_at: u128,
    reserved: [u8; 39],
}

impl SegmentHeader {

    pub fn magic(&self) -> [u8; 4] {
        self.magic
    }

    pub fn version(&self) -> u8 {
        self.version
    }

    pub fn segment_id(&self) -> u32 {
        self.segment_id
    }

    pub fn created_at(&self) -> u128 {
        self.created_at
    }

    pub fn is_valid_magic(&self) -> bool {
        self.magic == *SEGMENT_MAGIC
    }
}

pub struct Segment {
    storage: FileStorage,
    sequence: Sequence,
    header: SegmentHeader,
    /// Index of entry offset and lengths.
    index: Vec<(usize, usize)>,
    /// The segment size. The value increases with each append operation.
    size: u64
}

impl Segment {

    /// Opens the segment file at the specified path.
    /// An individual file must only be opened by one segment at a time.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Segment, WalError> {
        let mut storage = FileStorage::open(path)?;
        let segment_header: SegmentHeader = storage.read()?;

        if segment_header.magic != *SEGMENT_MAGIC {
            return Err(WalError::CorruptedEntry("Wrong segment magic number".into()))
        }

        if segment_header.version() != 1 {
            return Err(WalError::CorruptedEntry("Version is not supported".into()))
        }

        let header_size = Segment::header_size();

        let file_size = storage.size();
        if file_size < header_size as u64 {
            return Err(WalError::CorruptedEntry("File is too small".into()));
        }

        let mut index = Vec::new();
        let mut offset = header_size;
        while offset < file_size as usize {
            let log_entry_header: LogEntryHeader = storage.read_at(offset as u64)?;
            index.push((offset, log_entry_header.block_size as usize));
            offset += log_entry_header.block_size as usize;
        }

        let sequence = Sequence::new(segment_header.segment_id);
        Ok(Segment {
            storage,
            sequence,
            header: segment_header,
            index,
            size: offset as u64,
        })
    }

    pub fn create<P: AsRef<Path>>(path: P, capacity: usize) -> Result<Self, WalError> {
        let mut  sequence = Sequence::new(0);
        create_segment_storage(path, sequence.next())
    }

    pub fn create_next_segment<P: AsRef<Path>>(&mut self, path: P) -> Result<Segment, WalError> {
        let sequence = self.sequence.next();
        create_segment_storage(path, sequence)
    }

    pub fn append<T: Writable>(&mut self, record: &T) -> Result<(), WalError> {
        let n = self.storage.write(record)?;
        self.size += n as u64;
        Ok(())
    }

    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    pub fn segment_size(&self) -> std::io::Result<u64> {
        Ok(self.size)
    }

    fn header_size() -> usize {
        size_of::<SegmentHeader>()
    }
}

impl Writable for SegmentHeader {
    fn serialize(&self) -> Vec<u8> {
        info!("Serializing WAL header entry");
        let mut buffer = Vec::with_capacity(self.serialized_size());
        buffer.extend(&self.magic);
        buffer.push(self.version);
        buffer.extend(&self.segment_id.to_le_bytes());
        buffer.extend(&self.created_at.to_le_bytes());
        buffer.extend(&self.reserved);
        buffer
    }

    fn serialized_size(&self) -> usize {
        size_of::<SegmentHeader>()
    }
}

impl Readable for SegmentHeader {
    fn deserialize(buffer: &[u8]) -> Result<Self, WalError>
    where
        Self: Sized
    {
        if buffer.len() < SegmentHeader::num_bytes_to_read() {
            return Err(WalError::CorruptedEntry("Buffer too small".into()));
        }

        Ok(SegmentHeader {
            magic: [buffer[0], buffer[1], buffer[2], buffer[3]],
            version: buffer[4],
            segment_id: u32::from_le_bytes(buffer[5..9].try_into().expect("segment id overflow")),
            created_at: u128::from_le_bytes(buffer[9..25].try_into().unwrap()),
            reserved: buffer[25..64].try_into().unwrap(),
        })
    }

    fn num_bytes_to_read() -> usize {
        size_of::<SegmentHeader>()
    }
}

fn create_segment_storage<P: AsRef<Path>>(path: P, sequence: Sequence) -> Result<Segment, WalError> {
    let current_segment_id = sequence.current();
    let segment_name = format!("{}{:06}.wal",  SEGMENT_PREFIX, current_segment_id);
    let segment_path = PathBuf::from(path.as_ref()).join(segment_name);

    let mut storage = FileStorage::open(segment_path)?;
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis();

    let header = SegmentHeader {
        magic: *SEGMENT_MAGIC,
        version: 1,
        segment_id: current_segment_id,
        created_at: current_time,
        reserved: [0; 39],
    };

    storage
        .write(&header)
        .expect("Error occurred when writing to WAL");

    Ok(Segment {
        storage,
        sequence,
        header,
        index: Vec::new(),
        size: SegmentHeader::num_bytes_to_read() as u64,
    })

}