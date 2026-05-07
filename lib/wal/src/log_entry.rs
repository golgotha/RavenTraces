use log::{trace};
use crate::errors::WalError;
use crate::segment::Segment;
use crate::storage::{Readable, Writable};
use crate::wal::WAL;

pub type LogEntryIteratorResult<'a> = Result<Box<dyn Iterator<Item = Result<LogEntryPointer, WalError>> + 'a>, WalError>;

#[repr(C, packed)]
pub struct LogEntryHeader {
    pub block_size: u32,
    pub payload_size: u32,
    pub checksum: u32
}

pub struct LogEntry {
    pub header: LogEntryHeader,
    pub payload: Vec<u8>,
}

pub struct LogEntryPointer {
    pub segment_id: u32,
    pub offset: u64,
    pub payload: Option<Vec<u8>>
}

impl LogEntry {
    pub fn new(payload: Vec<u8>) -> Self {
        let checksum = crc32fast::hash(&payload);
        let header_size = size_of::<LogEntryHeader>();
        let payload_size = payload.len() as u32;

        let header = LogEntryHeader {
            block_size:   (header_size + payload_size as usize) as u32,
            payload_size,
            checksum,
        };
        Self { header, payload }
    }

    pub fn header(&self) -> &LogEntryHeader {
        &self.header
    }
}

impl Writable for LogEntry {

    fn serialize(&self) -> Vec<u8> {
        trace!("Serializing WAL log entry");
        let mut bytes = self.header.serialize();
        bytes.extend_from_slice(&self.payload);
        bytes
    }

    fn serialized_size(&self) -> usize {
        self.header.block_size as usize
    }
}

impl Writable for LogEntryHeader {

    fn serialize(&self) -> Vec<u8> {
        trace!("Serializing WAL log entry header");
        let mut buf = Vec::with_capacity(self.serialized_size());
        buf.extend(&self.block_size.to_le_bytes());
        buf.extend(&self.payload_size.to_le_bytes());
        buf.extend(&self.checksum.to_le_bytes());
        buf
    }

    fn serialized_size(&self) -> usize {
        size_of::<LogEntryHeader>()
    }
}

impl Readable for LogEntryHeader {

    fn deserialize(buffer: &[u8]) -> Result<Self, WalError>
    where
        Self: Sized
    {
        if buffer.len() < LogEntryHeader::num_bytes_to_read() {
            return Err(WalError::CorruptedEntry("Corrupted log entry header. Buffer too small".into()));
        }

        Ok(LogEntryHeader {
            block_size: u32::from_le_bytes(buffer[0..4].try_into().expect("block_size error")),
            payload_size: u32::from_le_bytes(buffer[4..8].try_into().expect("payload_size error")),
            checksum: u32::from_le_bytes(buffer[8..12].try_into().expect("payload_size error")),
        })
    }

    fn num_bytes_to_read() -> usize {
        size_of::<LogEntryHeader>()
    }
}

pub struct LogEntryIterator<'a> {
    wal: &'a WAL,
    current_segment_id: u32,
    current_segment: Option<Segment>,
    last_segment_id: u32,
    offset: u64,
    current_segment_size: u64,
}

impl<'a> LogEntryIterator<'a> {
    pub fn new(wal: &'a WAL,
               first_segment_id: u32,
               last_segment_id: u32) -> Self {
        Self {
            wal,
            current_segment_id: first_segment_id,
            current_segment: None,
            last_segment_id,
            offset: Segment::header_size() as u64,
            current_segment_size: 0,
        }
    }
}

impl<'a> Iterator for LogEntryIterator<'a> {
    type Item = Result<LogEntryPointer, WalError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_segment_id > self.last_segment_id {
                return None;
            }

            if self.current_segment.is_none() {
                // open segment
                let segment = match self.wal.open_segment_by_id(self.current_segment_id) {
                    Ok(segment) => segment,
                    Err(err) => {
                        // Skip segment and move to the next one
                        self.current_segment_id += 1;
                        return Some(Err(err))
                    },
                };

                let segment_size = match segment.segment_size() {
                    Ok(size) => size,
                    Err(err) => return Some(Err(WalError::IoError(err))),
                };

                self.offset = Segment::header_size() as u64;
                self.current_segment_size = segment_size;
                self.current_segment = Some(segment);
            }

            if self.offset >= self.current_segment_size {
                self.current_segment = None;
                self.current_segment_id += 1;
                self.offset = Segment::header_size() as u64;
                self.current_segment_size = 0;
                continue;
            }

            let segment = self.current_segment.as_mut().unwrap();
            let offset = self.offset;
            let log_entry = match segment.read_log_entry(offset) {
                Ok(entry) => entry,
                Err(err) => return Some(Err(err)),
            };

            self.offset += log_entry.header.block_size as u64;

            let segment_id = segment.header().segment_id();

            return Some(Ok(LogEntryPointer {
                segment_id,
                offset,
                payload: Some(log_entry.payload),
            }));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_log_entry() {
        let payload = b"Payload".to_vec();
        let test_payload_size = payload.len() as u32;

        let log_entry = LogEntry::new(payload);
        let payload_size = log_entry.header.payload_size;

        let total_log_entry_size = log_entry.header.serialized_size() +
            payload_size as usize;

        assert_eq!(payload_size, test_payload_size);
        assert_eq!(total_log_entry_size, log_entry.header.block_size as usize);
        assert_eq!(log_entry.payload, b"Payload".to_vec());
    }

    #[test]
    fn test_log_entry_header_serialize() {
        let header = stub_header();
        let result_vector = header.serialize();

        let mut expected: Vec<u8> = Vec::new();
        expected.extend(&10u32.to_le_bytes());   // block_size
        expected.extend(&1u64.to_le_bytes());    // sequence
        expected.extend(&20u32.to_le_bytes());   // payload_size
        expected.extend(&100u32.to_le_bytes());  // checksum

        assert_eq!(result_vector, expected);
        assert_eq!(result_vector.len(), 4 + 8 + 4 + 4);
    }

    #[test]
    fn test_log_entry_serialized_size() {
        let header = stub_header();
        let header_size = header.serialized_size();
        assert_eq!(header_size, 4 + 8 + 4 + 4, "expected a log entry header 20 bytes size");
    }

    #[test]
    fn test_log_entry_serialization() {
        let header = stub_header();
        let payload = b"Payload".to_vec();

        let log_entry = LogEntry {
            header,
            payload,
        };

        let result_vector = log_entry.serialize();

        let mut expected = Vec::new();
        expected.extend(&10u32.to_le_bytes());
        expected.extend(&1u64.to_le_bytes());
        expected.extend(&20u32.to_le_bytes());
        expected.extend(&100u32.to_le_bytes());
        expected.extend_from_slice(&log_entry.payload);

        assert_eq!(result_vector, expected);
    }


    fn stub_header() -> LogEntryHeader {
        LogEntryHeader {
            block_size: 10,
            payload_size: 20,
            checksum: 100,
        }
    }
}