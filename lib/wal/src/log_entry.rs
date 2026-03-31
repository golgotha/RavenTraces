use log::{debug};
use crate::errors::WalError;
use crate::storage::{Readable, Writable};

#[repr(C, packed)]
pub struct LogEntryHeader {
    pub block_size: u32,
    pub sequence: u64,
    pub payload_size: u32,
    pub checksum: u32
}

pub struct LogEntry {
    pub header: LogEntryHeader,
    pub payload: Vec<u8>,
}

impl LogEntry {
    pub fn new(sequence: u64, payload: Vec<u8>) -> Self {
        let checksum = crc32fast::hash(&payload);
        let header_size = size_of::<LogEntryHeader>();
        let payload_size = payload.len() as u32;

        let header = LogEntryHeader {
            block_size:   (header_size + payload_size as usize) as u32,
            sequence,
            payload_size,
            checksum,
        };
        Self { header, payload }
    }
}

impl Writable for LogEntry {

    fn serialize(&self) -> Vec<u8> {
        debug!("Serializing WAL log entry");
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
        debug!("Serializing WAL log entry header");
        let mut buf = Vec::with_capacity(self.serialized_size());
        buf.extend(&self.block_size.to_le_bytes());
        buf.extend(&self.sequence.to_le_bytes());
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
            sequence: u64::from_le_bytes(buffer[4..12].try_into().expect("sequence error")),
            payload_size: u32::from_le_bytes(buffer[12..16].try_into().expect("payload_size error")),
            checksum: u32::from_le_bytes(buffer[16..20].try_into().expect("payload_size error")),
        })
    }

    fn num_bytes_to_read() -> usize {
        size_of::<LogEntryHeader>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_log_entry() {
        let payload = b"Payload".to_vec();
        let test_payload_size = payload.len() as u32;

        let log_entry = LogEntry::new(1, payload);
        let sequence = log_entry.header.sequence;
        let payload_size = log_entry.header.payload_size;

        let total_log_entry_size = log_entry.header.serialized_size() +
            payload_size as usize;

        assert_eq!(sequence, 1);
        assert_eq!(payload_size, test_payload_size);
        assert_eq!(total_log_entry_size, log_entry.header.block_size as usize);
        assert_eq!(log_entry.payload, b"Payload".to_vec());
    }

    #[test]
    fn test_log_entry_header_serialize() {
        let header = stub_header();
        let result_vector = header.serialize();

        let mut expected = Vec::new();
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
            header: header,
            payload: payload.clone(),
        };

        let result_vector = log_entry.serialize();

        let mut expected = Vec::new();
        expected.extend(&10u32.to_le_bytes());
        expected.extend(&1u64.to_le_bytes());
        expected.extend(&20u32.to_le_bytes());
        expected.extend(&100u32.to_le_bytes());
        expected.extend_from_slice(&payload);

        assert_eq!(result_vector, expected);
    }


    fn stub_header() -> LogEntryHeader {
        LogEntryHeader {
            block_size: 10,
            sequence: 1,
            payload_size: 20,
            checksum: 100,
        }
    }
}