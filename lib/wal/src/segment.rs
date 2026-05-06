use std::path::{Path, PathBuf};
use log::{info};
use common::clock;
use crate::errors::WalError;
use crate::storage::storage::FileStorage;
use crate::storage::{Readable, Storage, Writable};
use crate::log_entry::{LogEntry, LogEntryHeader};
use crate::sequence::Sequence;

const SEGMENT_MAGIC: &[u8; 4] = b"RWAL";
pub const SEGMENT_PREFIX: &str = "segment_";
pub const SEGMENT_EXTENSION: &str = ".wal";

#[repr(C, packed)]
#[derive(Debug)]
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
    pub(crate) fn segment_id(&self) -> u32 {
        self.header.segment_id
    }
}

impl Segment {

    /// Opens the segment file at the specified path.
    /// An individual file must only be opened by one segment at a time.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Segment, WalError> {
        let mut storage = FileStorage::open(path, true)?;
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

    pub fn create<P: AsRef<Path>>(path: P, _capacity: usize) -> Result<Self, WalError> {
        let mut  sequence = Sequence::new(0);
        create_segment_storage(path, sequence.next())
    }

    pub fn create_next_segment<P: AsRef<Path>>(&mut self, path: P) -> Result<Segment, WalError> {
        let sequence = self.sequence.next();
        create_segment_storage(path, sequence)
    }

    pub fn append<T: Writable>(&mut self, record: T) -> Result<(), WalError> {
        let n = self.storage.write(&record)?;
        let offset = self.size as usize;
        self.index.push((offset, n));
        self.size += n as u64;
        Ok(())
    }

    pub fn read_log_entry(&mut self, offset: u64) -> Result<LogEntry, WalError> {
        let header: LogEntryHeader = self.storage.read_at(offset)?;
        let header_size = LogEntryHeader::num_bytes_to_read();
        let payload_size = header.payload_size;
        let payload = self.storage.read_bytes_at(offset + header_size as u64, payload_size as usize)?;
        let checksum = crc32fast::hash(&payload);
        if header.checksum != checksum {
            return Err(WalError::CorruptedEntry("Log entry checksum mismatch".into()));
        }

        let result = LogEntry {
            header,
            payload,
        };
        Ok(result)
    }

    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    pub fn segment_size(&self) -> std::io::Result<u64> {
        Ok(self.size)
    }

    pub fn index(&self) -> Vec<(usize, usize)> {
        self.index.clone()
    }

    pub fn header_size() -> usize {
        size_of::<SegmentHeader>()
    }

    pub fn get_segment_name(segment_id: u32) -> String {
        format!("{}{:010}{}",  SEGMENT_PREFIX, segment_id, SEGMENT_EXTENSION)
    }

    pub fn remove<P: AsRef<Path>>(path: P) -> Result<(), WalError> {
        FileStorage::delete(path)
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
    let segment_name = Segment::get_segment_name(current_segment_id);
    let segment_path = PathBuf::from(path.as_ref()).join(segment_name);

    let mut storage = FileStorage::open(segment_path, true)?;
    let current_time = clock::now_millis();

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

    storage.flush()
    .expect("Error occurred when flushing WAL segment header");

    Ok(Segment {
        storage,
        sequence,
        header,
        index: Vec::new(),
        size: SegmentHeader::num_bytes_to_read() as u64,
    })

}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    const DEFAULT_CAPACITY: usize = 512;

    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("failed to create temp dir")
    }

    /// A minimal `Writable` test record.
    struct TestRecord {
        data: Vec<u8>,
    }

    impl TestRecord {
        fn new(data: &[u8]) -> Self {
            Self { data: data.to_vec() }
        }
    }

    impl Writable for TestRecord {
        fn serialize(&self) -> Vec<u8> {
            self.data.clone()
        }

        fn serialized_size(&self) -> usize {
            self.data.len()
        }
    }

    mod segment_header {
        use super::*;

        fn make_header(segment_id: u32) -> SegmentHeader {
            SegmentHeader {
                magic: *b"RWAL",
                version: 1,
                segment_id,
                created_at: 1_700_000_000_000,
                reserved: [0u8; 39],
            }
        }

        #[test]
        fn is_valid_magic_returns_true_for_correct_magic() {
            let h = make_header(1);
            assert!(h.is_valid_magic());
        }

        #[test]
        fn is_valid_magic_returns_false_for_wrong_magic() {
            let h = SegmentHeader {
                magic: *b"XWAL",
                version: 1,
                segment_id: 0,
                created_at: 0,
                reserved: [0; 39],
            };
            assert!(!h.is_valid_magic());
        }

        #[test]
        fn accessors_return_correct_values() {
            let header = make_header(42);
            assert_eq!(header.magic(), *b"RWAL");
            assert_eq!(header.version(), 1);
            assert_eq!(header.segment_id(), 42);
            assert_eq!(header.created_at(), 1_700_000_000_000);
        }

        // Serialise → deserialise round-trip.
        #[test]
        fn roundtrip_serialization() {
            let header = make_header(7);
            let bytes = header.serialize();
            assert_eq!(bytes.len(), SegmentHeader::num_bytes_to_read());

            let decoded = SegmentHeader::deserialize(&bytes).expect("deserialize failed");
            assert_eq!(decoded.magic(), header.magic());
            assert_eq!(decoded.version(), header.version());
            assert_eq!(decoded.segment_id(), header.segment_id());
            assert_eq!(decoded.created_at(), header.created_at());
        }

        #[test]
        fn deserialize_rejects_undersized_buffer() {
            let short = vec![0u8; 4]; // much smaller than size_of::<SegmentHeader>()
            let result = SegmentHeader::deserialize(&short);
            assert!(
                matches!(result, Err(WalError::CorruptedEntry(_))),
                "expected CorruptedEntry, got {:?}",
                result
            );
        }

        #[test]
        fn serialized_size_matches_struct_size() {
            let h = make_header(0);
            assert_eq!(h.serialized_size(), size_of::<SegmentHeader>());
        }
    }

    mod segment_create {
        use super::*;

        #[test]
        fn creates_wal_segment_file_in_given_directory() {
            let dir = temp_dir();
            Segment::create(dir.path(), DEFAULT_CAPACITY).expect("create failed");

            let wal_files: Vec<_> = fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map_or(false, |x| x == "wal"))
                .collect();

            assert_eq!(wal_files.len(), 1, "expected exactly one .wal file");
        }

        #[test]
        fn new_segment_has_nonzero_size() {
            let dir = temp_dir();
            let seg = Segment::create(dir.path(), DEFAULT_CAPACITY).expect("create failed");
            let size = seg.segment_size().expect("size error");
            assert!(size > 0, "segment size should include the header");
        }

        #[test]
        fn new_segment_size_equals_header_size() {
            let dir = temp_dir();
            let seg = Segment::create(dir.path(), DEFAULT_CAPACITY).expect("create failed");
            let expected = size_of::<SegmentHeader>() as u64;
            assert_eq!(seg.segment_size().unwrap(), expected);
        }

        #[test]
        fn header_has_valid_magic_after_create() {
            let dir = temp_dir();
            let seg = Segment::create(dir.path(), DEFAULT_CAPACITY).expect("create failed");
            assert!(seg.header().is_valid_magic());
        }

        #[test]
        fn header_version_is_one() {
            let dir = temp_dir();
            let seg = Segment::create(dir.path(), DEFAULT_CAPACITY)
                .expect("Segment creation failed");
            assert_eq!(seg.header().version(), 1);
        }
    }

    mod segment_open {
        use super::*;

        #[test]
        fn open_after_create() {
            let dir = temp_dir();
            let created = Segment::create(dir.path(), DEFAULT_CAPACITY)
                .expect("Segment creation failed");
            let segment_id = created.header().segment_id();

            // Find the written file and re-open it.
            let wal_path = fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .find(|e| e.path().extension().map_or(false, |x| x == "wal"))
                .expect("no .wal file found")
                .path();

            let opened = Segment::open(&wal_path).expect("open failed");
            assert_eq!(opened.header().segment_id(), segment_id);
            assert!(opened.header().is_valid_magic());
            assert_eq!(opened.header().version(), 1);
        }

        #[test]
        fn open_fails_on_wrong_magic() {
            let dir = temp_dir();
            let path = dir.path().join("bad.wal");

            // Write a file whose first 4 bytes are not "RWAL".
            let mut data = vec![0u8; size_of::<SegmentHeader>()];
            data[0..4].copy_from_slice(b"XXXX");
            fs::write(&path, &data).unwrap();

            let result = Segment::open(&path);
            assert!(
                matches!(result, Err(WalError::CorruptedEntry(_))),
                "expected CorruptedEntry for wrong magic"
            );
        }

        #[test]
        fn open_fails_on_unsupported_version() {
            let dir = temp_dir();
            let path = dir.path().join("badver.wal");

            let h = SegmentHeader {
                magic: *b"RWAL",
                version: 99, // unsupported
                segment_id: 0,
                created_at: 0,
                reserved: [0; 39],
            };
            fs::write(&path, h.serialize()).unwrap();

            let result = Segment::open(&path);
            assert!(
                matches!(result, Err(WalError::CorruptedEntry(_))),
                "expected CorruptedEntry for unsupported version"
            );
        }

        #[test]
        fn open_fails_when_file_too_small() {
            let dir = temp_dir();
            let path = dir.path().join("tiny.wal");
            fs::write(&path, b"RWAL").unwrap(); // only 4 bytes

            let result = Segment::open(&path);
            assert!(
                matches!(result, Err(WalError::CorruptedEntry(_))),
                "expected CorruptedEntry for undersized file"
            );
        }
    }

    mod segment_append {
        use super::*;

        #[test]
        fn append_increases_segment_size() {
            let dir = temp_dir();
            let mut seg = Segment::create(dir.path(), 1024).expect("create failed");
            let size_before = seg.segment_size().unwrap();

            let record = TestRecord::new(b"hello world");
            seg.append(record).expect("append failed");

            let size_after = seg.segment_size().unwrap();
            assert!(size_after > size_before, "size should grow after append");
        }

        #[test]
        fn append_multiple_records_grows_size_monotonically() {
            let dir = temp_dir();
            let mut seg = Segment::create(dir.path(), 4096).expect("create failed");

            let mut prev = seg.segment_size().unwrap();
            for i in 0u8..5 {
                seg.append(TestRecord::new(&[i; 16])).expect("append failed");
                let curr = seg.segment_size().unwrap();
                assert!(curr > prev, "size must strictly increase");
                prev = curr;
            }
        }

        #[test]
        fn append_empty_record_does_not_error() {
            let dir = temp_dir();
            let mut seg = Segment::create(dir.path(), 512).expect("create failed");
            // An empty payload is unusual but should not panic.
            seg.append(TestRecord::new(b"")).expect("append of empty record failed");
        }
    }

    mod segment_rotation {
        use super::*;

        #[test]
        fn next_segment_creates_new_wal_file() {
            let dir = temp_dir();
            let mut seg = Segment::create(dir.path(), 512).expect("create failed");
            let _next = seg
                .create_next_segment(dir.path())
                .expect("create_next_segment failed");

            let wal_files: Vec<_> = fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map_or(false, |x| x == "wal"))
                .collect();

            assert_eq!(wal_files.len(), 2, "there should be two .wal files after rotation");
        }

        #[test]
        fn next_segment_has_incremented_id() {
            let dir = temp_dir();
            let mut seg = Segment::create(dir.path(), 512).expect("create failed");
            let first_id = seg.header().segment_id();

            let next = seg
                .create_next_segment(dir.path())
                .expect("create_next_segment failed");

            assert!(
                next.header().segment_id() > first_id,
                "next segment id ({}) should be greater than first ({})",
                next.header().segment_id(),
                first_id
            );
        }

        #[test]
        fn next_segment_is_empty_initially() {
            let dir = temp_dir();
            let mut seg = Segment::create(dir.path(), 512).unwrap();
            seg.append(TestRecord::new(b"some data")).unwrap();

            let next = seg.create_next_segment(dir.path()).unwrap();
            let expected = size_of::<SegmentHeader>() as u64;
            assert_eq!(
                next.segment_size().unwrap(),
                expected,
                "fresh segment should only contain the header"
            );
        }
    }
}