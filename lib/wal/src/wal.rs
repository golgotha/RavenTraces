use std::fs;
use std::fs::{DirEntry};
use std::path::{Path, PathBuf};
use log::{info};
use crate::errors::WalError;
use crate::log_entry::LogEntry;
use crate::segment::Segment;

#[derive(Debug)]
pub struct WalOptions {
    /// The segment capacity. Defaults to 128MiB.
    pub segment_capacity: usize,

    /// The number of segments to create ahead of time, so that appends never
    /// need to wait on creating a new segment.
    pub segment_queue_len: usize,
}

impl Default for WalOptions {
    fn default() -> WalOptions {
        WalOptions {
            segment_capacity: 64 * 1024 * 1024,
            segment_queue_len: 0,
        }
    }
}

pub struct WAL {
    active_segment: Segment,
    options: WalOptions,
    dir: PathBuf
}

impl WAL {

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WalError> {
        WAL::with_options(path, WalOptions::default())
    }

    pub fn with_options<P: AsRef<Path>>(path: P, options: WalOptions) -> Result<Self, WalError> {
        let path = path.as_ref();
        info!("Opening WAL directory {:?}", &path);

        if !path.exists() {
            info!("Create {:?} directory. ", &path);
            fs::create_dir_all(path)?;
        }

        let active_segment: Segment;
        let last_segment = get_last_segment(path);
        if last_segment.is_none() {
            info!("No segments found in {:?}", &path);
            let segment  = Segment::create(path, options.segment_capacity)?;
            active_segment = segment;
        } else {
            let segment = open_dir_entry(last_segment.unwrap())?;
            info!("Found segment found with segment index {:?}", segment.header().segment_id());
            active_segment = segment;
        }

        info!("Active segment size: {}Kb", active_segment.segment_size()? / 1024);

        let wal = WAL { active_segment, options, dir: path.to_path_buf() };
        Ok(wal)
    }

    pub fn append(&mut self, entry: &LogEntry) -> Result<(), WalError> {
        if self.should_rotate(entry) {
            info!("Rotate segment");
            self.rotate_segment()?;
        }
        self.active_segment.append(entry)
    }

    pub fn rotate_segment(&mut self) -> Result<(), WalError> {
        let segment  = self.active_segment.create_next_segment(self.dir.to_path_buf())?;
        self.active_segment = segment;
        Ok(())
    }

    pub fn replay(&mut self) -> Result<Vec<LogEntry>, WalError> {
        Ok(Vec::new())
    }

    pub fn flush(&mut self) -> Result<(), WalError> {
        todo!("Require implementation")
    }

    fn should_rotate(&self, entry: &LogEntry) -> bool {
        let payload_size = entry.payload.len();
        let segment_size = self.active_segment.segment_size()
            .expect("WAL entry must have segment size");
        segment_size + payload_size as u64 > self.options.segment_capacity as u64
    }
}

fn open_dir_entry(entry: DirEntry) -> Result<Segment, WalError> {
    info!("Opening WAL directory entry");
    let metadata = entry.metadata()?;
    if !metadata.is_file() {
        return Err(WalError::NotAFile(format!(
            "Expected a file but found: {:?}",
            entry.path()
        )));
    }

    let segment = Segment::open(entry.path())?;
    Ok(segment)
}

fn get_last_segment(path: &Path) -> Option<DirEntry> {
    let mut segments: Vec<DirEntry> = fs::read_dir(path)
        .ok()?                       // handle read_dir error
        .filter_map(|e| e.ok())      // ignore individual entry errors
        .filter(|entry| {
            entry
                .path()
                .extension()
                .map(|ext| ext == "wal")
                .unwrap_or(false)
        })
        .collect();

    // Sort by filename (lexicographically)
    segments.sort_by_key(|entry| entry.path());

    // Return the last DirEntry
    segments.pop()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::log_entry::LogEntryHeader;

    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("failed to create temp dir")
    }

    fn make_header() -> LogEntryHeader {
        LogEntryHeader {
            block_size: 10,
            sequence: 1,
            payload_size: 1024,
            checksum: 1234,
        }
    }


    fn make_entry(payload: &[u8]) -> LogEntry {
        LogEntry {
            header: make_header(),
            payload: payload.to_vec()
        }
    }

    #[test]
    fn open_creates_directory_if_missing() {
        let dir = temp_dir();
        let wal_path = dir.path().join("new_wal_dir");
        assert!(!wal_path.exists());

        WAL::open(&wal_path).expect("open failed");

        assert!(wal_path.exists());
    }

    #[test]
    fn open_creates_initial_segment_when_directory_is_empty() {
        let dir = temp_dir();
        WAL::open(dir.path()).expect("open failed");

        let wal_files: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |x| x == "wal"))
            .collect();

        assert_eq!(wal_files.len(), 1, "expected one segment file on first open");
    }

    #[test]
    fn open_reopens_existing_segment_without_creating_new_one() {
        let dir = temp_dir();
        WAL::open(dir.path()).expect("first open failed");
        WAL::open(dir.path()).expect("second open failed");

        let wal_files: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |x| x == "wal"))
            .collect();

        assert_eq!(wal_files.len(), 1, "second open should reuse existing segment");
    }

    #[test]
    fn append_single_entry_succeeds() {
        let dir = temp_dir();
        let mut wal = WAL::open(dir.path()).expect("open failed");
        wal.append(&make_entry(b"hello")).expect("append failed");
    }

    #[test]
    fn append_multiple_entries_succeeds() {
        let dir = temp_dir();
        let mut wal = WAL::open(dir.path()).expect("open failed");
        for i in 0u8..10 {
            wal.append(&make_entry(&[i; 32])).expect("append failed");
        }
    }

    #[test]
    fn rotation_creates_new_segment_file() {
        let dir = temp_dir();
        let options = WalOptions {
            segment_capacity: 64, // tiny — forces rotation quickly
            segment_queue_len: 0,
        };
        let mut wal = WAL::with_options(dir.path(), options).expect("open failed");

        // Fill past the capacity threshold to trigger rotation.
        for _ in 0..4 {
            wal.append(&make_entry(&[0u8; 32])).expect("append failed");
        }

        let wal_files: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |x| x == "wal"))
            .collect();

        assert!(wal_files.len() > 1, "rotation should have produced a second segment");
    }

    #[test]
    fn rotate_segment_explicitly_produces_new_file() {
        let dir = temp_dir();
        let mut wal = WAL::open(dir.path()).expect("open failed");
        wal.rotate_segment().expect("rotate failed");

        let wal_files: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |x| x == "wal"))
            .collect();

        assert_eq!(wal_files.len(), 2);
    }

    #[test]
    fn after_rotation_new_segment_id_is_greater() {
        let dir = temp_dir();
        let mut wal = WAL::open(dir.path()).expect("open failed");
        let id_before = wal.active_segment.header().segment_id();

        wal.rotate_segment().expect("rotate failed");

        let id_after = wal.active_segment.header().segment_id();
        assert!(id_after > id_before, "segment id must increase after rotation");
    }

    #[test]
    fn default_options_have_expected_capacity() {
        let opts = WalOptions::default();
        assert_eq!(opts.segment_capacity, 64 * 1024 * 1024);
        assert_eq!(opts.segment_queue_len, 0);
    }

    #[test]
    fn get_last_segment_returns_none_for_empty_directory() {
        let dir = temp_dir();
        assert!(get_last_segment(dir.path()).is_none());
    }

    #[test]
    fn get_last_segment_returns_lexicographically_last_wal_file() {
        let dir = temp_dir();
        // Create two segments so there are two .wal files.
        let mut wal = WAL::open(dir.path()).expect("open failed");
        wal.rotate_segment().expect("rotate failed");

        let last = get_last_segment(dir.path()).expect("expected a segment");
        let name = last.file_name();
        let name = name.to_string_lossy();

        // The last file should sort highest — segment_000001.wal > segment_000000.wal
        assert!(name.contains("000002"), "got: {}", name);
    }

    #[test]
    fn get_last_segment_ignores_non_wal_files() {
        let dir = temp_dir();
        fs::write(dir.path().join("notes.txt"), b"ignore me").unwrap();
        fs::write(dir.path().join("data.log"), b"ignore me too").unwrap();

        assert!(get_last_segment(dir.path()).is_none());
    }
}