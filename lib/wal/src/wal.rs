use std::fs;
use std::fs::{DirEntry, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use log::{debug, info, Level};
use serde::{Deserialize, Serialize};
use serde_json;
use common::clock::{now_millis};
use crate::errors::WalError;
use crate::log_entry::{LogEntry, LogEntryPointer};
use crate::segment::{Segment};
use crate::storage::{Readable, Storage, Writable};
use crate::storage::storage::FileStorage;

const CHECKPOINT_FILE_NAME: &str = "checkpoint.json";
const FILE_EXTENSION: &str = ".wal";

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

#[derive(Serialize, Deserialize, Debug)]
pub struct Checkpoint {
    safe_segment: u32,
}

pub struct WAL {
    active_segment: Segment,
    options: WalOptions,
    dir: PathBuf,
    last_segment_id: u32,
    last_checkpoint: Option<Checkpoint>,

}

pub struct AppendResult {
    segment_id: u32,
    offset: u64,
    length: u32,
    last_update: u128,
}

impl WAL {

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WalError> {
        WAL::with_options(path, WalOptions::default())
    }

    pub fn with_options<P: AsRef<Path>>(path: P, options: WalOptions) -> Result<Self, WalError> {
        let wal_path = PathBuf::from(path.as_ref())
            .join("wal");
        info!("Opening WAL directory {:?}", &wal_path );

        if !wal_path.exists() {
            info!("Create {:?} directory. ", &wal_path );
            fs::create_dir_all(&wal_path)?;
        }

        let mut last_segment_id = 0;
        let active_segment: Segment;
        let last_segment = get_last_segment(&wal_path);
        if last_segment.is_none() {
            info!("No segments found in {:?}", &wal_path);
            let segment  = Segment::create(&wal_path , options.segment_capacity)?;
            active_segment = segment;
        } else {
            let segment = open_segment(last_segment.unwrap())?;
            last_segment_id = segment.header().segment_id();
            info!("Found segment found with segment index {:?}", segment.header().segment_id());
            active_segment = segment;
        }

        info!("Active segment size: {}Kb", active_segment.segment_size()? / 1024);

        let wal = WAL {
            active_segment,
            options,
            dir: wal_path,
            last_segment_id,
            last_checkpoint: None,
        };
        Ok(wal)
    }

    pub fn append(&mut self, entry: LogEntry) -> Result<AppendResult, WalError> {
        if self.should_rotate(&entry) {
            info!("Rotate segment");
            self.rotate_segment()?;
        }
        let block_size = entry.header().block_size;
        self.active_segment.append(entry)?;
        Ok(AppendResult {
            segment_id: self.active_segment.header().segment_id(),
            offset: self.active_segment.segment_size()?,
            length: block_size,
            last_update: now_millis(),
        })
    }

    pub fn rotate_segment(&mut self) -> Result<Checkpoint, WalError> {
        let safe_segment = self.active_segment.segment_id();
        let segment  = self.active_segment.create_next_segment(self.dir.to_path_buf())?;
        debug!("Rotate segment with safe segment. The segment checkpoint: {}, new segment id: {}", safe_segment, segment.segment_id());
        self.active_segment = segment;

        let checkpoint = Checkpoint { safe_segment };
        self.last_checkpoint = Some(checkpoint);
        Ok(Checkpoint { safe_segment })
    }

    pub fn replay(&mut self) -> Result<Vec<LogEntryPointer>, WalError> {
        info!("Replaying WAL entries");

        let mut offset = Segment::header_size() as u64;
        let checkpoint = self.read_checkpoint();
        let safe_segment = checkpoint.map(|c| c.safe_segment)
            .unwrap_or(0);

        info!("Last safe segment #{}", safe_segment);

        let mut entries = Vec::new();
        let mut current_segment_id = safe_segment;
        while current_segment_id < self.last_segment_id {
            current_segment_id += 1;
            debug!("Opening segment={}", current_segment_id);
            let segment = self.open_segment_by_id(current_segment_id)?;
            let segment_size = segment.segment_size()?;

            while offset < segment_size {
                let log_entry : LogEntry = self.active_segment.read_log_entry(offset)?;
                let segment_id = self.active_segment.header().segment_id();
                entries.push(LogEntryPointer {
                    segment_id,
                    offset,
                    payload: Some(log_entry.payload),
                });
                offset += log_entry.header.block_size as u64;
            }
        }

        info!("Reading {} WAL entries completed", entries.len());
        Ok(entries)
    }

    pub fn commit_checkpoint(&mut self) -> Result<(), WalError> {
        let checkpoint_file_path = self.dir.to_path_buf().join(CHECKPOINT_FILE_NAME);
        let mut storage = FileStorage::open(checkpoint_file_path, false)?;
        if let Some(checkpoint) = &self.last_checkpoint {
            debug!("Committing WAL checkpoint: {}", checkpoint.safe_segment);
            storage.write(checkpoint)?;
        }
        Ok(())
    }

    pub fn cleanup(&mut self) -> Result<(), WalError> {
        if let Some(checkpoint) = &self.last_checkpoint {
            info!("Cleaning WAL segments");
            let safe_segment = checkpoint.safe_segment;
            let all_segments = list_segments(&self.dir)?;

            let segments_to_remove: Vec<DirEntry> = all_segments
                .into_iter()
                .filter_map(|entry| {
                    let file_name = entry.file_name();
                    let file_name = file_name.to_str()?;

                    let segment_id = extract_segment_id(file_name)?;
                    if segment_id <= safe_segment {
                        Some(entry)
                    } else {
                        None
                    }
                })
                .collect();

            info!("Found {} segments to remove", segments_to_remove.len());

            segments_to_remove.into_iter().for_each(|dir_entry: DirEntry| {
                let segment_path = dir_entry.path();
                if log::log_enabled!(Level::Debug) {
                    debug!("Remove Segment: {:?} ", segment_path.display());
                }
                Segment::remove(segment_path)
                    .expect("Unable to remove segment file");
            });
        }

        Ok(())
    }

    fn should_rotate(&self, entry: &LogEntry) -> bool {
        let payload_size = entry.payload.len();
        let segment_size = self.active_segment.segment_size()
            .expect("WAL entry must have segment size");
        segment_size + payload_size as u64 > self.options.segment_capacity as u64
    }

    fn read_checkpoint(&mut self) -> Result<Checkpoint, WalError> {
        let checkpoint_file_path = self.dir.to_path_buf().join(CHECKPOINT_FILE_NAME);
        let exists = FileStorage::exists(&checkpoint_file_path);

        if exists {
            let file = File::open(checkpoint_file_path)?;
            let reader = BufReader::new(file);
            let checkpoint: Checkpoint = serde_json::from_reader(reader)
                .map_err(|e| WalError::CorruptedEntry(format!("Can not open WAL checkpoint file: {}", e.to_string())))?;
            Ok(checkpoint)
        } else {
            Err(WalError::NoCheckpoint("No checkpoint found".into()))
        }
    }

    fn open_segment_by_id(&self, segment_id: u32) -> Result<Segment, WalError> {
        info!("Opening WAL directory entry");
        let segment_name = Segment::get_segment_name(segment_id);
        let segment_path = Path::new(self.dir.as_path())
            .join(segment_name);

        let file = File::open(&segment_path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(WalError::NotAFile(format!(
                "Expected a file but found: {:?}",
                segment_path
            )));
        }

        let segment = Segment::open(segment_path)?;
        Ok(segment)
    }
}

impl AppendResult {
    pub fn segment_id(&self) -> u32 {
        self.segment_id
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn length(&self) -> u32 {
        self.length
    }

    pub fn last_update(&self) -> u128 {
        self.last_update
    }
}

impl Writable for Checkpoint {
    fn serialize(&self) -> Vec<u8> {
        let json_string = serde_json::to_string(self)
            .unwrap();
        // Convert JSON string to Vec<u8>
        json_string.into_bytes()
    }

    fn serialized_size(&self) -> usize {
        todo!()
    }
}

impl Readable for Checkpoint {
    fn deserialize(buffer: &[u8]) -> Result<Self, WalError>
    where
        Self: Sized
    {
        let checkpoint: Checkpoint = serde_json::from_slice(buffer)
            .unwrap();
        Ok(checkpoint)
    }

    fn num_bytes_to_read() -> usize {
        size_of::<Checkpoint>()
    }
}

impl Checkpoint {

    pub fn checkpoint_id(&self) -> u32 {
        self.safe_segment
    }
}

fn open_segment(entry: DirEntry) -> Result<Segment, WalError> {
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
        .ok()?
        .filter_map(|e| e.ok())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .map(|ext| ext == "wal")
                .unwrap_or(false)
        })
        .collect();

    segments.sort_by_key(|entry| entry.path());

    segments.pop()
}

fn list_segments(path: &Path) -> Result<Vec<DirEntry>, WalError> {
    let mut segments: Vec<DirEntry> = fs::read_dir(path)
        .ok()
        .unwrap()
        .filter_map(|e| e.ok())      // ignore individual entry errors
        .filter(|entry| {
            entry
                .path()
                .extension()
                .map(|ext| ext == "wal")
                .unwrap_or(false)
        })
        .collect();
    segments.sort_by_key(|entry| entry.path());
    Ok(segments)
}

fn extract_segment_id(name: &str) -> Option<u32> {
    name.strip_prefix("segment_")?
        .strip_suffix(FILE_EXTENSION)?
        .parse::<u32>()
        .ok()
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

        let wal_files: Vec<_> = fs::read_dir(dir.path().join("wal"))
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

        let wal_files: Vec<_> = fs::read_dir(dir.path().join("wal"))
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
        wal.append(make_entry(b"hello")).expect("append failed");
    }

    #[test]
    fn append_multiple_entries_succeeds() {
        let dir = temp_dir();
        let mut wal = WAL::open(dir.path()).expect("open failed");
        for i in 0u8..10 {
            wal.append(make_entry(&[i; 32])).expect("append failed");
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
            wal.append(make_entry(&[0u8; 32])).expect("append failed");
        }

        let wal_files: Vec<_> = fs::read_dir(dir.path().join("wal"))
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

        let wal_files: Vec<_> = fs::read_dir(dir.path().join("wal"))
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

        let last = get_last_segment(&dir.path().join("wal")).expect("expected a segment");
        let name = last.file_name();
        let name = name.to_string_lossy();

        // The last file should sort highest — segment_0000000001.wal > segment_000000.wal
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