use std::fs;
use std::fs::{DirEntry, File};
use std::path::{Path, PathBuf};
use log::{info, warn, error};
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
    path: PathBuf
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

        let wal = WAL { active_segment, options, path: path.to_path_buf() };
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
        let segment  = self.active_segment.create_next_segment(self.path.to_path_buf())?;
        self.active_segment = segment;
        Ok(())
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

    // let filename = entry.file_name()
    //     .into_string()
    //     .map_err(|_| WalError::CorruptedEntry("Wal segment filename error".into()))?;

    // let splitted_name = filename.split_once("_");
    // splitted_name.map(|s| s.1), splitted_name.map(|s| s.0)
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