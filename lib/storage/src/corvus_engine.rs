use crate::errors::EngineError;
use crate::flush_worker::{DiskFlushWorker, FlushWorker};
use crate::memtable::{Memtable, MemtableConfig};
use crate::span::Span;
use crate::sstable_writer::SStableWriterImpl;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use serde::{Deserialize, Serialize};
use wal::log_entry::LogEntry;
use wal::wal::WAL;

pub trait CorvusEngine: Send + Sync {

    fn start(&mut self);

    fn append(&mut self, spans: &Vec<Span>) -> Result<(), EngineError>;

    fn replay_wal(wal: &mut WAL, memtable: &mut Memtable);
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CorvusEngineConfig {
    pub memtable_config: MemtableConfig
}

pub struct CorvusEngineImpl {
    inner: Arc<Mutex<CorvusEngineState>>,
    config: CorvusEngineConfig,
}

struct CorvusEngineState {
    wal: WAL,
    memtable: Memtable,
    flush_worker: Box<dyn FlushWorker>,
}

impl CorvusEngineImpl {
}

impl CorvusEngineImpl {
    pub fn new(base_dir: PathBuf, config: CorvusEngineConfig) -> Self {
        let memtable_config = config.memtable_config.clone();
        let memtable = Memtable::new(memtable_config);

        let wal_dir_path = Path::new(&base_dir);
        let wal = WAL::open(wal_dir_path).
            expect("could not open traces.wal");

        let stable_writer = SStableWriterImpl::new(Path::new(&base_dir).to_path_buf());
        let flush_worker = Box::new(DiskFlushWorker::new(stable_writer));

        let engine_state = CorvusEngineState {
            wal,
            memtable,
            flush_worker,
        };

        Self {
            inner: Arc::new(Mutex::new(engine_state)),
            config,
        }
    }
}

impl CorvusEngine for CorvusEngineImpl {

    fn start(&mut self) {
        let mut state = self.inner.lock().unwrap();
        let CorvusEngineState {
            wal,
            memtable,
            ..
        } = &mut *state;

        Self::replay_wal(wal, memtable);
    }

    fn append(&mut self, spans: &Vec<Span>) -> Result<(), EngineError> {
        let mut state = self.inner.lock().unwrap();
        let CorvusEngineState {
            wal,
            memtable,
            flush_worker,
            ..
        } = &mut *state;

        for span in spans {
            let vector = span.serialize();
            let log_entry = LogEntry::new(vector);

            let append_result = wal.append(&log_entry).expect("cannot append log entry");

            let segment_id = append_result.segment_id();
            memtable.insert(&span.trace_id, span.clone(), segment_id);
        }

        if memtable.should_flush() {
            let mut old_memtable = std::mem::replace(&mut *memtable, Memtable::new(self.config.memtable_config.clone()));
            flush_worker
                .flush(wal, &mut old_memtable)
                .expect("cannot flush memtable");
        }

        Ok(())
    }

    fn replay_wal(wal: &mut WAL, memtable: &mut Memtable) {
        let entries = {
            wal.replay().unwrap()
        };

        entries.into_iter().for_each(|entry| {
            if let Some(payload) = entry.payload {
                let span = Span::deserialize(payload);
                let trace_id = span.trace_id.clone();
                memtable
                    .insert(&trace_id, span, entry.segment_id);
            }
        });
    }
}
