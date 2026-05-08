use std::collections::HashSet;
use crate::errors::EngineError;
use crate::flush_service::FlushService;
use crate::flush_worker::{DiskFlushWorker, FlushWorker};
use crate::index::service_name_index::{
    LocalServiceNameIndexReader, LocalServiceNameIndexWriter, ServiceNameIndex,
    ServiceNameIndexReader, ServiceNameIndexWriter,
};
use crate::memtable::Memtable;
use crate::search_request::SearchRequest;
use crate::span::{AttributeValue, SERVICE_NAME_ATTRIBUTE, Span, TraceId};
use crate::sstable_writer::SStableWriterImpl;
use crate::types::{MemtableConfig, StorageConfig};
use log::{error, info};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use wal::log_entry::LogEntry;
use wal::wal::WAL;
use crate::index::index_factory::{local_service_name_index, local_span_name_index};
use crate::index::span_name_index::{Pair, SpanNameIndex};

pub trait CorvusEngine: Send + Sync {
    fn start(&self);

    fn append(&self, spans: Vec<Span>) -> Result<(), EngineError>;

    fn replay_wal(&self, wal: &mut WAL, mem_table: &mut Memtable);

    fn search(&self, request: &SearchRequest) -> Vec<Span>;

    fn fetch_by_time(&self, start_ts: u64, end_ts: u64) -> Vec<Span>;

    fn fetch_trace(&self, trace_id: &TraceId) -> Vec<Span>;

    fn fetch_services(&self) -> Vec<String>;

    fn fetch_spans(&self, service_name: String) -> HashSet<String>;
}

#[derive(Debug, Clone, Deserialize)]
pub struct CorvusEngineConfig {
    pub mem_table_config: MemtableConfig,
}

pub struct CorvusEngineImpl {
    wal: Arc<Mutex<WAL>>,
    active_mem_table: Arc<Mutex<Memtable>>,
    config: CorvusEngineConfig,
    flush_service: FlushService,
    service_name_index: Arc<ServiceNameIndex>,
    span_name_index: Arc<SpanNameIndex>,
}

impl CorvusEngineImpl {}

impl CorvusEngineImpl {
    pub fn new(base_dir: PathBuf, mem_table: Arc<Mutex<Memtable>>, config: StorageConfig) -> Self {
        let mem_table_config = config.mem_table.clone();
        let max_block_size = config.max_block_size.clone();

        let wal_dir_path = Path::new(&base_dir);
        let wal = WAL::open(wal_dir_path).expect("could not open traces.wal");

        let wal = Arc::new(Mutex::new(wal));

        let stable_writer = SStableWriterImpl::new(Path::new(&base_dir).to_path_buf());

        let service_name_index = Arc::new(local_service_name_index(&base_dir));
        let span_name_index = Arc::new(local_span_name_index(&base_dir));
        service_name_index.load_or_create()
            .expect("could not load local service name index");
        span_name_index.load_or_create()
            .expect("could not load span name index");

        let flush_worker = Box::new(DiskFlushWorker::new(
            stable_writer,
            Arc::clone(&service_name_index),
            Arc::clone(&span_name_index),
            max_block_size,
        ));

        let flush_worker: Arc<Mutex<Box<dyn FlushWorker + Send + Sync>>> =
            Arc::new(Mutex::new(flush_worker));

        let flush_service = FlushService::new(Arc::clone(&wal), flush_worker);

        let engine = Self {
            wal,
            active_mem_table: mem_table,
            config: CorvusEngineConfig { mem_table_config },
            flush_service,
            service_name_index,
            span_name_index,
        };

        engine
    }

    pub fn start_stats_logger(&self, mem_table: Arc<Mutex<Memtable>>) {
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(15));

                let stats = {
                    let mt = mem_table.lock().unwrap();
                    mt.stats()
                };

                info!(
                    "spans = {}, trace_ids = {}, time_keys = {}, services = {}, trace_refs = {}, time_refs = {}, service_refs = {}, spans_mb = {}, total_estimated_size_mb = {}",
                    stats.spans_len,
                    stats.trace_ids,
                    stats.time_index_keys,
                    stats.service_keys,
                    stats.trace_ids_refs,
                    stats.time_index_refs,
                    stats.service_index_refs,
                    stats.span_size_bytes / 1024 / 1024,
                    stats.total_estimated_size_bytes / 1024 / 1024
                );
            }
        });
    }
}

impl CorvusEngine for CorvusEngineImpl {
    fn start(&self) {
        let mut wal = self.wal.lock().unwrap();
        let mut mem_table = self.active_mem_table.lock().unwrap();
        self.replay_wal(&mut wal, &mut mem_table);
        self.start_stats_logger(Arc::clone(&self.active_mem_table));
    }

    fn append(&self, spans: Vec<Span>) -> Result<(), EngineError> {
        {
            let mut wal = self.wal.lock().unwrap();
            let mut mem_table = self.active_mem_table.lock().unwrap();
            for span in spans {
                let vector = span.serialize();
                let log_entry = LogEntry::new(vector);

                match wal.append(log_entry) {
                    Ok(_) => {
                        let trace_id = span.trace_id;

                        if let Some(service_name) = span
                            .attributes
                            .get(SERVICE_NAME_ATTRIBUTE)
                            .and_then(AttributeValue::as_str)
                        {
                            self.service_name_index.add(service_name);
                            self.span_name_index.add(Pair {
                                span_name: span.name.clone(),
                                service_name: service_name.to_string(),
                            })
                        }

                        mem_table.insert(&trace_id, span);
                    }
                    Err(error) => {
                        error!("Error occurred while appending log entry: {:?}", error);
                    }
                };
            }
        }

        {
            let (old_memtable, checkpoint) = {
                let mut mem_table = self.active_mem_table.lock().unwrap();
                if !mem_table.should_flush() || self.flush_service.is_flushing() {
                    return Ok(());
                }

                let mut wal = self.wal.lock().unwrap();
                let checkpoint = wal
                    .rotate_segment()
                    .map_err(|e| EngineError::EngineError(e.to_string()))?;

                let next_memtable = mem_table.next_generation();
                let frozen_memtable = std::mem::replace(&mut *mem_table, next_memtable);
                (frozen_memtable, checkpoint)
            };

            self.flush_service.request_flush(old_memtable, checkpoint);
        }

        Ok(())
    }

    fn replay_wal(&self, wal: &mut WAL, mem_table: &mut Memtable) {
        info!("Replaying WAL, it takes a while");
        let entries = wal.replay().expect("Error while replaying WAL");

        let mut entries_count = 0;
        for entry in entries {
            let pointer = match entry {
                Ok(pointer) => pointer,
                Err(e) => {
                    error!("Error while reading WAL entry: {}", e.to_string());
                    continue;
                }
            };

            if let Some(payload) = pointer.payload {
                let span = Span::deserialize(&payload);
                let trace_id = span.trace_id.clone();
                mem_table.insert(&trace_id, span);
            }
            entries_count += 1;
        }
        info!("Reading {} WAL entries completed", entries_count);
    }

    fn search(&self, request: &SearchRequest) -> Vec<Span> {
        let mem_table = self.active_mem_table.lock().unwrap();
        let service_name = &request.service_name;
        let span_name = &request.span_name;
        let limit = request.limit.unwrap_or(usize::MAX);

        let spans = match service_name {
            Some(service_name) => mem_table.get_spans_by_service(service_name, limit),
            None => mem_table
                .entries()
                .iter()
                .flat_map(|(_trace_id, entry)| entry.get_spans().iter())
                .map(|span| Span::deserialize(span))
                .take(limit)
                .collect(),
        };

        let spans = spans
            .into_iter()
            .filter(|span| match &span_name {
                Some(name) => span.name == name.as_str(),
                None => true,
            })
            .collect::<Vec<Span>>();

        spans
    }

    fn fetch_by_time(&self, start_ts: u64, end_ts: u64) -> Vec<Span> {
        let mem_table = self.active_mem_table.lock().unwrap();
        let spans = mem_table.query_by_time(start_ts, end_ts);
        spans
    }

    fn fetch_trace(&self, trace_id: &TraceId) -> Vec<Span> {
        let mem_table = self.active_mem_table.lock().unwrap();
        mem_table.get_index(trace_id)
    }

    fn fetch_services(&self) -> Vec<String> {
        let services = self.service_name_index.list();
        services
    }

    fn fetch_spans(&self, service_name: String) -> HashSet<String> {
        self.span_name_index.list(service_name)
    }
}
