use crate::span::{AttributeValue, SizeEstimator, Span, TraceId};
use crate::types::MemtableConfig;
use log::{debug, info};
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};
use metrics::metrics;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Microseconds(u64);

impl Microseconds {
    fn from_millis(ms: u64) -> Self {
        Microseconds(ms * 1_000)
    }
}

pub struct Entry {
    trace_id: TraceId,
    spans: Vec<Vec<u8>>,
    estimated_size_bytes: usize,
    min_ts: u64,
    max_ts: u64,
}

pub struct MemtableStats {
    pub spans_len: usize,
    pub trace_ids: usize,
    pub time_index_keys: usize,
    pub service_keys: usize,
    pub trace_ids_refs: usize,
    pub time_index_refs: usize,
    pub service_index_refs: usize,
    pub span_size_bytes: usize,
    pub total_estimated_size_bytes: usize,
    pub generation: u64,
}

pub struct Memtable {
    generation: u64,
    config: MemtableConfig,
    traces: HashMap<u64, Entry>,
    time_index: BTreeMap<Microseconds, Vec<u64>>,
    services: HashMap<String, Vec<u64>>,
    size: usize,
    created_at: Instant,
}

impl Memtable {
    pub fn new(config: MemtableConfig, generation: u64) -> Memtable {
        let initial_capacity = config.initial_capacity;
        info!("Create Memtable with size: {}", initial_capacity);

        Memtable {
            generation,
            config,
            traces: HashMap::with_capacity(initial_capacity),
            time_index: BTreeMap::new(),
            services: HashMap::new(),
            size: 0,
            created_at: Instant::now(),
        }
    }

    pub fn next_generation(&self) -> Memtable {
        let initial_capacity = self.config.initial_capacity;
        debug!("Create next Memtable version with size: {}", initial_capacity);
        metrics::MEMTABLE_ENTRIES.set(0);
        metrics::MEMTABLE_SIZE_BYTES.set(0);
        Memtable::new(self.config.clone(), self.generation + 1)
    }

    pub fn insert(&mut self, trace_id: &TraceId, span: Span) {
        let trache_id_key = trace_id.fnv1a_64();
        let span_timestamp = span.timestamp;
        let estimated_size = span.estimated_size_bytes();

        self.size += estimated_size;

        let trace = self.traces.entry(trache_id_key).or_insert_with(|| Entry {
            trace_id: *trace_id,
            spans: vec![],
            estimated_size_bytes: 0,
            min_ts: span_timestamp,
            max_ts: span_timestamp,
        });

        trace.min_ts = trace.min_ts.min(span_timestamp);
        trace.max_ts = trace.max_ts.max(span_timestamp);
        trace.spans.push(span.serialize());

        self.time_index
            .entry(Microseconds(span_timestamp))
            .or_default()
            .push(trache_id_key);

        let service_name = span.attributes.get("service.name")
            .and_then(AttributeValue::as_str)
            .unwrap_or_else(|| "Unknown");

        self.services
            .entry(service_name.to_string())
            .or_insert_with(Vec::new)
            .push(trache_id_key);

        metrics::MEMTABLE_SIZE_BYTES.set(self.size as i64);
        metrics::MEMTABLE_ENTRIES.inc();
        metrics::MEMTABLE_WRITES.inc();
    }

    pub fn get_index(&self, trace_id: &TraceId) -> Vec<Span> {
        metrics::MEMTABLE_READS.inc();
        let trace_id_key = trace_id.fnv1a_64();
        let Some(entry) = self.traces.get(&trace_id_key) else {
            return Vec::new();
        };

        entry.spans
            .iter()
            .map(|span| Span::deserialize(span))
            .collect()
    }

    pub fn query_by_time(&self, start: u64, end: u64) -> Vec<Span> {
        metrics::MEMTABLE_READS.inc();
        self.time_index
            .range(Microseconds::from_millis(start)..=Microseconds::from_millis(end))
            .flat_map(|(_, keys)| keys.iter())
            .filter_map(|key| self.traces.get(key))
            .flat_map(|entry| entry.spans.iter())
            .map(|span| Span::deserialize(span))
            .collect()
    }

    pub fn get_spans_by_service(&self, service: &str, limit: usize) -> Vec<Span> {
        metrics::MEMTABLE_READS.inc();
        let Some(hashes) = self.services.get(service)  else {
            return Vec::new()
        };

        hashes
            .iter()
            .filter_map(|key| self.traces.get(key))
            .flat_map(|entry| entry.spans.iter())
            .map(|span| Span::deserialize(span))
            .take(limit)
            .collect()
    }

    pub fn entries(&self) -> &HashMap<u64, Entry>  {
        metrics::MEMTABLE_READS.inc();
        &self.traces
    }

    pub fn len(&self) -> usize {
        self.traces.len()
    }

    pub fn is_empty(&self) -> bool {
        self.traces.is_empty()
    }

    pub fn should_flush(&mut self) -> bool {
        &self.size > &self.config.max_size_bytes ||
            self.age() > Duration::from_secs(self.config.max_age_seconds)
    }

    pub fn services(&self) -> Vec<String> {
        self.services.keys().into_iter().cloned().collect::<Vec<String>>()
    }
    
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn stats(&self) -> MemtableStats {
        MemtableStats {
            spans_len: self.traces.len(),
            trace_ids: self.traces.len(),
            time_index_keys: self.time_index.len(),
            service_keys: self.services.len(),
            trace_ids_refs: self.traces.values().map(|v| v.len()).sum(),
            time_index_refs: self.time_index.values().map(|v| v.len()).sum(),
            service_index_refs: self.services.values().map(|v| v.len()).sum(),
            span_size_bytes: self.size,
            total_estimated_size_bytes: self.estimated_size_bytes(),
            generation: self.generation,
        }
    }

    pub fn estimated_size_bytes(&self) -> usize {
        let mut size = self.size;
        size += size_of::<u64>();
        // size += self.trace_index.capacity() * (size_of::<TraceId>() + size_of::<Vec<usize>>());
        // size += self.trace_index
        //     .values()
        //     .map(|v| v.capacity() * size_of::<usize>())
        //     .sum::<usize>();

        size += self.time_index.len() * (size_of::<Microseconds>() + size_of::<Vec<usize>>());
        size += self.time_index
            .values()
            .map(|v| v.capacity() * size_of::<usize>())
            .sum::<usize>();

        size += self.services.capacity() * (size_of::<String>() + size_of::<Vec<usize>>());
        size += self.services
            .iter()
            .map(|(service, indexes)| {
                service.capacity() + indexes.capacity() * size_of::<usize>()
            })
            .sum::<usize>();
        size
    }

    fn age(&self) -> Duration {
        self.created_at.elapsed()
    }
}

impl Entry {

    pub fn trace_id(&self) -> TraceId {
        self.trace_id
    }

    pub fn len(&self) -> usize {
        self.estimated_size_bytes
    }

    pub fn get_spans(&self) -> &Vec<Vec<u8>> {
        &self.spans
    }

    pub fn min_ts(&self) -> u64 {
        self.min_ts
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}

impl Drop for Memtable {
    fn drop(&mut self) {
        info!(
            "DROP memtable gen={}, spans={}, capacity={}, vec_mb={}",
            self.generation,
            self.traces.len(),
            // self.spans.capacity(),
            self.traces.len(),
            // self.spans.capacity() * size_of::<Entry>() / 1024 / 1024,
            self.traces.len() * size_of::<Entry>() / 1024 / 1024,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::span::{SpanId, SpanKind};

    fn make_unified_span(trace_id: TraceId, span_id: SpanId) -> Span {
        Span {
            trace_id,
            span_id,
            parent_span_id: None,
            name: "Test span".to_string(),
            kind: SpanKind::Internal,
            timestamp: 1775335409772,
            duration: 500,
            attributes: Default::default(),
            events: vec![],
            status_code: None,
            status_message: None,
        }
    }

    fn make_default_config() -> MemtableConfig {
        MemtableConfig {
            max_size_bytes: 1024,
            max_age_seconds: 10,
            initial_capacity: 10,
        }
    }

    #[test]
    fn create_memtable() {
        let config = make_default_config();
        let memtable = Memtable::new(config, 1);
        assert_eq!(memtable.config.initial_capacity, 10)
    }

    #[test]
    fn push_span() {
        let trace_id: TraceId = TraceId(*b"5af7183fb1d4cf5f");
        let span_id: SpanId = SpanId(*b"5af7183f");
        let unified_span = make_unified_span(trace_id, span_id);
        let config = make_default_config();
        let mut memtable = Memtable::new(config, 1);
        memtable.insert(&trace_id, unified_span);

        assert_eq!(memtable.traces.len(), 1);

        let actual_entry_index = memtable.traces.get(&trace_id.fnv1a_64());
        assert!(actual_entry_index.is_some());
        let actual_span_index_vec = actual_entry_index.unwrap();
        let actual_span_data = actual_span_index_vec.get_spans().get(0).unwrap();

        assert!(actual_span_data.len() > 0);
        // assert_eq!(actual_span_data.trace_id, trace_id);
        // assert_eq!(actual_span_data.span_id, span_id);
        // assert_eq!(actual_span_data.parent_span_id, None);
        // assert_eq!(actual_span_data.name, "Test span");
        // assert_eq!(actual_span_data.kind, SpanKind::Internal);
        // assert_eq!(actual_span_data.timestamp, 1775335409772);
        // assert_eq!(actual_span_data.duration, 500);
    }

    #[test]
    fn get_index() {
        let config = make_default_config();
        let mut memtable = Memtable::new(config, 1);
        let trace_ids: [TraceId; 2] =
            [TraceId(*b"5af7183fb1d4cf5f"), TraceId(*b"6b221d5bc9e6496c")];
        let batch1 = [
            make_unified_span(TraceId(*b"5af7183fb1d4cf5f"), SpanId(*b"5af7183f")),
            make_unified_span(TraceId(*b"5af7183fb1d4cf5f"), SpanId(*b"b1d4cf5f")),
        ];

        let batch2 = [
            make_unified_span(TraceId(*b"6b221d5bc9e6496c"), SpanId(*b"6b221d5b")),
            make_unified_span(TraceId(*b"6b221d5bc9e6496c"), SpanId(*b"c9e6496c")),
        ];

        memtable.insert(&trace_ids[0], batch1[0].clone());
        memtable.insert(&trace_ids[0], batch1[1].clone());

        memtable.insert(&trace_ids[1], batch2[0].clone());
        memtable.insert(&trace_ids[1], batch2[1].clone());

        // memtable.insert_batch_ref(&trace_ids[1], batch2);
        assert_eq!(memtable.traces.len(), 2);

        let actual_entry_for_trace_1 = memtable.get_index(&trace_ids[0]);
        // assert_span_pointer(actual_entry_for_trace_1, batch1);

        let actual_entry_for_trace_2 = memtable.get_index(&trace_ids[1]);
        // assert_span_pointer(actual_entry_for_trace_2, batch2);
    }

    fn assert_span_pointer(spans: &Vec<Span>, expected_batch: &[Span]) {
        spans.iter().enumerate().for_each(|(index, sp)| {
            assert_eq!(sp.trace_id, expected_batch[index].trace_id);
            assert_eq!(sp.span_id, expected_batch[index].span_id);
            assert_eq!(sp.name, expected_batch[index].name);
            assert_eq!(sp.timestamp, expected_batch[index].timestamp);
            assert_eq!(sp.duration, expected_batch[index].duration);
        });
    }

    mod trace_eviction {
        use super::*;

        fn ptr(trace_id: TraceId) -> Span {
            make_unified_span(trace_id, SpanId(*b"5af7183f"))
        }

        fn tid(id: [u8; 16]) -> TraceId {
            TraceId(id)
        }

        #[test]
        fn evicts_oldest_when_full() {
            let config = MemtableConfig {
                max_size_bytes: 1024,
                max_age_seconds: 10,
                initial_capacity: 2,
            };
            let mut m = Memtable::new(config, 1);
            m.insert(
                &tid(*b"5af7183fb1d4cf5f"),
                ptr(tid(*b"5af7183fb1d4cf5f")),
            );
            m.insert(
                &tid(*b"5af7183fb1d4cf5a"),
                ptr(tid(*b"5af7183fb1d4cf5a")),
            );
            m.insert(
                &tid(*b"5af7183fb1d4cf5b"),
                ptr(tid(*b"5af7183fb1d4cf5b")),
            ); // should evict tid(1)

            assert_eq!(m.len(), 2);

            let spans1 = m.get_index(&tid(*b"5af7183fb1d4cf5f"));
            let spans2 = m.get_index(&tid(*b"5af7183fb1d4cf5a"));
            let spans3 = m.get_index(&tid(*b"5af7183fb1d4cf5b"));

            assert!(spans1.is_empty(), "tid(1) should have been evicted");
            assert!(spans2.len() > 0);
            assert!(spans3.len() > 0);
        }

        #[test]
        fn access_promotes_to_most_recent() {
            let trace_1: TraceId = tid(*b"5af7183fb1d4cf5f");
            let trace_2: TraceId = tid(*b"5af7183fb1d4cf5a");
            let trace_3: TraceId = tid(*b"5af7183fb1d4cf5b");
            let config = MemtableConfig {
                max_size_bytes: 1024,
                max_age_seconds: 10,
                initial_capacity: 2,
            };
            let mut m = Memtable::new(config, 1);

            m.insert(&trace_1, ptr(trace_1));
            m.insert(&trace_2, ptr(trace_2));
            // re-touch tid(1) so tid(2) becomes the oldest
            m.insert(&trace_1, ptr(trace_1));
            m.insert(&trace_3, ptr(trace_3)); // should evict tid(2)

            let spans_trace2 = m.get_index(&trace_2);
            let spans_trace1 = m.get_index(&trace_1);
            let spans_trace3 = m.get_index(&trace_3);
            assert!(spans_trace2.is_empty(), "tid(2) should have been evicted");
            assert!(spans_trace1.len() > 0);
            assert!(spans_trace3.len() > 0);
        }
    }
}
