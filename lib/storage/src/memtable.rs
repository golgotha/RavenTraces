use crate::span::{SizeEstimator, Span, TraceId};
use crate::types::MemtableConfig;
use indexmap::IndexSet;
use log::{debug, info};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Microseconds(u64);

impl Microseconds {
    fn from_millis(ms: u64) -> Self {
        Microseconds(ms * 1_000)
    }
}

#[derive(Clone)]
pub struct Entry {
    span: Span,
}

pub struct Memtable {
    config: MemtableConfig,
    spans: Vec<Entry>,
    trace_index: HashMap<TraceId, Vec<usize>>,
    time_index: BTreeMap<Microseconds, Vec<usize>>,
    lru: IndexSet<TraceId>,
    services: HashMap<String, Vec<usize>>,
    min_segment_id: u32,
    max_segment_id: u32,
    size: usize,
}

impl Memtable {
    pub fn new(config: MemtableConfig) -> Memtable {
        let initial_capacity = config.initial_capacity;
        info!("Create Memtable with size: {}", initial_capacity);

        Memtable {
            config,
            spans: Vec::with_capacity(initial_capacity),
            trace_index: HashMap::new(),
            time_index: BTreeMap::new(),
            lru: IndexSet::new(),
            services: HashMap::new(),
            min_segment_id: 0,
            max_segment_id: 0,
            size: 0,
        }
    }

    pub fn next_memtable(&self) -> Memtable {
        let initial_capacity = self.config.initial_capacity;
        debug!("Create next Memtable version with size: {}", initial_capacity);
        Memtable::new(self.config.clone())
    }

    pub fn insert(&mut self, trace_id: &TraceId, span: &Span, segment_id: u32) {
        if self.spans.is_empty() {
            self.min_segment_id = segment_id;
        }

        self.touch(trace_id);
        let index = self.spans.len();
        let span_timestamp = span.timestamp;
        let estimated_size = span.estimated_size_bytes();
        let span_entry = Entry::new(span.clone());

        self.size += estimated_size;
        self.spans.push(span_entry);

        let pointers = self.trace_index.entry(*trace_id).or_insert_with(Vec::new);

        self.time_index
            .entry(Microseconds(span_timestamp))
            .or_default()
            .push(index);

        pointers.push(index);

        let local_service = span.local_service.clone();
        let service_name = if local_service.is_some() {
            local_service.unwrap()
        } else {
            "Unknown".to_string()
        };
        self.services
            .entry(service_name)
            .or_insert_with(Vec::new)
            .push(index);
        self.max_segment_id = segment_id;
    }

    pub fn get_index(&self, trace_id: &TraceId) -> Option<Vec<Span>> {
        self.trace_index.get(trace_id).map(|indices| {
            indices
                .iter()
                .map(|&i| &self.spans[i])
                .map(|entry: &Entry| entry.span.clone())
                .collect::<Vec<Span>>()
        })
    }

    pub fn query_by_time(&self, start: u64, end: u64) -> Vec<Span> {
        self.time_index
            .range(Microseconds::from_millis(start)..=Microseconds::from_millis(end))
            .flat_map(|(_, indices)| {
                indices
                    .iter()
                    .map(|&i| &self.spans[i])
                    .map(|entry: &Entry| entry.span.clone())
            })
            .collect()
    }

    pub fn get_spans_by_service(&self, service: &str, limit: usize) -> Option<Vec<Span>> {
        self.services.get(service).map(|indices| {
            indices
                .iter()
                .map(|&i| &self.spans[i])
                .map(|entry: &Entry| &entry.span)
                .take(limit)
                .cloned()
                .collect::<Vec<Span>>()
        })
    }

    pub fn entries(&self) -> &Vec<Entry> {
        &self.spans
    }

    pub fn len(&self) -> usize {
        self.trace_index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.trace_index.is_empty()
    }

    pub fn clear(&mut self) {
        self.spans.clear();
        self.trace_index.clear();
        self.time_index.clear();
        self.services.clear();
        self.lru.clear();
        self.size = 0;
    }

    pub fn should_flush(&mut self) -> bool {
        &self.size > &self.config.max_size_bytes
    }

    pub fn min_segment_id(&self) -> u32 {
        self.min_segment_id
    }

    pub fn max_segment_id(&self) -> u32 {
        self.max_segment_id
    }

    pub fn services(&self) -> Vec<String> {
        self.services.keys().cloned().collect::<Vec<String>>()
    }

    fn touch(&mut self, trace_id: &TraceId) {
        self.lru.shift_remove(trace_id);
        self.lru.insert(*trace_id);
    }
}

impl Entry {
    fn new(span: Span) -> Self {
        Entry { span }
    }

    pub fn len(&self) -> usize {
        self.span.estimated_size_bytes()
    }

    pub fn get_span(&self) -> &Span {
        &self.span
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
            local_service: None,
            remote_service: None,
        }
    }

    fn make_default_config() -> MemtableConfig {
        MemtableConfig {
            max_size_bytes: 1024,
            initial_capacity: 10,
        }
    }

    #[test]
    fn create_memtable() {
        let config = make_default_config();
        let memtable = Memtable::new(config);
        assert_eq!(memtable.config.initial_capacity, 10)
    }

    #[test]
    fn push_span() {
        let trace_id: TraceId = TraceId(*b"5af7183fb1d4cf5f");
        let span_id: SpanId = SpanId(*b"5af7183f");
        let unified_span = make_unified_span(trace_id, span_id);
        let config = make_default_config();
        let mut memtable = Memtable::new(config);
        memtable.insert(&trace_id, &unified_span, 1);

        assert_eq!(memtable.trace_index.len(), 1);

        let actual_entry_index = memtable.trace_index.get(&trace_id);
        assert!(actual_entry_index.is_some());
        let actual_span_index_vec = actual_entry_index.unwrap();
        let actual_span_index = actual_span_index_vec.get(0).unwrap();
        let actual_span: &Entry = &memtable.spans[*actual_span_index];
        let actual_span_data = actual_span.span.clone();

        assert_eq!(actual_span_data.trace_id, trace_id);
        assert_eq!(actual_span_data.span_id, span_id);
        assert_eq!(actual_span_data.parent_span_id, None);
        assert_eq!(actual_span_data.name, "Test span");
        assert_eq!(actual_span_data.kind, SpanKind::Internal);
        assert_eq!(actual_span_data.timestamp, 1775335409772);
        assert_eq!(actual_span_data.duration, 500);
    }

    #[test]
    fn get_index() {
        let config = make_default_config();
        let mut memtable = Memtable::new(config);
        let trace_ids: [TraceId; 2] =
            [TraceId(*b"5af7183fb1d4cf5f"), TraceId(*b"6b221d5bc9e6496c")];
        let batch1: &[Span] = &[
            make_unified_span(TraceId(*b"5af7183fb1d4cf5f"), SpanId(*b"5af7183f")),
            make_unified_span(TraceId(*b"5af7183fb1d4cf5f"), SpanId(*b"b1d4cf5f")),
        ];

        let batch2: &[Span] = &[
            make_unified_span(TraceId(*b"6b221d5bc9e6496c"), SpanId(*b"6b221d5b")),
            make_unified_span(TraceId(*b"6b221d5bc9e6496c"), SpanId(*b"c9e6496c")),
        ];

        memtable.insert(&trace_ids[0], &batch1[0], 1);
        memtable.insert(&trace_ids[0], &batch1[1], 1);

        memtable.insert(&trace_ids[1], &batch2[0], 1);
        memtable.insert(&trace_ids[1], &batch2[1], 1);

        // memtable.insert_batch_ref(&trace_ids[1], batch2);
        assert_eq!(memtable.trace_index.len(), 2);

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
                initial_capacity: 2,
            };
            let mut m = Memtable::new(config);
            m.insert(
                &tid(*b"5af7183fb1d4cf5f"),
                &ptr(tid(*b"5af7183fb1d4cf5f")),
                1,
            );
            m.insert(
                &tid(*b"5af7183fb1d4cf5a"),
                &ptr(tid(*b"5af7183fb1d4cf5a")),
                1,
            );
            m.insert(
                &tid(*b"5af7183fb1d4cf5b"),
                &ptr(tid(*b"5af7183fb1d4cf5b")),
                1,
            ); // should evict tid(1)

            assert_eq!(m.len(), 2);

            let spans1 = m.get_index(&tid(*b"5af7183fb1d4cf5f"));
            let spans2 = m.get_index(&tid(*b"5af7183fb1d4cf5a"));
            let spans3 = m.get_index(&tid(*b"5af7183fb1d4cf5b"));

            assert!(
                spans1.unwrap().is_empty(),
                "tid(1) should have been evicted"
            );
            assert!(spans2.unwrap().len() > 0);
            assert!(spans3.unwrap().len() > 0);
        }

        #[test]
        fn access_promotes_to_most_recent() {
            let trace_1: TraceId = tid(*b"5af7183fb1d4cf5f");
            let trace_2: TraceId = tid(*b"5af7183fb1d4cf5a");
            let trace_3: TraceId = tid(*b"5af7183fb1d4cf5b");
            let config = MemtableConfig {
                max_size_bytes: 1024,
                initial_capacity: 2,
            };
            let mut m = Memtable::new(config);

            m.insert(&trace_1, &ptr(trace_1), 1);
            m.insert(&trace_2, &ptr(trace_2), 1);
            // re-touch tid(1) so tid(2) becomes the oldest
            m.insert(&trace_1, &ptr(trace_1), 1);
            m.insert(&trace_3, &ptr(trace_3), 1); // should evict tid(2)

            let spans_trace2 = m.get_index(&trace_2);
            let spans_trace1 = m.get_index(&trace_1);
            let spans_trace3 = m.get_index(&trace_3);
            assert!(
                spans_trace2.unwrap().is_empty(),
                "tid(2) should have been evicted"
            );
            assert!(spans_trace1.unwrap().len() > 0);
            assert!(spans_trace3.unwrap().len() > 0);
        }

    }
}
