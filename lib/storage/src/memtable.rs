use crate::span::{TraceId, UnifiedSpan};
use indexmap::IndexSet;
use log::info;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Microseconds(u64);

impl Microseconds {
    fn from_millis(ms: u64) -> Self {
        Microseconds(ms * 1_000)
    }
}

pub struct Memtable {
    spans: Vec<Arc<UnifiedSpan>>,
    trace_index: HashMap<TraceId, Vec<usize>>,
    time_index: BTreeMap<Microseconds, Vec<usize>>,
    lru: IndexSet<TraceId>,
    size: usize,
}

impl Memtable {
    pub fn new(size: usize) -> Memtable {
        info!("Create Memtable with size: {}", size);
        Memtable {
            spans: Vec::with_capacity(size),
            trace_index: HashMap::new(),
            time_index: BTreeMap::new(),
            lru: IndexSet::new(),
            size,
        }
    }

    pub fn insert(&mut self, trace_id: &TraceId, span: UnifiedSpan) {
        self.touch(trace_id);
        let index = self.spans.len();
        let span_timestamp = span.timestamp;
        let span_arc = Arc::new(span);
        self.spans.push(Arc::clone(&span_arc));

        let pointers = self.trace_index.entry(*trace_id).or_insert_with(Vec::new);

        self.time_index
            .entry(Microseconds(span_timestamp))
            .or_default()
            .push(index);

        pointers.push(index);
        self.evict();
    }

    pub fn get_index(&self, trace_id: &TraceId) -> Vec<Arc<UnifiedSpan>> {
        self.trace_index
            .get(trace_id)
            .map(|indices| {
                indices.iter()
                    .map(|&i| Arc::clone(&self.spans[i]))
                    .collect::<Vec<Arc<UnifiedSpan>>>()
            })
            .unwrap_or_default()
    }

    pub fn query_by_time(&self, start: u64, end: u64) -> Vec<&UnifiedSpan> {
        self.time_index
            .range(Microseconds::from_millis(start)..=Microseconds::from_millis(end))
            .flat_map(|(_, indices)| indices.iter().map(|&i| &*self.spans[i]))
            .collect()
    }

    pub fn traces(&self) -> &[Arc<UnifiedSpan>] {
        &self.spans
    }

    pub fn len(&self) -> usize {
        self.trace_index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.trace_index.is_empty()
    }

    fn touch(&mut self, trace_id: &TraceId) {
        self.lru.shift_remove(trace_id);
        self.lru.insert(*trace_id);
    }

    fn evict(&mut self) {
        while self.len() > self.size {
            info!("Evicting an element from Memtable");

            if let Some(trace_id) = self.lru.shift_remove_index(0) {
                if let Some(indices) = self.trace_index.remove(&trace_id) {
                    let mut sorted_indices = indices.clone();
                    sorted_indices.sort_unstable_by(|a, b| b.cmp(a));

                    for i in sorted_indices {
                        // Clean up time_index for the span being removed
                        let key = Microseconds(self.spans[i].timestamp);
                        if let Some(idx_list) = self.time_index.get_mut(&key) {
                            idx_list.retain(|&x| x != i);
                            if idx_list.is_empty() {
                                self.time_index.remove(&key);
                            }
                        }

                        // swap_remove swaps this slot with the last element
                        let last = self.spans.len() - 1;
                        if i != last {
                            self.spans.swap(i, last);

                            // The span that was at `last` is now at `i` — remap its indices
                            let moved_span = &self.spans[i];
                            let moved_trace_id = moved_span.trace_id;

                            // Fix trace_index
                            if let Some(idx_list) = self.trace_index.get_mut(&moved_trace_id) {
                                for idx in idx_list.iter_mut() {
                                    if *idx == last {
                                        *idx = i;
                                    }
                                }
                            }

                            // Fix time_index
                            let moved_key = Microseconds(moved_span.timestamp);
                            if let Some(idx_list) = self.time_index.get_mut(&moved_key) {
                                for idx in idx_list.iter_mut() {
                                    if *idx == last {
                                        *idx = i;
                                    }
                                }
                            }
                        }

                        self.spans.pop();
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::span::{SpanId, SpanKind};

    fn make_unified_span(trace_id: TraceId, span_id: SpanId) -> UnifiedSpan {
        UnifiedSpan {
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

    #[test]
    fn create_memtable() {
        let memtable = Memtable::new(10);
        assert_eq!(memtable.size, 10)
    }

    #[test]
    fn push_span() {
        let trace_id: TraceId = TraceId(*b"5af7183fb1d4cf5f");
        let span_id: SpanId = SpanId(*b"5af7183f");
        let unified_span = make_unified_span(trace_id, span_id);
        let mut memtable = Memtable::new(10);
        memtable.insert(&trace_id, unified_span.clone());

        assert_eq!(memtable.trace_index.len(), 1);

        let actual_entry_index = memtable.trace_index.get(&trace_id);
        assert!(actual_entry_index.is_some());
        let actual_span_index_vec = actual_entry_index.unwrap();
        let actual_span_index = actual_span_index_vec.get(0).unwrap();
        let actual_span = memtable.spans[*actual_span_index].clone();

        assert_eq!(actual_span.trace_id, trace_id);
        assert_eq!(actual_span.span_id, span_id);
        assert_eq!(actual_span.parent_span_id, None);
        assert_eq!(actual_span.name, "Test span");
        assert_eq!(actual_span.kind, SpanKind::Internal);
        assert_eq!(actual_span.timestamp, 1775335409772);
        assert_eq!(actual_span.duration, 500);
    }

    #[test]
    fn get_index() {
        let mut memtable = Memtable::new(10);
        let trace_ids: [TraceId; 2] =
            [TraceId(*b"5af7183fb1d4cf5f"), TraceId(*b"6b221d5bc9e6496c")];
        let batch1: &[UnifiedSpan] = &[
            make_unified_span(TraceId(*b"5af7183fb1d4cf5f"), SpanId(*b"5af7183f")),
            make_unified_span(TraceId(*b"5af7183fb1d4cf5f"), SpanId(*b"b1d4cf5f")),
        ];

        let batch2: &[UnifiedSpan] = &[
            make_unified_span(TraceId(*b"6b221d5bc9e6496c"), SpanId(*b"6b221d5b")),
            make_unified_span(TraceId(*b"6b221d5bc9e6496c"), SpanId(*b"c9e6496c")),
        ];

        memtable.insert(&trace_ids[0], batch1[0].clone());
        memtable.insert(&trace_ids[0], batch1[1].clone());

        memtable.insert(&trace_ids[1], batch2[0].clone());
        memtable.insert(&trace_ids[1], batch2[1].clone());

        // memtable.insert_batch_ref(&trace_ids[1], batch2);
        assert_eq!(memtable.trace_index.len(), 2);

        let actual_entry_for_trace_1 = memtable.get_index(&trace_ids[0]);
        // assert_span_pointer(actual_entry_for_trace_1, batch1);

        let actual_entry_for_trace_2 = memtable.get_index(&trace_ids[1]);
        // assert_span_pointer(actual_entry_for_trace_2, batch2);
    }

    fn assert_span_pointer(spans: &Vec<UnifiedSpan>, expected_batch: &[UnifiedSpan]) {
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

        fn ptr(trace_id: TraceId) -> UnifiedSpan {
            make_unified_span(trace_id, SpanId(*b"5af7183f"))
        }

        fn tid(id: [u8; 16]) -> TraceId {
            TraceId(id)
        }

        #[test]
        fn evicts_oldest_when_full() {
            let mut m = Memtable::new(2);
            m.insert(&tid(*b"5af7183fb1d4cf5f"), ptr(tid(*b"5af7183fb1d4cf5f")));
            m.insert(&tid(*b"5af7183fb1d4cf5a"), ptr(tid(*b"5af7183fb1d4cf5a")));
            m.insert(&tid(*b"5af7183fb1d4cf5b"), ptr(tid(*b"5af7183fb1d4cf5b"))); // should evict tid(1)

            assert_eq!(m.len(), 2);

            let spans1 = m.get_index(&tid(*b"5af7183fb1d4cf5f"));
            let spans2 = m.get_index(&tid(*b"5af7183fb1d4cf5a"));
            let spans3 = m.get_index(&tid(*b"5af7183fb1d4cf5b"));

            assert!(
                spans1.is_empty(),
                "tid(1) should have been evicted"
            );
            assert!(spans2.len() > 0);
            assert!(spans3.len() > 0);
        }

        #[test]
        fn access_promotes_to_most_recent() {
            let trace_1: TraceId = tid(*b"5af7183fb1d4cf5f");
            let trace_2: TraceId = tid(*b"5af7183fb1d4cf5a");
            let trace_3: TraceId = tid(*b"5af7183fb1d4cf5b");
            let mut m = Memtable::new(2);
            m.insert(&trace_1, ptr(trace_1));
            m.insert(&trace_2, ptr(trace_2));
            // re-touch tid(1) so tid(2) becomes the oldest
            m.insert(&trace_1, ptr(trace_1));
            m.insert(&trace_3, ptr(trace_3)); // should evict tid(2)

            let spans_trace2 = m.get_index(&trace_2);
            let spans_trace1 = m.get_index(&trace_1);
            let spans_trace3 = m.get_index(&trace_3);
            assert!(
                spans_trace2.is_empty(),
                "tid(2) should have been evicted"
            );
            assert!(spans_trace1.len() > 0);
            assert!(spans_trace3.len() > 0);
        }

        // #[test]
        // fn size_zero_means_unbounded() {
        //     let mut m = Memtable::new(0);
        //     for i in 0..1000 {
        //         m.push(&tid(i), ptr(i));
        //     }
        //     assert_eq!(m.len(), 1000);
        // }
    }
}
