use crate::span::{TraceId};
use std::collections::{HashMap};
use indexmap::IndexSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanPointer {
    segment_id: u64,
    offset: u64,
    length: u32,
    last_update: u64,
}

pub struct Memtable {
    traces: HashMap<TraceId, Vec<SpanPointer>>,
    lru: IndexSet<TraceId>,
    size: usize,
}

impl Memtable {
    pub fn new(size: usize) -> Memtable {
        Memtable {
            traces: HashMap::new(),
            lru: IndexSet::new(),
            size,
        }
    }

    pub fn push(&mut self, trace_id: &TraceId, span: SpanPointer) {
        self.touch(trace_id);
        let pointers = self.traces.entry(*trace_id).or_insert_with(Vec::new);
        pointers.push(span);
        self.evict();
    }

    pub fn push_batch_ref(&mut self, trace_id: &TraceId, spans: &[SpanPointer])
    where
        SpanPointer: Clone,
    {
        self.touch(trace_id);
        self.traces
            .entry(*trace_id)
            .or_insert_with(Vec::new)
            .extend_from_slice(spans);
        self.evict();
    }

    pub fn get_index(&self, trace_id: &TraceId) -> Option<&Vec<SpanPointer>> {
        self.traces.get(trace_id)
    }

    pub fn len(&self) -> usize {
        self.traces.len()
    }

    pub fn is_empty(&self) -> bool {
        self.traces.is_empty()
    }

    fn touch(&mut self, trace_id: &TraceId) {
        self.lru.shift_remove(trace_id);
        self.lru.insert(*trace_id);
    }

    fn evict(&mut self) {
        while self.len() > self.size {
            if let Some(trace_id) = self.lru.shift_remove_index(0) {
                self.traces.remove(&trace_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_memtable() {
        let memtable = Memtable::new(10);
        assert_eq!(memtable.size, 10)
    }

    #[test]
    fn push_span() {
        let trace_id: &TraceId = b"5af7183fb1d4cf5f";
        let span_pointer = SpanPointer {
            segment_id: 1,
            offset: 100,
            length: 64,
            last_update: 5000,
        };
        let mut memtable = Memtable::new(10);
        memtable.push(trace_id, span_pointer.clone());

        assert_eq!(memtable.traces.len(), 1);

        let actual_entry = memtable.traces.get(trace_id);
        assert!(actual_entry.is_some());
        let actual_span_vector = actual_entry.unwrap();
        let pointer = actual_span_vector.get(0).unwrap();
        assert_eq!(pointer.segment_id, span_pointer.segment_id);
        assert_eq!(pointer.offset, span_pointer.offset);
        assert_eq!(pointer.length, span_pointer.length);
        assert_eq!(pointer.last_update, span_pointer.last_update);
    }

    #[test]
    fn push_batch_ref() {
        let trace_id: &TraceId = b"5af7183fb1d4cf5f";
        let batch: &[SpanPointer] = &[
            SpanPointer {
                segment_id: 1,
                offset: 100,
                length: 64,
                last_update: 5000,
            },
            SpanPointer {
                segment_id: 2,
                offset: 64,
                length: 64,
                last_update: 5000,
            },
        ];
        let mut memtable = Memtable::new(10);
        memtable.push_batch_ref(trace_id, batch);

        assert_eq!(memtable.traces.len(), 1);

        let actual_entry = memtable.traces.get(trace_id);
        assert!(actual_entry.is_some());

        let actual_span_vector = actual_entry.unwrap();
        assert_eq!(actual_span_vector.len(), batch.len());

        for i in 0..actual_span_vector.len() {
            let pointer = actual_span_vector.get(i).unwrap();
            assert_eq!(pointer.segment_id, batch[i].segment_id);
            assert_eq!(pointer.offset, batch[i].offset);
            assert_eq!(pointer.length, batch[i].length);
            assert_eq!(pointer.last_update, batch[i].last_update);
        }
    }

    #[test]
    fn get_index() {
        let mut memtable = Memtable::new(10);
        let trace_ids: [&TraceId; 2] = [b"5af7183fb1d4cf5f", b"6b221d5bc9e6496c"];
        let batch1: &[SpanPointer] = &[
            SpanPointer {
                segment_id: 1,
                offset: 100,
                length: 64,
                last_update: 5000,
            },
            SpanPointer {
                segment_id: 2,
                offset: 64,
                length: 64,
                last_update: 5000,
            },
        ];

        let batch2: &[SpanPointer] = &[
            SpanPointer {
                segment_id: 1,
                offset: 200,
                length: 64,
                last_update: 5000,
            },
            SpanPointer {
                segment_id: 2,
                offset: 255,
                length: 64,
                last_update: 5000,
            },
        ];

        memtable.push_batch_ref(trace_ids[0], batch1);
        memtable.push_batch_ref(trace_ids[1], batch2);
        assert_eq!(memtable.traces.len(), 2);

        let actual_entry_for_trace_1 = memtable.get_index(trace_ids[0]);
        assert_span_pointer(actual_entry_for_trace_1.unwrap(), batch1);

        let actual_entry_for_trace_2 = memtable.get_index(trace_ids[1]);
        assert_span_pointer(actual_entry_for_trace_2.unwrap(), batch2);
    }

    fn assert_span_pointer(pointers: &Vec<SpanPointer>, expected_batch: &[SpanPointer]) {
        pointers.iter()
            .enumerate()
            .for_each(|(index, sp)| {
                assert_eq!(sp.segment_id, expected_batch[index].segment_id);
                assert_eq!(sp.offset, expected_batch[index].offset);
                assert_eq!(sp.length, expected_batch[index].length);
                assert_eq!(sp.last_update, expected_batch[index].last_update);
            });
    }

    mod trace_eviction {
        use super::*;

        fn ptr(segment_id: u64) -> SpanPointer {
            SpanPointer {
                segment_id,
                offset: 100,
                length: 64,
                last_update: 5000,
            }
        }

        fn tid(id: [u8; 16]) -> TraceId {
            id
        }

        #[test]
        fn evicts_oldest_when_full() {
            let mut m = Memtable::new(2);
            m.push(&tid(*b"5af7183fb1d4cf5f"), ptr(1));
            m.push(&tid(*b"5af7183fb1d4cf5a"), ptr(2));
            m.push(&tid(*b"5af7183fb1d4cf5b"), ptr(3)); // should evict tid(1)

            assert_eq!(m.len(), 2);
            assert!(m.get_index(&tid(*b"5af7183fb1d4cf5f")).is_none(), "tid(1) should have been evicted");
            assert!(m.get_index(&tid(*b"5af7183fb1d4cf5a")).is_some());
            assert!(m.get_index(&tid(*b"5af7183fb1d4cf5b")).is_some());
        }

        #[test]
        fn access_promotes_to_most_recent() {
            let trace_1: TraceId = tid(*b"5af7183fb1d4cf5f");
            let trace_2: TraceId = tid(*b"5af7183fb1d4cf5a");
            let trace_3: TraceId = tid(*b"5af7183fb1d4cf5b");
            let mut m = Memtable::new(2);
            m.push(&trace_1, ptr(1));
            m.push(&trace_2, ptr(2));
            // re-touch tid(1) so tid(2) becomes the oldest
            m.push(&trace_1, ptr(11));
            m.push(&trace_3, ptr(3)); // should evict tid(2)

            assert!(m.get_index(&trace_2).is_none(), "tid(2) should have been evicted");
            assert!(m.get_index(&trace_1).is_some());
            assert!(m.get_index(&trace_3).is_some());
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
