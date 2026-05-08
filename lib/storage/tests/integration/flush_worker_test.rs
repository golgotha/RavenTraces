#[cfg(test)]
mod tests {
    use common::binary_readers::{read_n_bytes, read_u32};
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use storage::block_storage::{BlockStorage, LocalBlockStorage};
    use storage::bloom::bloom_filter::BloomFilter;
    use storage::flush_worker::{DiskFlushWorker, FlushWorker};
    use storage::index::service_name_index::ServiceNameIndex;
    use storage::memtable::Memtable;
    use storage::span::{AttributeValue, Span, SpanId, SpanKind, TraceId};
    use storage::sstable_writer::SStableWriterImpl;
    use storage::types::MemtableConfig;
    use tempfile::TempDir;

    const TOTAL_TRACES: usize = 100;

    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("failed to create temp dir")
    }

    #[test]
    fn test_flush_memtable() {
        let dir = temp_dir();
        if fs::exists(&dir).unwrap() {
            fs::remove_dir_all(&dir)
                .expect("Can not remove test dir");
        }

        println!("Test dir: {}", dir.path().display());
        println!("{}", generate_span_id());
        println!("{}", generate_trace_id());

        let dir_path = Path::new(dir.path());

        let config = MemtableConfig {
            max_size_bytes: 1024,
            max_age_seconds: 10,
            initial_capacity: 2,
        };

        let mut memtable = Memtable::new(config, 1);
        let mut traces: Vec<TraceId> = Vec::new();

        for _ in 0..TOTAL_TRACES {
            let trace_id  = TraceId::from_str(generate_trace_id().as_str()).unwrap();
            let span_id = SpanId::from_str(generate_span_id().as_str()).unwrap();
            let span = make_span(trace_id, span_id);
            memtable.insert(&trace_id, span);
            traces.push(trace_id);
        }

        let table_writer = SStableWriterImpl::new(dir_path.to_path_buf());
        let service_name_index =  Arc::new(ServiceNameIndex::load_or_create(&dir_path).unwrap());
        let mut flusher = DiskFlushWorker::new(table_writer, service_name_index, 64 * 1024);
        let flusher_result = flusher.flush(memtable);
        assert!(flusher_result.is_ok());

        let storage = LocalBlockStorage::new(dir_path);
        let blocks = storage.list_blocks();
        assert!(blocks.is_ok());

        let blocks_list = blocks.unwrap();
        let block_id = blocks_list.get(0).unwrap();
        let block_index = storage.read_block_index(&block_id).unwrap();

        let bloom_filter_block_res = storage.read_bloom_filter(&block_id);
        assert!(bloom_filter_block_res.is_ok());
        let bloom_filter_block = bloom_filter_block_res.unwrap();
        let bloom_filter = bloom_filter_block.get_filter();
        let actual_prediction = bloom_filter.might_contain(traces.get(0).unwrap());
        assert_eq!(actual_prediction, true, "Trace might be in bloom filter");

        let index_entries = block_index.entries();
        assert_eq!(index_entries.len(), TOTAL_TRACES);

        for entry in index_entries.values() {
            let offset = entry.offset();
            let length = entry.length();
            let block_result = storage.read_block_at(&block_id, offset, length);
            assert!(block_result.is_ok());
            let block_data = block_result.unwrap();
            let spans: Vec<Span> = read_block_entries(&block_data);
            assert!(spans.len() > 0);
        }

        // assert_checkpoint();
    }

    #[test]
    fn generate_ids_for_test() {
        let trace_id = generate_trace_id();
        let span_id = generate_span_id();

        println!("trace_id = {}", trace_id);
        println!("span_id = {}", span_id);

        assert_eq!(trace_id.len(), 32);
        assert_eq!(span_id.len(), 16);
    }

    fn make_span(trace_id: TraceId, span_id: SpanId) -> Span {
        let mut attributes: HashMap<String, AttributeValue> = HashMap::new();
        attributes.insert("test".to_string(), AttributeValue::String("test".to_string()));

        let span = Span {
            trace_id,
            span_id,
            parent_span_id: None,
            name: "test span".to_string(),
            kind: SpanKind::Internal,
            timestamp: 0,
            duration: 0,
            attributes,
            events: vec![],
            status_code: Some(1),
            status_message: Some("test messgae".to_string()),
        };
        span
    }

    fn read_block_entries(data: &Vec<u8>) -> Vec<Span> {
        let mut offset = 0;
        let mut spans = Vec::<Span>::new();
        while offset < data.len() {
            let payload_size = read_u32(data, &mut offset).unwrap();
            let payload = read_n_bytes(data, &mut offset, payload_size as usize);
            let span = Span::deserialize(&payload);
            spans.push(span);
        }

        spans
    }

    fn generate_span_id() -> String {
        let id: u64 = rand::random();
        format!("{:016x}", id)
    }

    fn generate_trace_id() -> String {
        let part_1 = generate_span_id();
        let part_2 = generate_span_id();
        format!("{}{}", part_1, part_2)
    }
}