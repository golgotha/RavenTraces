#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;
    use common::binary_readers::{read_n_bytes, read_u32};
    use storage::block_storage::{BlockStorage, LocalBlockStorage};
    use storage::flush_worker::{DiskFlushWorker, FlushWorker};
    use storage::memtable::Memtable;
    use storage::span::{AttributeValue, Span, SpanId, SpanKind, TraceId};
    use storage::sstable_writer::SStableWriterImpl;
    use storage::types::MemtableConfig;
    use wal::wal::WAL;
    
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

        let dir_path = Path::new(dir.path());
        let mut wal = WAL::open(dir_path).expect("could not open traces.wal");

        let config = MemtableConfig {
            max_size_bytes: 1024,
            initial_capacity: 2,
        };
        let mut memtable = Memtable::new(config);

        let trace_id_1 = TraceId::from_str("69d42ce0d381bd1be42e50d0571cc5bf").unwrap();
        let trace_id_2 = TraceId::from_str("69d42ce0d381bd1be42e50d0571cc5ba").unwrap();
        let span_id_1 = SpanId::from_str("69d42ce0d381bd1a").unwrap();
        let span_id_2 = SpanId::from_str("69d42ce0d381bd1b").unwrap();
        let span_id_3 = SpanId::from_str("69d42ce0d381bd1c").unwrap();
        let span_1 = make_span(trace_id_1, span_id_1);
        let span_2 = make_span(trace_id_1, span_id_2);
        let span_3 = make_span(trace_id_2, span_id_3);

        memtable.insert(&trace_id_1, &span_1, 1);
        // memtable.insert(&trace_id_1, span_2, 1);
        // memtable.insert(&trace_id_2, span_3, 1);

        let table_writer = SStableWriterImpl::new(dir_path.to_path_buf());
        let mut flusher = DiskFlushWorker::new(table_writer, 64 * 1024);
        let flusher_result = flusher.flush(&mut wal, &mut memtable);
        assert!(flusher_result.is_ok());

        let storage = LocalBlockStorage::new(dir_path);
        let blocks = storage.list_blocks();
        assert!(blocks.is_ok());

        let blocks_list = blocks.unwrap();
        let block_id = blocks_list.get(0).unwrap();
        let block_index = storage.read_block_index(&block_id).unwrap();

        let index_entries = block_index.entries();
        assert_eq!(index_entries.len(), 1);

        for entry in index_entries.values() {
            let offset = entry.offset();
            let length = entry.length();
            let block_result = storage.read_block_at(&block_id, offset, length);
            assert!(block_result.is_ok());
            let block_data = block_result.unwrap();
            let spans: Vec<Span> = read_block_entries(&block_data);
            assert_eq!(spans.len(), 1);
        }

        // assert_checkpoint();
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
            local_service: None,
            remote_service: None,
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
}