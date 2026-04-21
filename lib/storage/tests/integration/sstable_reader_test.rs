
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;
    use common::serialization::Writable;
    use storage::block::{BlockEntry, BlockId, DataBlock};
    use storage::block_storage::{BlockStorage, LocalBlockStorage};
    use storage::span::{AttributeValue, Span, SpanId, SpanKind, TraceId};
    use storage::sstable_reader::{SStableReader, SStableReaderImpl};
    use super::*;

    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("failed to create temp dir")
    }

    fn create_reader(path: &Path) -> SStableReaderImpl {
        let dir_path = Arc::new(Path::new(path));
        let table_reader = SStableReaderImpl::new(dir_path.clone().to_path_buf());
        table_reader
    }

    fn make_span(trace_id: TraceId, span_id: SpanId, name: String) -> Span {
        let mut attributes: HashMap<String, AttributeValue> = HashMap::new();
        attributes.insert("test".to_string(), AttributeValue::String("test".to_string()));

        let span = Span {
            trace_id,
            span_id,
            parent_span_id: None,
            name,
            kind: SpanKind::Internal,
            timestamp: 0,
            duration: 0,
            attributes,
            events: vec![],
            status_code: Some(1),
            status_message: Some("test message".to_string()),
            local_service: None,
            remote_service: None,
        };
        span
    }

    #[test]
    fn test_read_block_slice() {
        let dir = temp_dir();
        if fs::exists(&dir).unwrap() {
            fs::remove_dir_all(&dir)
                .expect("Can not remove test dir");
        }

        let dir_path = Arc::new(Path::new(dir.path()));
        let table_reader = create_reader(dir.path());
        let mut storage = LocalBlockStorage::new(dir_path.clone().to_path_buf());

        let block_id = make_block_id();

        storage.open(&block_id)
            .expect("cannot open block");

        let trace_id = TraceId::from_str("69d42ce0d381bd1be42e50d0571cc5bf").unwrap();
        let span_id = SpanId::from_str("69d42ce0d381bd1b").unwrap();

        let span = make_span(trace_id, span_id, "test span".to_string());

        let payload = span.serialize();
        let payload_size = payload.len();

        let entry = BlockEntry::new(payload_size as u32, Arc::from(payload));
        let entry_bytes = entry.serialize();
        let block_entry_size = entry_bytes.len();

        storage.write_block(&block_id, &entry_bytes)
            .expect("Error occurred while writing a block");

        let data_block = table_reader.read_block_slice(&block_id, 0, block_entry_size as u32)
            .expect("Error occurred while reading a block");

        let spans = data_block.spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans.get(0).unwrap().size(), payload_size as u32);
        let actual_span = Span::deserialize(&spans.get(0).unwrap().payload());
        assert_eq!(actual_span.span_id, span_id);
        assert_eq!(actual_span.trace_id, trace_id);
        assert_eq!(actual_span.parent_span_id, span.parent_span_id);
        assert_eq!(actual_span.name, span.name);
    }

    #[test]
    fn test_read_block() {
        let dir = temp_dir();
        if fs::exists(&dir).unwrap() {
            fs::remove_dir_all(&dir)
                .expect("Can not remove test dir");
        }

        let dir_path = Arc::new(Path::new(dir.path()));
        let table_reader = create_reader(dir.path());
        let mut storage = LocalBlockStorage::new(dir_path.clone().to_path_buf());
        let block_id = make_block_id();

        let mut block_data = DataBlock::new(256);
        let mut expected_spans = Vec::new();
        for index in 0..3 {
            let trace_id = TraceId::from_str(generate_trace_id().as_str())
                .unwrap();
            let span_id = SpanId::from_str(generate_span_id().as_str())
                .unwrap();
            let span= make_span(trace_id, span_id, format!("test span {}", index));

            let payload = span.serialize();
            block_data.add_span(&payload);
            expected_spans.push(span);
        }

        storage.open(&block_id)
            .expect("cannot open block");
        let spans = block_data.spans();
        spans.iter()
            .for_each(|entry| {
                let data = entry.serialize();
                storage.write_block(&block_id, &data)
                    .expect("Error occurred while writing a block");
            });

        let data_block = table_reader.read_block(&block_id)
            .expect("Error occurred while reading a block");
        let block_entries = data_block.spans();
        assert_eq!(block_entries.len(), 3);
        let actual_entry = block_entries.get(1).unwrap();
        let actual_span = Span::deserialize(&actual_entry.payload());

        let expected_span = expected_spans.get(1).unwrap();
        assert_eq!(actual_span.span_id, expected_span.span_id);
        assert_eq!(actual_span.trace_id, expected_span.trace_id);
        assert_eq!(actual_span.parent_span_id, expected_span.parent_span_id);
        assert_eq!(actual_span.name, expected_span.name);
    }

    fn make_block_id() -> BlockId{
        BlockId::new("test_block".to_string())
    }

    fn generate_trace_id() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos() as u64;

        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut state = secs ^ (nanos << 32) ^ (nanos >> 32);
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;

        let part1 = state;

        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;

        let part2 = state;

        format!("{:016x}{:016x}", part1, part2)
    }

    fn generate_span_id() -> String {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap();

        let secs = duration.as_secs();
        let nanos = duration.subsec_nanos() as u64;

        let mut state = secs ^ (nanos << 32) ^ (nanos >> 32);
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;

        format!("{:016x}", state)
    }
}