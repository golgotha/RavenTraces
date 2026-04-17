
#[cfg(test)]
mod tests {
    // use rand::RngExt;

    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;
    use common::binary_readers::{read_n_bytes, read_u32};
    use storage::block::{BlockEntry, BlockId, BlockMeta, DataBlock};
    use storage::block_storage::{BlockStorage, LocalBlockStorage};
    use storage::span::{AttributeValue, SpanId, SpanKind, TraceId, Span};
    use storage::sstable_writer::{SStableWriter, SStableWriterImpl};
    use super::*;

    fn make_block_id() -> BlockId{
        BlockId::new("test_block".to_string())
    }

    #[test]
    fn test_write_block() {
        let dir_path = Arc::new(Path::new("./test_dir"));
        let mut table_writer = SStableWriterImpl::new(dir_path.clone().to_path_buf());
        let mut storage = LocalBlockStorage::new(dir_path.clone().to_path_buf());

        let block_id = make_block_id();
        let trace_id = TraceId::from_str("69d42ce0d381bd1be42e50d0571cc5bf").unwrap();
        let span_id = SpanId::from_str("69d42ce0d381bd1b").unwrap();

        let mut attributes: HashMap<String, AttributeValue> = HashMap::new();
        attributes.insert("test".to_string(), AttributeValue::String("test".to_string()));
        let mut block = DataBlock::new(64 * 1024);

        let span = Span {
            trace_id,
            span_id,
            parent_span_id: None,
            name: "".to_string(),
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

        block.add_span(&span.serialize());
        let n_bytes = table_writer.write_block(&block)
            .unwrap();
        assert!(n_bytes > 0);

        let block_data = storage.read_block(&block_id, 0, n_bytes as u32);
        assert!(block_data.is_ok());
        let block_data = block_data.unwrap();
        assert_eq!(block_data.len(), n_bytes);
        let actual_spans = read_block_entries(&block_data);
        assert_eq!(actual_spans.len(), 1);
    }

    fn read_block_entries(data: &Vec<u8>) -> Vec<Span> {
        let mut offset = 0;
        let mut spans = Vec::<Span>::new();
        while offset < data.len() {
            let payload_size = read_u32(data, &mut offset).unwrap();
            let payload = read_n_bytes(data, &mut offset, payload_size as usize);
            let span = Span::deserialize(payload);
            spans.push(span);
        }

        spans
    }
}