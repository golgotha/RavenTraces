use std::usize;
use log::error;
use storage::block::BlockId;
use storage::errors::StorageError;
use storage::span::Span;
use storage::sstable_reader::SStableReader;
use crate::querier::model::SearchRequest;

pub trait BlockStorageSearch {

    fn search(&self, query: &SearchRequest) -> Result<Vec<Span>, StorageError>;

}

pub struct LocalStorageSearch {
    storage: Box<dyn SStableReader + Send + Sync>,
}

impl LocalStorageSearch {

    pub fn new(storage: Box<dyn SStableReader + Send + Sync>) -> Self {
        Self { storage }
    }
}

impl BlockStorageSearch for LocalStorageSearch {

    fn search(&self, query: &SearchRequest) -> Result<Vec<Span>, StorageError> {
        let storage_meta = match self.storage.read_blocks_meta() {
            Ok(meta) => meta,
            Err(_) => {
                error!("Error occurred while reading storage metadata");
                return Ok(vec![]);
            }
        };

        let span_name = query.span_name.clone();
        let service_name = query.service_name.clone();
        let limit = query.limit.unwrap_or(usize::MAX);

        let mut trace_spans: Vec<Span> = Vec::new();
        for block in storage_meta.blocks {
            let block_id = BlockId::new(block);
            let block_index = self.storage.read_block_index(&block_id)?;

            let block_data = match query.trace_id {
                Some(trace_id) => {
                    let Some(entry) = block_index.find_trace_id(&trace_id) else {
                        continue;
                    };

                    self.storage.read_block_slice(&block_id, entry.offset(), entry.length())?
                },
                None => self.storage.read_block(&block_id)?,
            };

            let block_spans = block_data.spans();
            let spans = block_spans.iter()
                .map(|entry| Span::deserialize(entry.payload().to_vec()))
                .filter(|span| match &service_name {
                    Some(service_name) => span.local_service.as_deref() == Some(service_name.as_str()),
                    None => true,
                })
                .filter(|span| match &span_name {
                    Some(name) => span.name == name.as_str(),
                    None => true,
                })
                .collect::<Vec<Span>>();
            trace_spans.extend(spans);

            if trace_spans.len() >= limit {
                break;
            }
        }

        Ok(trace_spans.iter().take(limit).cloned().collect())
    }
}