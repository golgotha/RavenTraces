use crate::querier::model::SearchRequest;
use storage::span::{TraceId};

#[derive(Debug, thiserror::Error)]
pub enum QuerierError {
    #[error("missing required field: {0}")]
    MissingField(&'static str),
    #[error("invalid trace id: {0}")]
    InvalidTraceId(String),
    #[error("invalid span id: {0}")]
    InvalidSpanId(String),
    #[error("trace id: {1} not found")]
    TraceNotFound(String, TraceId),
    #[error("spans not found")]
    SpansNotFound(String),
}

pub trait Querier<R>: Send + Sync {
    fn search_by_trace_id(&self, trace_id: impl AsRef<str>) -> Result<Vec<R>, QuerierError>;

    fn search_spans_between(&self, start_ts: u64, end_ts: u64) -> Result<Vec<R>, QuerierError>;

    fn search_traces(&self, search_request: &SearchRequest) -> Result<Vec<R>, QuerierError>;

    fn get_services(&self) -> Result<Vec<String>, QuerierError>;
}
