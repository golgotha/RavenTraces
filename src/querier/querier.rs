use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use storage::memtable::Memtable;
use storage::span::{TraceId, Span, SpanId};
use crate::querier::model::SearchRequest;
use crate::querier::trace_querier::TraceQuerier;

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

pub struct Querier {
    trace_querier: TraceQuerier,
}

impl Querier {
    
    pub fn new(trace_querier: TraceQuerier, ) -> Querier {
        Querier {
            trace_querier,
        }
    }

    pub fn look_up_trace_id(&self, id: String) -> Result<Vec<Span>, QuerierError> {
        let trace_id = TraceId::from_str(&id).map_err(|_| QuerierError::InvalidTraceId(id))?;

        let spans = self.lookup_trace_ref(&trace_id)
            .ok_or("trace not found")
            .map_err(|arg0: &str| QuerierError::InvalidSpanId(arg0.to_string()))?;
        
        Ok(spans)
    }

    pub fn get_services(&self) -> Result<Vec<String>, QuerierError> {
        let services = self.trace_querier.get_services();
        Ok(services)
    }

    pub fn lookup_traces(
        &self,
        service_name: Option<String>,
        span_name: Option<String>,
        limit: Option<usize>,
    ) -> Result<Vec<Span>, QuerierError> {
        let request = SearchRequest {
            trace_id: None,
            service_name,
            span_name,
            limit
        };

        let spans = self.trace_querier.search(&request)
            .map_err(|e| QuerierError::SpansNotFound(format!("Trace not found, {}", e.to_string())))?;
        
        Ok(spans)
    }

    pub fn within_time_range(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<Span>, QuerierError> {
        let spans = self.trace_querier.query_by_time(start, end)
            .map_err(|e| QuerierError::SpansNotFound(format!("trace query time not found, {}", e)))?;
        Ok(spans)
    }

    fn lookup_trace_ref(&self, trace_id: &TraceId) -> Option<Vec<Span>> {
        self.trace_querier.get_trace(trace_id)
    }
}
