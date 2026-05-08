use crate::span::TraceId;

pub struct SearchRequest {
    pub trace_id: Option<TraceId>,
    pub service_name: Option<String>,
    pub span_name: Option<String>,
    pub limit: Option<usize>,
    pub end_ts: Option<u64>,
    pub lookback: Option<u64>,
}

impl SearchRequest {
    pub fn for_trace_id(trace_id: &TraceId) -> SearchRequest {
        SearchRequest {
            trace_id: Some(trace_id.clone()),
            service_name: None,
            span_name: None,
            limit: None,
            end_ts: None,
            lookback: None,
        }
    }

    pub fn for_service_name(service_name: &String) -> SearchRequest {
        SearchRequest {
            trace_id: None,
            service_name: Some(service_name.clone()),
            span_name: None,
            limit: None,
            end_ts: None,
            lookback: None,
        }
    }
}