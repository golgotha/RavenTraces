use storage::span::TraceId;

pub struct SearchRequest {
    pub trace_id: Option<TraceId>,
    pub service_name: Option<String>,
    pub span_name: Option<String>,
    pub limit: Option<usize>,
    pub end_ts: Option<u64>,
    pub lookback: Option<u64>,
}
