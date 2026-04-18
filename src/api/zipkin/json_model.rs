use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use storage::span::{AttributeValue, SpanId, SpanKind, TraceId, Span};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ZipkinEndpoint {
    #[serde(rename = "serviceName")]
    pub service_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ipv4: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ipv6: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "UPPERCASE")]
pub enum ZipkinKind {
    Client,
    Server,
    Producer,
    Consumer,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ZipkinSpan {
    pub id: String,
    #[serde(rename = "traceId")]
    pub trace_id: String,
    #[serde(rename = "parentId", skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    pub name: String,
    pub timestamp: u64,
    pub duration: u64,
    pub kind: Option<ZipkinKind>,
    #[serde(rename = "localEndpoint", skip_serializing_if = "Option::is_none")]
    pub local_endpoint: Option<ZipkinEndpoint>,
    #[serde(rename = "remoteEndpoint", skip_serializing_if = "Option::is_none")]
    pub remote_endpoint: Option<ZipkinEndpoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Deserialize)]
pub struct ZipkinTraceQuery {
    #[serde(rename = "serviceName")]
    pub service_name: Option<String>,
    #[serde(rename = "spanName")]
    pub span_name: Option<String>,
    pub limit: Option<usize>,
    #[serde(rename = "endTs")]
    pub end_ts: Option<u64>,
    pub lookback: Option<u64>,
}

#[derive(Deserialize)]
pub struct ZipkinSpansQuery {
    #[serde(rename = "serviceName")]
    pub service_name: String,
}

impl From<&ZipkinSpan> for Span {
    fn from(span: &ZipkinSpan) -> Self {
        let trace_id = TraceId::from_str(&span.trace_id).unwrap();
        let span_id = SpanId::from_str(&span.id).unwrap();
        let parent_span_id = span.parent_id.as_ref().map(|v| SpanId::from_str(&v).unwrap());

        let empty_tags = HashMap::new();
        let tags = span.tags.as_ref().unwrap_or(&empty_tags);

        Self {
            trace_id,
            span_id,
            parent_span_id,
            name: span.name.clone(),
            kind: span.kind.clone().map(convert_kind).unwrap_or(SpanKind::Internal),
            timestamp: span.timestamp,
            duration: span.duration,
            attributes: convert_tags(&tags),
            events: vec![],
            status_code: extract_status_code(&tags),
            status_message: extract_status_message(&tags),
            local_service: span.local_endpoint.clone().and_then(|e| e.service_name),
            remote_service: span.remote_endpoint.clone().and_then(|e| e.service_name),
        }
    }
}

fn convert_kind(kind: ZipkinKind) -> SpanKind {
    match kind {
        ZipkinKind::Client => SpanKind::Client,
        ZipkinKind::Server => SpanKind::Server,
        ZipkinKind::Producer => SpanKind::Producer,
        ZipkinKind::Consumer => SpanKind::Consumer,
    }
}

fn convert_tags(tags: &HashMap<String, String>) -> HashMap<String, AttributeValue> {
    tags.iter()
        .map(|(k, v)| (k.clone(), AttributeValue::String(v.clone())))
        .collect()
}

fn extract_status_code(tags: &HashMap<String, String>) -> Option<u32> {
    tags.get("http.status_code")
        .and_then(|v| v.parse::<u32>().ok())
}

fn extract_status_message(tags: &HashMap<String, String>) -> Option<String> {
    tags.get("error").cloned()
}
