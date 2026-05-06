use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::format;
use storage::span::{AttributeValue, Span, SpanEvent, SpanId, SpanKind, TraceId};

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
pub struct ZipkinAnnotation {
    pub timestamp: u64,
    pub value: String,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Vec<ZipkinAnnotation>>,
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

impl From<ZipkinSpan> for Span {
    fn from(span: ZipkinSpan) -> Self {
        let trace_id = TraceId::from_str(&span.trace_id)
            .unwrap_or_else(|_| panic!("Unable to parse trace ID {}", span.trace_id));

        let span_id = SpanId::from_str(&span.id)
            .unwrap_or_else(|_| panic!("Unable to parse span ID: {}", span.id));

        let parent_span_id = span.parent_id.as_ref().map(|v| {
            SpanId::from_str(&v).unwrap_or_else(|_| panic!("Unable to parse parent span ID: {}", v))
        });

        let status_code = extract_status_code(span.tags.as_ref());
        let status_message = extract_status_message(span.tags.as_ref());

        let tags = span.tags;

        let events = span
            .annotations
            .unwrap_or_default()
            .into_iter()
            .map(|a| SpanEvent {
                timestamp: a.timestamp * 1000,
                name: a.value,
                attributes: HashMap::new(),
            })
            .collect();

        let mut attributes = convert_tags(tags);
        convert_local_endpoint_to_attributes(span.local_endpoint, &mut attributes);
        convert_remote_endpoint_to_attributes(span.remote_endpoint, &mut attributes);

        Self {
            trace_id,
            span_id,
            parent_span_id,
            name: span.name,
            kind: span.kind.map(convert_kind).unwrap_or(SpanKind::Internal),
            timestamp: span.timestamp,
            duration: span.duration,
            attributes,
            events,
            status_code,
            status_message,
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

fn convert_tags(tags: Option<HashMap<String, String>>) -> HashMap<String, AttributeValue> {
    match tags {
        Some(tags) => tags
            .into_iter()
            .map(|(k, v)| (k, AttributeValue::String(v)))
            .collect(),
        None => HashMap::new(),
    }
}

fn extract_status_code(tags: Option<&HashMap<String, String>>) -> Option<u32> {
    match tags {
        Some(tags) => tags
            .get("http.status_code")
            .and_then(|v| v.parse::<u32>().ok()),
        None => None,
    }
}

fn extract_status_message(tags: Option<&HashMap<String, String>>) -> Option<String> {
    tags.and_then(|tags| tags.get("error").cloned())
}

fn convert_local_endpoint_to_attributes(
    local_endpoint: Option<ZipkinEndpoint>,
    attributes: &mut HashMap<String, AttributeValue>) {

    let Some(endpoint) = local_endpoint else {
        return;
    };

    if let Some(service_name) = endpoint.service_name {
        attributes.insert(
            "service.name".to_string(),
            AttributeValue::String(service_name),
        );
    }

    if let Some(ipv4) = endpoint.ipv4 {
        attributes.insert("server.address".to_string(), AttributeValue::String(ipv4));
    }

    if let Some(ipv6) = endpoint.ipv6 {
        attributes.insert("server.address".to_string(), AttributeValue::String(ipv6));
    }

    if let Some(port) = endpoint.port {
        attributes.insert("server.port".to_string(), AttributeValue::Int(port as i64));
    }
}

fn convert_remote_endpoint_to_attributes(
    remote_endpoint: Option<ZipkinEndpoint>,
    attributes: &mut HashMap<String, AttributeValue>) {

    let Some(endpoint) = remote_endpoint else {
        return;
    };

    if let Some(service_name) = endpoint.service_name {
        attributes.insert(
            "peer.service".to_string(),
            AttributeValue::String(service_name),
        );
    }

    if let Some(ipv4) = endpoint.ipv4 {
        attributes.insert("client.address".to_string(), AttributeValue::String(ipv4));
    }

    if let Some(ipv6) = endpoint.ipv6 {
        attributes.insert("client.address".to_string(), AttributeValue::String(ipv6));
    }

    if let Some(port) = endpoint.port {
        attributes.insert("client.port".to_string(), AttributeValue::Int(port as i64));
    }
}
