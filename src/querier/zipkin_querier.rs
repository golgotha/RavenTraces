use std::collections::HashMap;
use storage::span::{AttributeValue, Span, SpanEvent, TraceId};
use crate::api::zipkin::json_model::{ZipkinAnnotation, ZipkinEndpoint, ZipkinKind, ZipkinSpan};
use crate::querier::model::SearchRequest;
use crate::querier::querier::{Querier, QuerierError};
use crate::querier::trace_querier::TraceQuerier;

pub struct ZipkinQuerier {
    trace_querier: TraceQuerier,
}

impl ZipkinQuerier {
    pub fn new(trace_querier: TraceQuerier) -> Self {
        ZipkinQuerier{ trace_querier }
    }
}

impl Querier<ZipkinSpan> for ZipkinQuerier {

    fn search_by_trace_id(&self, trace_id: impl AsRef<str>) -> Result<Vec<ZipkinSpan>, QuerierError> {
        let trace_id_str = trace_id.as_ref();
        let trace_id = TraceId::from_str(&trace_id_str).map_err(|_| QuerierError::InvalidTraceId(trace_id_str.to_string()))?;

        let spans = self.trace_querier.get_trace(&trace_id)
            .into_iter()
            .map(convert_span_to_zipkin)
            .collect();

        Ok(spans)
    }

    fn search_spans_between(&self, start_ts: u64, end_ts: u64) -> Result<Vec<ZipkinSpan>, QuerierError> {
        let spans = self.trace_querier.query_by_time(start_ts, end_ts)
            .map_err(|e| QuerierError::SpansNotFound(format!("trace query time not found, {}", e)))?
            .iter()
            .map(|span| convert_span_to_zipkin(span.clone()))
            .collect();
        Ok(spans)
    }

    fn search_traces(&self, search_request: &SearchRequest) -> Result<Vec<ZipkinSpan>, QuerierError> {
        let spans = self.trace_querier.search(&search_request)
            .unwrap_or_default()
            .into_iter()
            .map(convert_span_to_zipkin)
            .collect::<Vec<ZipkinSpan>>();

        Ok(spans)
    }

    fn get_services(&self) -> Result<Vec<String>, QuerierError> {
        let services = self.trace_querier.get_services();
        Ok(services)
    }
}

fn convert_span_to_zipkin(span: Span) -> ZipkinSpan {
    let parent_span_id = span.parent_span_id.map(|id| id.to_hex());
    let tags: HashMap<String, String> = span
        .attributes
        .iter()
        .map(|(k, v)| {
            let value_str = match v {
                AttributeValue::String(s) => s.clone(), // clone the string
                AttributeValue::Int(i) => i.to_string(),
                AttributeValue::Float(f) => f.to_string(),
                AttributeValue::Bool(b) => b.to_string(),
                // AttributeValue::StringArray(v) => ???  handle arrays if needed
                // AttributeValue::IntArray(v) => ???
                // AttributeValue::FloatArray(v) => ???
                // AttributeValue::BoolArray(v) => ???
                _ => "unknown".to_string(),
            };
            (k.clone(), value_str)
        })
        .collect();

    let zipkin_kind = match span.kind {
        storage::span::SpanKind::Internal => Some(ZipkinKind::Client),
        storage::span::SpanKind::Client => Some(ZipkinKind::Client),
        storage::span::SpanKind::Server => Some(ZipkinKind::Server),
        storage::span::SpanKind::Producer => Some(ZipkinKind::Producer),
        storage::span::SpanKind::Consumer => Some(ZipkinKind::Consumer),
    };

    let local_endpoint = span.local_service.map(|endpoint| ZipkinEndpoint {
        service_name: Some(endpoint),
        ipv4: None,
        ipv6: None,
        port: None,
    });

    let annotations = span.events
        .into_iter()
        .map(convert_event_to_annotation)
        .collect();

    ZipkinSpan {
        id: span.span_id.to_hex(),
        trace_id: span.trace_id.to_hex(),
        parent_id: parent_span_id,
        name: span.name,
        timestamp: span.timestamp,
        duration: span.duration,
        kind: zipkin_kind,
        local_endpoint,
        remote_endpoint: None,
        tags: Some(tags),
        annotations: Some(annotations),
    }
}

fn convert_event_to_annotation(event: SpanEvent) -> ZipkinAnnotation {
    let timestamp = event.timestamp / 1000; // nanos -> to micros
    let name = event.name;
    ZipkinAnnotation {
        timestamp,
        value: name,
    }
}