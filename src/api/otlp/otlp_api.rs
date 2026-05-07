use std::collections::HashMap;
use actix_web::{HttpResponse, post, web, Result};
use opentelemetry_proto::tonic::collector::trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse};
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::trace::v1::Status;
use prost::Message;
use storage::span::{AttributeValue, Span, SpanId, SpanKind, TraceId};
use crate::distributor::distributor::Distributor;

#[post("/v1/traces")]
async fn post_otlp_span(
    distributor: web::Data<Distributor>,
    body: web::Bytes,
) -> Result<HttpResponse> {
    let request = ExportTraceServiceRequest::decode(body.as_ref())
        .map_err(actix_web::error::ErrorBadRequest)?;
    let unified_spans_vec = convert_otlp_request(request);
    distributor.deliver(unified_spans_vec);

    let response = ExportTraceServiceResponse {
        partial_success: None,
    };

    let mut buf = Vec::new();
    response.encode(&mut buf)
        .map_err(actix_web::error::ErrorInternalServerError)?;

    Ok(HttpResponse::Ok()
        .content_type("application/x-protobuf")
        .body(buf))
}

fn convert_otlp_request(request: ExportTraceServiceRequest) -> Vec<Span> {
    let mut result = Vec::new();

    for resource_spans in request.resource_spans {
        for scope_spans in resource_spans.scope_spans {
            for span in scope_spans.spans {
                let trace_id = TraceId::from_bytes(&span.trace_id);
                let span_id = SpanId::from_bytes(&span.span_id);

                let parent_span_id = if span.parent_span_id.is_empty() {
                    None
                } else {
                    SpanId::from_bytes(&span.parent_span_id)
                };

                let timestamp = span.start_time_unix_nano;
                let duration = span
                    .end_time_unix_nano
                    .saturating_sub(span.start_time_unix_nano);

                result.push(Span {
                    trace_id: trace_id.unwrap(),
                    span_id: span_id.unwrap(),
                    parent_span_id,
                    name: span.name,
                    kind: map_span_kind(span.kind).unwrap(),
                    timestamp,
                    duration,
                    attributes: convert_attributes(span.attributes),
                    events: vec![],
                    status_code: convert_status_code(span.status.as_ref()),
                    status_message: convert_status_message(span.status.as_ref()),
                });
            }
        }
    }

    result
}


fn convert_attributes(attributes: Vec<KeyValue>) -> HashMap<String, AttributeValue> {
    let mut result = HashMap::with_capacity(attributes.len());

    for kv in attributes {
        let Some(any_value) = kv.value else {
            continue;
        };

        let Some(value) = convert_any_value(any_value) else {
            continue;
        };

        result.insert(kv.key, value);
    }

    result
}

fn convert_any_value(any_value: AnyValue) -> Option<AttributeValue> {
    match any_value.value? {
        Value::StringValue(value) => Some(AttributeValue::String(value)),
        Value::BoolValue(value) => Some(AttributeValue::Bool(value)),
        Value::IntValue(value) => Some(AttributeValue::Int(value)),
        Value::DoubleValue(value) => Some(AttributeValue::Float(value)),

        // Value::ArrayValue(array) => {
        //     let values = array
        //         .values
        //         .into_iter()
        //         .filter_map(convert_any_value)
        //         .collect();
        //
        //     Some(AttributeValue::Array(values))
        // }

        // OTLP also supports KeyValueList and BytesValue.
        // Add these later if your internal model needs them.
        Value::KvlistValue(_) => None,
        Value::BytesValue(_) => None,
        Value::ArrayValue(_) => todo!(),
    }
}

fn convert_status_message(status: Option<&Status>) -> Option<String> {
    status
        .map(|s| s.message.as_str())
        .filter(|message| !message.is_empty())
        .map(String::from)
}

fn convert_status_code(status: Option<&Status>) -> Option<u32> {
    status.map(|s| s.code as u32)
}

fn find_string_attr(attrs: &[KeyValue], key: &str) -> Option<String> {
    attrs.iter().find_map(|kv| {
        (kv.key == key).then(|| {
            kv.value.as_ref().and_then(|val| match &val.value {
                Some(Value::StringValue(s)) => Some(s.clone()),
                _ => None,
            })
        })?
    })
}

fn map_span_kind(kind: i32) -> Result<SpanKind, String> {
    match kind {
        0 => Ok(SpanKind::Unspecified),
        1 => Ok(SpanKind::Internal),
        2 => Ok(SpanKind::Server),
        3 => Ok(SpanKind::Client),
        4 => Ok(SpanKind::Producer),
        5 => Ok(SpanKind::Consumer),
        _ => Err("invalid SpanKind".to_string()),
    }
}