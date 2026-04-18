use crate::api::zipkin::json_model::{
    ZipkinEndpoint, ZipkinKind, ZipkinSpan, ZipkinSpansQuery, ZipkinTraceQuery,
};
use crate::querier::querier::Querier;
use actix_web::{HttpResponse, get, web, post};
use std::collections::HashMap;
use std::sync::Mutex;
use storage::span::{AttributeValue, Span};
use crate::distributor::distributor::Distributor;

#[post("/api/v2/spans")]
async fn post_zipkin_span(
    distributor: web::Data<Mutex<Distributor>>,
    spans: web::Json<Vec<ZipkinSpan>>,
) -> HttpResponse {
    let unified_spans_vec = spans.iter()
        .map(Span::from)
        .collect();
    let mut distributor = distributor.lock().unwrap();
    distributor.deliver(&unified_spans_vec);

    HttpResponse::Ok().finish()
}

#[get("/api/v2/trace/{id}")]
async fn get_zipkin_trace(
    querier: web::Data<Mutex<Querier>>,
    path: web::Path<String>,
) -> HttpResponse {
    let querier = querier.lock().unwrap();
    let result = querier.look_up_trace_id(path.to_string());

    match result {
        Ok(result) => {
            let spans = Some(result);
            let response = convert_to_zipkin(spans.unwrap());
            HttpResponse::Ok().json(response)
        },
        Err(_) => HttpResponse::NotFound().finish(),
    }
}

#[get("/api/v2/traces")]
async fn get_zipkin_traces(
    querier: web::Data<Mutex<Querier>>,
    query: web::Query<ZipkinTraceQuery>,
) -> HttpResponse {
    let traces_query = query.into_inner();
    let service_name = traces_query.service_name;
    let span_name = traces_query.span_name;
    let limit = traces_query.limit;
    let end_ts = traces_query.end_ts;
    let lookback = traces_query.lookback;

    let querier_lock = querier.lock().unwrap();

    if let Some(end_ts) = end_ts {
        let lookback = lookback.unwrap_or(60000);
        let start_time = end_ts - lookback;
        let spans = querier_lock.within_time_range(start_time, end_ts);

        let zipkin_traces: Vec<Vec<ZipkinSpan>> = spans
            .iter()
            .map(|spans| {
                spans
                    .iter()
                    .map(|s| convert_span_to_zipkin((*s).clone()))
                    .collect()
            })
            .collect();
        HttpResponse::Ok().json(zipkin_traces)
    } else {
        let internal_traces: Vec<Span> =
            querier_lock.lookup_traces(service_name, span_name, limit)
                .expect("Internal trace query failed");

        let mut traces_map: HashMap<String, Vec<&Span>> = HashMap::new();
        for span in &internal_traces {
            let trace_id = span.trace_id;
            traces_map
                .entry(trace_id.to_hex().to_string())
                .or_insert_with(Vec::new)
                .push(span);
        }

        // Convert to Zipkin format
        let zipkin_traces: Vec<Vec<ZipkinSpan>> = traces_map
            .values()
            .map(|spans| {
                spans
                    .iter()
                    .map(|s| convert_span_to_zipkin((*s).clone()))
                    .collect()
            })
            .collect();
        HttpResponse::Ok().json(zipkin_traces)
    }
}

#[get("/api/v2/services")]
async fn get_zipkin_services(querier: web::Data<Mutex<Querier>>) -> HttpResponse {
    let result = querier.lock().unwrap().get_services();

    match result {
        Ok(services) => {
            HttpResponse::Ok().json(services)
        },
        Err(_) => HttpResponse::NotFound().finish(),
    }
}

#[get("/api/v2/spans")]
async fn get_zipkin_spans(
    querier: web::Data<Mutex<Querier>>,
    query: web::Query<ZipkinSpansQuery>,
) -> HttpResponse {
    let spans_query = query.into_inner();
    let service_name = spans_query.service_name;
    let internal_traces = querier
        .lock()
        .unwrap()
        .lookup_traces(service_name, None, None)
        .unwrap();

    // Convert to Zipkin format
    let zipkin_spans: Vec<_> = internal_traces
        .iter()
        .map(|span| convert_span_to_zipkin(span.clone()))
        .map(|span| span.name.clone())
        .collect();

    HttpResponse::Ok().json(zipkin_spans)
}

fn convert_to_zipkin(spans: Vec<Span>) -> Vec<ZipkinSpan> {
    spans
        // .sort_by_key(|span| std::cmp::Reverse(span.timestamp))
        .into_iter()
        .map(|span| convert_span_to_zipkin(span))
        .collect()
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
        _ => None,
    };

    let local_endpoint = span.local_service.map(|endpoint| ZipkinEndpoint {
        service_name: Some(endpoint),
        ipv4: None,
        ipv6: None,
        port: None,
    });

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
    }
}
