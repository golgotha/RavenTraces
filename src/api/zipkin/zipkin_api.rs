use crate::api::zipkin::json_model::{ZipkinSpan, ZipkinSpansQuery, ZipkinTraceQuery};
use crate::distributor::distributor::Distributor;
use crate::querier::model::SearchRequest;
use crate::querier::querier::Querier;
use crate::querier::zipkin_querier::ZipkinQuerier;
use actix_web::{HttpResponse, get, post, web};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use actix_web::http::StatusCode;
use storage::span::Span;

#[post("/api/v2/spans")]
async fn post_zipkin_span(
    distributor: web::Data<Mutex<Distributor>>,
    spans: web::Json<Vec<ZipkinSpan>>,
) -> HttpResponse {
    let unified_spans_vec = spans.iter().map(Span::from).collect();
    let mut distributor = distributor.lock().unwrap();
    distributor.deliver(&unified_spans_vec);

    HttpResponse::Ok().status(StatusCode::OK).finish()
}

#[get("/api/v2/trace/{id}")]
async fn get_zipkin_trace(
    querier: web::Data<Mutex<ZipkinQuerier>>,
    path: web::Path<String>,
) -> HttpResponse {
    let querier = querier.lock().unwrap();
    let result = querier.search_by_trace_id(path.to_string());

    match result {
        Ok(result) => {
            let response = Some(result);
            HttpResponse::Ok().json(response)
        }
        Err(_) => HttpResponse::NotFound().finish(),
    }
}

#[get("/api/v2/traces")]
async fn get_zipkin_traces(
    querier: web::Data<Mutex<ZipkinQuerier>>,
    query: web::Query<ZipkinTraceQuery>,
) -> HttpResponse {
    let traces_query = query.into_inner();
    let service_name = traces_query.service_name;
    let span_name = traces_query.span_name;
    let limit = traces_query.limit;
    let end_ts = traces_query.end_ts;
    let lookback = traces_query.lookback;
    let querier_lock = querier.lock().unwrap();

    let request = SearchRequest {
        trace_id: None,
        service_name,
        span_name,
        limit,
        end_ts,
        lookback,
    };

    if let Some(end_ts) = end_ts {
        let lookback = lookback.unwrap_or(60000);
        let start_time = end_ts - lookback;
        let traces = querier_lock.search_spans_between(start_time, end_ts);
        match traces {
            Ok(result) => {
                let response = Some(result);
                HttpResponse::Ok().json(response)
            }
            Err(_) => HttpResponse::NotFound().finish(),
        }
    } else {
        match querier_lock.search_traces(&request) {
            Ok(traces) => {
                let mut traces_map: HashMap<String, Vec<ZipkinSpan>> = HashMap::new();
                for span in traces {
                    let trace_id = span.trace_id.clone();
                    traces_map
                        .entry(trace_id)
                        .or_insert_with(Vec::new)
                        .push(span);
                }

                let zipkin_traces: Vec<Vec<ZipkinSpan>> = traces_map.values().cloned().collect();
                HttpResponse::Ok().json(zipkin_traces)
            }
            Err(_) => HttpResponse::NotFound().finish(),
        }
    }
}

#[get("/api/v2/services")]
async fn get_zipkin_services(querier: web::Data<Mutex<ZipkinQuerier>>) -> HttpResponse {
    let result = querier.lock().unwrap().get_services();

    match result {
        Ok(services) => HttpResponse::Ok().json(services),
        Err(_) => HttpResponse::NotFound().finish(),
    }
}

#[get("/api/v2/spans")]
async fn get_zipkin_spans(
    querier: web::Data<Mutex<ZipkinQuerier>>,
    query: web::Query<ZipkinSpansQuery>,
) -> HttpResponse {
    let spans_query = query.into_inner();
    let service_name = spans_query.service_name;

    let request = SearchRequest {
        trace_id: None,
        service_name: Some(service_name),
        span_name: None,
        limit: None,
        end_ts: None,
        lookback: None,
    };

    let zipkin_spans = querier.lock().unwrap().search_traces(&request)
        .unwrap()
        .iter()
        .map(|span| span.name.clone())
        .collect::<HashSet<String>>();

    HttpResponse::Ok().json(zipkin_spans)
}
