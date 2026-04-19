use crate::api::rest::models::VersionInfo;
use crate::distributor::distributor::Distributor;
use actix_web::middleware::{Compress, Logger, NormalizePath};
use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use actix_web_prom::PrometheusMetricsBuilder;
use log::info;
use std::io;
use std::sync::Mutex;
use crate::settings::Settings;
use crate::api::zipkin::zipkin_api::{post_zipkin_span, get_zipkin_services,
                                     get_zipkin_spans, get_zipkin_trace, get_zipkin_traces};
use crate::querier::zipkin_querier::ZipkinQuerier;

pub mod models;

#[get("/")]
pub async fn index() -> impl Responder {
    HttpResponse::Ok().json(VersionInfo::default())
}

async fn health() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({ "status": "ok" }))
}

pub fn init(
    settings: &Settings,
    distributor: Mutex<Distributor>,
    querier: Mutex<ZipkinQuerier>,
) -> io::Result<()> {
    let host = &settings.service.host;
    let port = &settings.service.http_port;

    actix_web::rt::System::new()
        .block_on(async {
            let distributor = web::Data::new(distributor);
            let querier = web::Data::new(querier);

            let mut server = HttpServer::new(move || {
                let prometheus = PrometheusMetricsBuilder::new("")
                    .endpoint("/metrics")
                    .build()
                    .unwrap();

                let app = App::new()
                    .wrap(Logger::default())
                    .wrap(Compress::default())
                    .wrap(NormalizePath::trim())
                    .wrap(prometheus)
                    .app_data(distributor.clone())
                    .app_data(querier.clone())
                    .service(
                        web::scope("/zipkin")
                            .service(get_zipkin_trace)
                            .service(get_zipkin_services)
                            .service(post_zipkin_span)
                            .service(get_zipkin_traces)
                            .service(get_zipkin_spans)
                    )
                    .route("/health", web::get().to(health))
                    .route("/api/echo", web::get().to(health))
                    .service(index);

                app
            });

            let bind_addr = format!("{}:{}", host, port);
            server = server.bind(bind_addr)?;
            info!("RavenTraces HTTP listening on {}", port);
            server.run().await
        })
        .expect("Failed to start HTTP server");
    Ok(())
}
