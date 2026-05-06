use log::{error};
use storage::span::{Span};
use crate::ingester::ingester::Ingester;
use crate::ingester::local_ingester::LocalIngester;

pub struct Distributor {
    ingester: LocalIngester,
}

#[derive(Debug, thiserror::Error)]
pub enum DistributorError {
    #[error("missing required field: {0}")]
    MissingField(&'static str),
    #[error("invalid trace id: {0}")]
    InvalidTraceId(String),
    #[error("invalid span id: {0}")]
    InvalidSpanId(String),
}


impl Distributor {

    pub fn new(ingester: LocalIngester) -> Self {
        Distributor {ingester}
    }

    pub fn deliver(&self, spans: Vec<Span>) {

        match self.ingester.ingest(spans) {
            Ok(()) => {},
            Err(e) => error!("Span ingestion error: {}", e)
        }

    }
}