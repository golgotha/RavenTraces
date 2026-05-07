use storage::errors::StorageError;
use storage::span::Span;

pub trait Ingester {

    fn ingest(&self, spans: Vec<Span>) -> Result<(), StorageError>;

}