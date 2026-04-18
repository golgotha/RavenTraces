use storage::errors::StorageError;
use storage::span::Span;

pub trait Ingester {

    fn ingest(&mut self, spans: &Vec<Span>) -> Result<(), StorageError>;

}