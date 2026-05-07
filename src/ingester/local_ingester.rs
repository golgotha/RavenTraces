use std::sync::{Arc, Mutex};
use storage::corvus_engine::{CorvusEngine, CorvusEngineImpl};
use storage::errors::StorageError;
use storage::span::Span;
use crate::ingester::ingester::Ingester;

pub struct LocalIngester {
    corvus_engine: Arc<dyn CorvusEngine>,
}

impl LocalIngester {
    pub fn new(corvus_engine: Arc<dyn CorvusEngine>) -> Self {
        Self {
            corvus_engine
        }
    }
}

impl Ingester for LocalIngester {

    fn ingest(&self, spans: Vec<Span>) -> Result<(), StorageError> {
        let result = self.corvus_engine.append(spans);

        match result {
            Ok(()) => Ok(()),
            Err(_) => Err(StorageError::StorageAppendError("Storage append error".to_string())),
        }
    }
}