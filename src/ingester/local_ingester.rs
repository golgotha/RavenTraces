use std::sync::{Arc, Mutex};
use storage::corvus_engine::{CorvusEngine, CorvusEngineImpl};
use storage::errors::StorageError;
use storage::span::Span;
use crate::ingester::ingester::Ingester;

pub struct LocalIngester {
    corvus_engine: Arc<Mutex<CorvusEngineImpl>>,
}

impl LocalIngester {
    pub fn new(corvus_engine: Arc<Mutex<CorvusEngineImpl>>) -> Self {
        Self {
            corvus_engine
        }
    }
}

impl Ingester for LocalIngester {

    fn ingest(&mut self, spans: &Vec<Span>) -> Result<(), StorageError> {
        let mut corvus_engine = self.corvus_engine.lock().unwrap();

        match corvus_engine.append(spans) {
            Ok(()) => Ok(()),
            Err(e) => Err(StorageError::StorageAppendError("Storage append error".to_string())),
        }
    }
}