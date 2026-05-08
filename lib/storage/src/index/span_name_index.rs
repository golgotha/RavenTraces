use std::collections::{HashMap, HashSet};
use std::{fs, io};
use std::path::PathBuf;
use std::sync::RwLock;
use crate::errors::StorageError;

pub struct Pair {
    pub span_name: String,
    pub service_name: String,
}

pub struct SpanNameIndex {
    reader: Box<dyn SpanNameIndexReader>,
    writer: Box<dyn SpanNameIndexWriter>,
    services: RwLock<HashMap<String, HashSet<String>>>,
}

impl SpanNameIndex {

    pub fn new(reader: Box<dyn SpanNameIndexReader>, writer: Box<dyn SpanNameIndexWriter>) -> Self {
        Self {
            reader,
            writer,
            services: RwLock::new(HashMap::new()),
        }
    }

    pub fn load_or_create(&self) -> Result<(), StorageError> {
        let span_names = self.reader.read()?;
        let mut services = self.services.write().unwrap();

        for (service, spans) in span_names {
            services.entry(service)
                .or_insert_with(HashSet::new)
                .extend(spans);
        }

        Ok(())
    }

    pub fn add(&self, service_span: Pair) {
        let mut services = self.services.write().unwrap();
        services.entry(service_span.service_name)
            .or_insert_with(HashSet::new)
            .insert(service_span.span_name);
    }

    pub fn add_many<I>(&self, pairs: I)
    where
        I: IntoIterator<Item = Pair>,
    {
        let mut services = self.services.write().unwrap();

        for pair in pairs {
            services.entry(pair.service_name)
                .or_insert_with(HashSet::new)
                .insert(pair.span_name);
        }
    }

    pub fn list(&self, service_name: String) -> HashSet<String> {
        let services = self.services.read().unwrap();
        services.get(&service_name)
            .map(|spans| spans.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        let services = self.services.read().unwrap();
        self.writer.write(services.clone())
    }
}

pub trait SpanNameIndexReader: Send + Sync {
    fn read(&self) -> Result<HashMap<String, HashSet<String>>, StorageError>;
}

pub trait SpanNameIndexWriter: Send + Sync {
    fn write(&self, span_names: HashMap<String, HashSet<String>>) -> Result<(), StorageError>;
}

pub struct LocalSpanNameIndexReader {
    path: PathBuf,
}

impl LocalSpanNameIndexReader {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
        }
    }
}

impl SpanNameIndexReader for LocalSpanNameIndexReader {
    fn read(&self) -> Result<HashMap<String, HashSet<String>>, StorageError> {
        let file_path = self.path.join("service_spans.json");

        let spans = if file_path.exists() {
            let bytes = fs::read(&file_path)?;

            if bytes.is_empty() {
                HashMap::new()
            } else {
                serde_json::from_slice::<HashMap<String, HashSet<String>>>(&bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            }
        } else {
            HashMap::new()
        };

        Ok(spans)
    }
}

pub struct LocalSpanNameIndexWriter {
    path: PathBuf,
}

impl LocalSpanNameIndexWriter {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
        }
    }
}

impl SpanNameIndexWriter for LocalSpanNameIndexWriter {
    fn write(&self, span_names: HashMap<String, HashSet<String>>) -> Result<(), StorageError> {
        let json = serde_json::to_string(&span_names)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let tmp_path = self.path.join("service_spans.json");

        fs::write(&tmp_path, json)?;

        Ok(())
    }
}