use std::collections::HashSet;
use std::{fs, io};
use std::path::PathBuf;
use std::sync::RwLock;
use crate::errors::StorageError;

pub struct ServiceNameIndex {
    reader: Box<dyn ServiceNameIndexReader>,
    writer: Box<dyn ServiceNameIndexWriter>,
    services: RwLock<HashSet<String>>,
}

impl ServiceNameIndex {

    pub fn new(reader: Box<dyn ServiceNameIndexReader>, writer: Box<dyn ServiceNameIndexWriter>) -> Self {
        Self {
            reader,
            writer,
            services: RwLock::new(HashSet::new()),
        }
    }

    pub fn load_or_create(&self) -> Result<(), StorageError> {
        let service_list = self.reader.read_services()?;
        let mut services = self.services.write().unwrap();

        for service in service_list {
            services.insert(service);
        }

        Ok(())
    }

    pub fn add(&self, service_name: &str) {
        let mut services = self.services.write().unwrap();
        services.insert(service_name.to_string());
    }

    pub fn add_many<I>(&self, names: I)
    where
        I: IntoIterator<Item = String>,
    {
        let mut services = self.services.write().unwrap();

        for name in names {
            services.insert(name);
        }
    }

    pub fn list(&self) -> Vec<String> {
        let services = self.services.read().unwrap();
        let mut result: Vec<String> = services.iter().cloned().collect();
        result.sort();

        result
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        let services = self.list();
        self.writer.write_services(services)
    }
}

pub trait ServiceNameIndexReader: Send + Sync {
    fn read_services(&self) -> Result<HashSet<String>, StorageError>;
}

pub trait ServiceNameIndexWriter: Send + Sync {
    fn write_services(&self, services: Vec<String>) -> Result<(), StorageError>;
}

pub struct LocalServiceNameIndexReader {
    path: PathBuf,
}

impl LocalServiceNameIndexReader {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
        }
    }
}

impl ServiceNameIndexReader for LocalServiceNameIndexReader {
    fn read_services(&self) -> Result<HashSet<String>, StorageError> {
        let file_path = self.path.join("services.json");

        let services = if file_path.exists() {
            let bytes = fs::read(&file_path)?;

            if bytes.is_empty() {
                HashSet::new()
            } else {
                serde_json::from_slice::<HashSet<String>>(&bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            }
        } else {
            HashSet::new()
        };

        Ok(services)
    }
}

pub struct LocalServiceNameIndexWriter {
    path: PathBuf,
}

impl LocalServiceNameIndexWriter {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
        }
    }
}

impl ServiceNameIndexWriter for LocalServiceNameIndexWriter {
    fn write_services(&self, services: Vec<String>) -> Result<(), StorageError> {
        let json = serde_json::to_vec(&services)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let tmp_path = self.path.join("services.json");

        fs::write(&tmp_path, json)?;

        Ok(())
    }
}