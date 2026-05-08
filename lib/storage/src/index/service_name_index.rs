use std::collections::HashSet;
use std::{fs, io};
use std::path::PathBuf;
use std::sync::RwLock;
use crate::errors::StorageError;

pub struct ServiceNameIndex {
    path: PathBuf,
    services: RwLock<HashSet<String>>,
}

impl ServiceNameIndex {
    pub fn load_or_create(path: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let path = path.into();
        let file_path = path.join("services.json");

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

        Ok(Self {
            path,
            services: RwLock::new(services),
        })
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
        let json = serde_json::to_vec(&services)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let tmp_path = self.path.join("services.json");

        fs::write(&tmp_path, json)?;

        Ok(())
    }
}

pub trait ServiceNameIndexReader {
    fn read_services() -> Result<HashSet<String>, StorageError>;
}