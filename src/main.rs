mod api;
pub mod distributor;
mod greeting;
pub mod ingester;
pub mod querier;
pub mod settings;

use crate::api::rest;
use crate::distributor::distributor::Distributor;
use crate::greeting::welcome;
use crate::ingester::local_ingester::LocalIngester;
use crate::querier::querier::Querier;
use crate::settings::Settings;
use flexi_logger::{Duplicate, FileSpec, Logger};
use log::{SetLoggerError, info, warn};
use querier::trace_querier::TraceQuerier;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use storage::corvus_engine::CorvusEngineImpl;
use storage::corvus_engine::{CorvusEngine, CorvusEngineConfig};
use storage::memtable::Memtable;
use storage::types::MemtableConfig;

fn main() {
    let settings = &Settings::default();
    let data_dir = &settings.data_dir;
    setup_logging(&settings);
    welcome(&settings);

    let mem_table_config = settings.storage_config.mem_table.clone();
    let corvus_engine_config = CorvusEngineConfig { mem_table_config };

    let mem_table = Memtable::new(corvus_engine_config.mem_table_config.clone());
    let mem_table = Arc::new(RwLock::new(mem_table));
    let blocks_path = Path::new(data_dir.as_str()).to_path_buf();

    let corvus_engine = CorvusEngineImpl::new(
        Path::new(data_dir.as_str()).to_path_buf(),
        Arc::clone(&mem_table),
        settings.storage_config.clone(),
    );
    let corvus_engine_arc = Arc::new(Mutex::new(corvus_engine));
    {
        let mut engine = corvus_engine_arc.lock().unwrap();
        engine.start();
    }

    let mut trace_querier = TraceQuerier::new(blocks_path, mem_table);

    if let Err(e) = trace_querier.load_blocks_index() {
        warn!("failed to load blocks index: {:?}", e);
    }

    let ingester = LocalIngester::new(corvus_engine_arc);

    let distributor = Distributor::new(ingester);
    let querier = Querier::new(trace_querier);
    let distributor_mutex = Mutex::new(distributor);
    let querier_mutex = Mutex::new(querier);

    rest::init(&settings, distributor_mutex, querier_mutex).unwrap();
}

fn setup_logging(settings: &Settings) {
    Logger::try_with_env_or_str(settings.log_level.as_str())
        .unwrap()
        .log_to_file(
            FileSpec::default()
                .directory(settings.log_dir.as_str())
                .basename("raven-traces"),
        )
        .duplicate_to_stdout(Duplicate::All)
        .start()
        .unwrap();
}
