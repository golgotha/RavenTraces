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
use crate::settings::Settings;
use flexi_logger::{Duplicate, FileSpec, Logger};
use log::{info, warn};
use querier::trace_querier::TraceQuerier;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use clap::Parser;
use storage::corvus_engine::CorvusEngineImpl;
use storage::corvus_engine::{CorvusEngine, CorvusEngineConfig};
use storage::memtable::Memtable;
use crate::querier::zipkin_querier::ZipkinQuerier;
#[cfg(all(
    not(target_env = "msvc"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(
    not(target_env = "msvc"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {

    #[arg(long, value_name = "PATH")]
    config_path: Option<String>,

    #[arg(long, value_name = "PATH")]
    storage_path: Option<String>,
}


fn main() {
    let settings = &Settings::default();
    let data_dir = &settings.data_dir;
    let args = Args::parse();

    setup_logging(&settings);
    welcome(&settings);

    let mem_table_config = settings.storage_config.mem_table.clone();
    let corvus_engine_config = CorvusEngineConfig { mem_table_config };

    let mem_table = Memtable::new(corvus_engine_config.mem_table_config.clone(), 1);
    let mem_table = Arc::new(Mutex::new(mem_table));
    let blocks_path = Path::new(data_dir.as_str()).to_path_buf();
    
    let corvus_engine = CorvusEngineImpl::new(
        Path::new(data_dir.as_str()).to_path_buf(),
        Arc::clone(&mem_table),
        settings.storage_config.clone(),
    );

    let corvus_engine: Arc<dyn CorvusEngine> = Arc::new(corvus_engine);
    corvus_engine.start();

    let mut trace_querier = TraceQuerier::new(blocks_path, Arc::clone(&corvus_engine));

    if let Err(e) = trace_querier.load_blocks_index() {
        warn!("failed to load blocks index: {:?}", e);
    }

    let ingester = LocalIngester::new(corvus_engine);

    let distributor = Distributor::new(ingester);
    let querier = ZipkinQuerier::new(trace_querier);

    rest::init(&settings, distributor, querier).unwrap();
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
