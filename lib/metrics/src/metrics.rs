use lazy_static::lazy_static;
use prometheus::{register_int_counter, register_int_gauge, Encoder, IntCounter, IntGauge, Registry, TextEncoder};

lazy_static! {
    pub static ref MEMTABLE_ENTRIES: IntGauge = register_int_gauge!(
        "rt_lsm_memtable_entries_total",
        "Current number of entries in the memtable"
    ).unwrap();

    pub static ref MEMTABLE_SIZE_BYTES: IntGauge = register_int_gauge!(
        "rt_lsm_memtable_size_bytes",
        "Current size of the memtable in bytes"
    ).unwrap();

    pub static ref MEMTABLE_WRITES: IntCounter = register_int_counter!(
        "rt_lsm_memtable_writes_total",
        "Total number of write operations to the memtable"
    ).unwrap();


    pub static ref MEMTABLE_READS: IntCounter = register_int_counter!(
        "rt_lsm_memtable_reads_total",
        "Total number of read operations on the memtable"
    ).unwrap();

    pub static ref MEMTABLE_FLUSHES: IntCounter = register_int_counter!(
        "rt_lsm_memtable_flushes_total",
        "Total number of memtable flush operations"
    ).unwrap();

    pub static ref MEMTABLE_FLUSH_DURATION_MS: IntCounter = register_int_counter!(
        "rt_lsm_memtable_flush_duration_total",
        "Total time spent flushing the memtable in milliseconds"
    ).unwrap();
}

pub fn init_metrics() {
    lazy_static::initialize(&MEMTABLE_ENTRIES);
    lazy_static::initialize(&MEMTABLE_SIZE_BYTES);
    lazy_static::initialize(&MEMTABLE_WRITES);
    lazy_static::initialize(&MEMTABLE_READS);
    lazy_static::initialize(&MEMTABLE_FLUSHES);
    lazy_static::initialize(&MEMTABLE_FLUSH_DURATION_MS);
}

pub fn register_metrics(registry: &Registry) {
    let metrics: Vec<Box<dyn prometheus::core::Collector>> = vec![
        Box::new(MEMTABLE_ENTRIES.clone()),
        Box::new(MEMTABLE_SIZE_BYTES.clone()),
        Box::new(MEMTABLE_WRITES.clone()),
        Box::new(MEMTABLE_READS.clone()),
        Box::new(MEMTABLE_FLUSHES.clone()),
        Box::new(MEMTABLE_FLUSH_DURATION_MS.clone()),
    ];

    for metric in metrics {
        registry.register(metric).unwrap();
    }
}

// rt_lsm_memtable_entries_total
// rt_lsm_memtable_size_bytes
// rt_lsm_memtable_writes_total
// rt_lsm_memtable_reads_total
// rt_lsm_memtable_read_hits_total
// rt_lsm_memtable_read_misses_total
// rt_lsm_memtable_flushes_total
// rt_lsm_memtable_flush_duration_milliseconds_total


