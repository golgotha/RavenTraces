use crate::bloom::bloom_filter::{BloomFilterImpl};
use crate::span::TraceId;
use common::binary_readers::{read_bytes, read_n_bytes, read_u16, read_u32, read_u64, read_u8};
use common::serialization::{Readable, Writable};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use uuid::Uuid;

const DEFAULT_VERSION: u8 = 1;

const BLOOM_FILTER_VERSION: u8 = 1;
const BLOOM_FILTER_MAGIC: &[u8; 4] = b"RTBF";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BlockId {
    pub id: String,
}

#[derive(Debug)]
pub struct DataBlock {
    id: BlockId,
    entries: Vec<BlockEntry>,
    block_meta: BlockMeta,
}

#[derive(Debug)]
pub struct BlockEntry {
    size: u32,
    payload: Arc<[u8]>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BlockMeta {
    pub id: BlockId,
    #[serde(rename = "totalSpans")]
    total_spans: u32,
    start_ts: u64,
    end_ts: u64,
    version: u8,
    /// Block size. The actual block size might be greater than the max_block_size
    /// to keep spans within the trace in the same block
    #[serde(skip)]
    block_size: usize,
    /// Maximum the size of the block in bytes
    #[serde(skip)]
    max_block_size: usize,
    #[serde(skip)]
    open: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageMeta {
    #[serde(rename = "blocks")]
    pub blocks: Vec<String>,
}

#[derive(Debug, Default)]
pub struct BlockIdBuilder {
    trace_id: Option<TraceId>,
    start_ts: Option<u64>,
    end_ts: Option<u64>,
}

#[derive(Debug, Default)]
pub struct DataBlockBuilder {
    id: Option<BlockId>,
    entries: Vec<BlockEntry>,
    block_meta: Option<BlockMeta>,
}

#[derive(Debug)]
pub struct BloomFilterHeader {
    magic: [u8; 4],
    version: u8,
    num_hashes: u8,
    reserved: u16,
    num_bits: u64,
    vector_size: u64,
}

#[derive(Debug)]
pub struct BloomFilterBlock {
    header: BloomFilterHeader,
    filter: BloomFilterImpl,
}

impl BloomFilterBlock {

    pub fn from_bloom_filter(bloom_filter: BloomFilterImpl) -> Self {
        let header = BloomFilterHeader {
            magic: *BLOOM_FILTER_MAGIC,
            version: BLOOM_FILTER_VERSION,
            num_hashes: bloom_filter.get_num_hashes(),
            reserved: 0,
            num_bits: bloom_filter.get_num_bits() as u64,
            vector_size: bloom_filter.vec_size() as u64,
        };
        Self {
            header,
            filter: bloom_filter,
        }
    }

    pub fn get_filter(&self) -> BloomFilterImpl {
        self.filter.clone()
    }
}

impl DataBlockBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn block_id(mut self, id: BlockId) -> Self {
        self.id = Some(id);
        self
    }

    pub fn block_meta(mut self, meta: BlockMeta) -> Self {
        self.block_meta = Some(meta);
        self
    }

    pub fn add_entry(mut self, entry: BlockEntry) -> Self {
        self.entries.push(entry);
        self
    }

    pub fn build(self) -> DataBlock {
        let block_id = self.id.unwrap();
        let entries = self.entries;
        let block_meta = self.block_meta.unwrap_or(BlockMeta::new(2));

        DataBlock {
            id: block_id,
            entries,
            block_meta,
        }
    }
}

impl BlockIdBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn trace_id(mut self, trace_id: TraceId) -> Self {
        self.trace_id = Some(trace_id);
        self
    }

    pub fn start_ts(mut self, start_ts: u64) -> Self {
        self.start_ts = Some(start_ts);
        self
    }

    pub fn end_ts(mut self, end_ts: u64) -> Self {
        self.end_ts = Some(end_ts);
        self
    }

    pub fn build(self) -> Result<BlockId, String> {
        let trace = self.trace_id.unwrap().to_hex();
        let start = self.start_ts.unwrap_or(0);
        let end = self.end_ts.unwrap_or(0);

        if let (Some(s), Some(e)) = (self.start_ts, self.end_ts) {
            if e < s {
                return Err(format!("end_ts ({e}) is before start_ts ({s})"));
            }
        }

        let trace_hash = fnv1a32(&trace);
        let duration_ms = end.saturating_sub(start);
        let duration_hex = (duration_ms.min(u32::MAX as u64)) as u32;

        // Layout:  [16 hex: ts_start][8 hex: trace_hash][16 hex: ts_end][8 hex: duration]
        let body = format!("{start:016x}{trace_hash:08x}{end:016x}{duration_hex:08x}");

        Ok(BlockId {
            id: format!("{body}"),
        })
    }
}

impl BlockMeta {
    pub fn new(max_block_size: usize) -> Self {
        Self {
            id: BlockId::uuid(),
            total_spans: 0,
            start_ts: 0,
            end_ts: 0,
            version: DEFAULT_VERSION,
            block_size: 0,
            max_block_size,
            open: false,
        }
    }

    pub fn is_full(&self) -> bool {
        self.block_size > self.max_block_size
    }

    pub fn update_size(&mut self, size: usize) {
        self.block_size += size;
    }

    pub fn is_open(&self) -> bool {
        self.open
    }

    pub fn set_start_ts(&mut self, start_ts: u64) {
        self.start_ts = start_ts;
    }

    pub fn set_end_ts(&mut self, end_ts: u64) {
        self.end_ts = end_ts;
    }

    pub fn get_start_ts(&self) -> u64 {
        self.start_ts
    }

    pub fn get_end_ts(&self) -> u64 {
        self.end_ts
    }

    fn increment_spans(&mut self) {
        self.total_spans += 1
    }
}

impl BlockId {
    pub fn new(id: String) -> Self {
        Self { id }
    }

    pub fn uuid() -> Self {
        Self::new(Uuid::now_v7().to_string())
    }
}

impl DataBlock {
    pub fn new(max_block_size: usize) -> Self {
        let block_meta = BlockMeta::new(max_block_size);
        DataBlock {
            id: block_meta.id.clone(),
            entries: Vec::new(),
            block_meta,
        }
    }

    pub fn add_span(&mut self, span: &[u8]) {
        let payload_size = span.len();
        // payload size +  the size of the size u32
        // you can not to use size_of_val to evaluate the size
        // because the payload_size is usize which is platform dependent type
        let entry_size = payload_size + 4;

        self.block_meta.update_size(entry_size);
        self.block_meta.increment_spans();

        let entry = BlockEntry {
            size: payload_size as u32,
            payload: Arc::from(span),
        };
        self.entries.push(entry);
    }

    pub fn id(&self) -> &BlockId {
        &self.id
    }

    pub fn spans(&self) -> &Vec<BlockEntry> {
        &self.entries
    }

    pub fn is_full(&self) -> bool {
        self.block_meta.is_full()
    }

    pub fn block_size(&self) -> usize {
        self.block_meta.block_size
    }

    pub fn get_block_meta(&mut self) -> &mut BlockMeta {
        &mut self.block_meta
    }
}

impl BlockEntry {
    pub fn new(size: u32, payload: Arc<[u8]>) -> Self {
        Self { size, payload }
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn size(&self) -> u32 {
        self.size
    }
}

impl Writable for BlockEntry {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(&self.size.to_le_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }
}

impl Readable for BlockEntry {
    fn deserialize(buffer: &[u8]) -> Result<Self, String>
    where
        Self: Sized,
    {
        let mut offset = 0;
        let size = read_u32(buffer, &mut offset)?;
        let payload = &buffer[offset..];

        Ok(BlockEntry {
            size,
            payload: Arc::from(payload),
        })
    }
}

impl Writable for BloomFilterBlock {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(&self.header.magic);
        buf.push(self.header.version);
        buf.extend(&self.header.num_hashes.to_le_bytes());
        buf.extend(&self.header.reserved.to_le_bytes());
        buf.extend(&self.header.num_bits.to_le_bytes());
        buf.extend(&self.header.vector_size.to_le_bytes());

        let vector = self.filter.serialize();
        buf.extend(&vector);
        buf
    }
}

impl Readable for BloomFilterBlock {

    fn deserialize(buffer: &[u8]) -> Result<Self, String>
    where
        Self: Sized
    {
        let mut offset = 0;
        let magic = read_bytes::<4>(&buffer, &mut offset);
        let version = read_u8(&buffer, &mut offset)?;
        let num_hashes = read_u8(&buffer, &mut offset)?;
        let reserved = read_u16(&buffer, &mut offset)?;
        let num_bits = read_u64(&buffer, &mut offset)?;
        let vector_size = read_u64(&buffer, &mut offset)?;
        let bloom_filter_vector = read_n_bytes(&buffer, &mut offset, vector_size as usize);

        let bloom_filter = BloomFilterBlock {
            header: BloomFilterHeader {
                magic,
                version,
                num_hashes,
                reserved,
                num_bits,
                vector_size,
            },
            filter: BloomFilterImpl::from_parts(num_bits, num_hashes, bloom_filter_vector),
        };

        Ok(bloom_filter)
    }
}

impl Display for DataBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.id.fmt(f)
    }
}

impl Display for BlockId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.id.fmt(f)
    }
}

fn fnv1a32(s: &str) -> u32 {
    let mut hash: u32 = 2_166_136_261;
    for byte in s.bytes() {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16_777_619);
    }
    hash
}

fn normalise_ts(ts: u64) -> u64 {
    if ts <= 1_000_000_000_000 {
        ts * 1_000
    } else {
        ts
    }
}
