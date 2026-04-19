use std::collections::{HashMap};
use crate::span::TraceId;
use common::binary_readers::{read_bytes, read_u32, read_u64};
use common::serialization::{Readable, Writable};

#[derive(Debug)]
pub struct BlockIndex {
    total_entries: u32,
    entries: HashMap<TraceId, BlockIndexEntry>,
}

#[derive(Debug)]
pub struct BlockIndexEntry {
    trace_id: TraceId,
    offset: u64,
    length: u32,
}

pub struct TimeIndexEntry {}

impl BlockIndex {
    pub fn new() -> Self {
        Self {
            total_entries: 0,
            entries: HashMap::new(),
        }
    }

    pub fn insert(&mut self, index: BlockIndexEntry) {
        self.entries.insert(index.trace_id, index);
        self.total_entries += 1;
    }

    pub fn entries(&self) -> &HashMap<TraceId, BlockIndexEntry> {
        &self.entries
    }
    
    pub fn find_trace_id(&self, trace_id: &TraceId) -> Option<&BlockIndexEntry> {
        self.entries.get(trace_id)
    }
}

impl BlockIndexEntry {
    pub fn builder() -> BlockIndexEntryBuilder {
        BlockIndexEntryBuilder {
            trace_id: None,
            offset: None,
            length: None,
        }
    }
    
    pub fn trace_id(&self) -> TraceId {
        self.trace_id
    }
    
    pub fn offset(&self) -> u64 {
        self.offset
    }
    
    pub fn length(&self) -> u32 {
        self.length
    }
}

impl Writable for BlockIndexEntry {
    fn serialize(&self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::new();
        buffer.extend_from_slice(self.trace_id.as_bytes());
        buffer.extend(&self.offset.to_le_bytes());
        buffer.extend(&self.length.to_le_bytes());
        buffer
    }
}

impl Readable for BlockIndexEntry {
    fn deserialize(buffer: &[u8]) -> Result<Self, String>
    where
        Self: Sized,
    {
        let mut offset = 0;
        let trace_id = read_bytes::<16>(&buffer, &mut offset);
        let block_offset = read_u64(buffer, &mut offset)?;
        let length = read_u32(buffer, &mut offset)?;

        Ok(Self {
            trace_id: TraceId(trace_id),
            offset: block_offset,
            length,
        })
    }
}

#[derive(Debug, Default)]
pub struct BlockIndexEntryBuilder {
    trace_id: Option<TraceId>,
    offset: Option<u64>,
    length: Option<u32>,
}

impl BlockIndexEntryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn trace_id(mut self, trace_id: TraceId) -> Self {
        self.trace_id = Some(trace_id);
        self
    }

    pub fn offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn length(mut self, length: u32) -> Self {
        self.length = Some(length);
        self
    }

    pub fn build(self) -> Result<BlockIndexEntry, String> {
        let trace_id = self.trace_id.unwrap();
        let offset = self.offset.unwrap_or(0);
        let length = self.length.unwrap_or(0);

        if let Some(length) = self.length
            && length < 0
        {
            return Err("Length must be greater than 0".into());
        }

        Ok(BlockIndexEntry {
            trace_id,
            offset,
            length,
        })
    }
}
