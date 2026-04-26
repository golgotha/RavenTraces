use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::str::FromStr;
use crate::readers::event_attribute_reader::read_span_events;
use crate::readers::local_service_reader::read_local_service;
use crate::readers::span_attibute_reader::read_attributes;
use common::binary_readers::{read_bytes, read_string, read_u64, read_u8};
use common::serialization::Writable;
use crate::readers::status_code_reader::read_status_code;
use crate::readers::status_message_reader::read_status_message;

#[derive(Debug)]
pub enum TypeError {
    InvalidTraceId(String),
    InvalidSpanId(String),
}

#[derive(Debug, Eq, Hash, PartialEq, Copy, Clone)]
pub struct TraceId(pub [u8; 16]);

#[derive(Debug, Eq, Hash, PartialEq, Copy, Clone)]
pub struct SpanId(pub [u8; 8]);

#[derive(Debug, Clone)]
pub struct Span {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub parent_span_id: Option<SpanId>,
    pub name: String,
    pub kind: SpanKind,
    pub timestamp: u64,
    pub duration: u64,
    pub attributes: HashMap<String, AttributeValue>,
    pub events: Vec<SpanEvent>,
    pub status_code: Option<u32>,
    pub status_message: Option<String>,
    pub local_service: Option<String>,
    pub remote_service: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanKind {
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AttributeValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    StringArray(Vec<String>),
    IntArray(Vec<i64>),
    FloatArray(Vec<f64>),
    BoolArray(Vec<bool>),
}

#[derive(Debug, Clone)]
pub struct SpanEvent {
    pub name: String,
    pub timestamp: u64, // nanoseconds since epoch
    pub attributes: HashMap<String, AttributeValue>,
}

impl Display for SpanKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            SpanKind::Internal => "INTERNAL",
            SpanKind::Server => "SERVER",
            SpanKind::Client => "CLIENT",
            SpanKind::Producer => "PRODUCER",
            SpanKind::Consumer => "CONSUMER",
        };
        write!(f, "{}", s)
    }
}

impl Display for AttributeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AttributeValue::String(s) => write!(f, "\"{}\"", s),
            AttributeValue::Int(i) => write!(f, "{}", i),
            AttributeValue::Float(fl) => write!(f, "{}", fl),
            AttributeValue::Bool(b) => write!(f, "{}", b),
            AttributeValue::StringArray(arr) => write!(f, "{:?}", arr),
            AttributeValue::IntArray(arr) => write!(f, "{:?}", arr),
            AttributeValue::FloatArray(arr) => write!(f, "{:?}", arr),
            AttributeValue::BoolArray(arr) => write!(f, "{:?}", arr),
        }
    }
}

impl SpanEvent {
    /// Create a new SpanEvent
    pub fn new(name: impl Into<String>, timestamp: u64) -> Self {
        Self {
            name: name.into(),
            timestamp,
            attributes: HashMap::new(),
        }
    }

    /// Add an attribute to the event
    pub fn add_attribute(mut self, key: impl Into<String>, value: AttributeValue) -> Self {
        self.attributes.insert(key.into(), value);
        self
    }
}

impl FromStr for SpanKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Internal" => Ok(SpanKind::Internal),
            "Server" => Ok(SpanKind::Server),
            "Client" => Ok(SpanKind::Client),
            "Producer" => Ok(SpanKind::Producer),
            "Consumer" => Ok(SpanKind::Consumer),
            _ => Err(format!("Invalid SpanKind: {}", s)),
        }
    }
}

impl SpanKind {
    pub fn from_index(index: i32) -> Option<Self> {
        match index {
            0 => Some(SpanKind::Internal),
            1 => Some(SpanKind::Server),
            2 => Some(SpanKind::Client),
            3 => Some(SpanKind::Producer),
            4 => Some(SpanKind::Consumer),
            _ => None, // invalid index
        }
    }

    pub fn to_index(self) -> i32 {
        match self {
            SpanKind::Internal => 0,
            SpanKind::Server => 1,
            SpanKind::Client => 2,
            SpanKind::Producer => 3,
            SpanKind::Consumer => 4,
        }
    }
}

impl TraceId {
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub fn from_str(s: &str) -> Result<TraceId, TypeError> {
        if s.len() != 32 {
            return Err(TypeError::InvalidTraceId(format!(
                "Invalid length: {}",
                s.len()
            )));
        }

        let mut bytes = [0u8; 16];

        for i in 0..16 {
            let byte_str = &s[2 * i..2 * i + 2]; // take two chars at a time
            bytes[i] = u8::from_str_radix(byte_str, 16)
                .map_err(|e| format!("Invalid hex: {}", e))
                .unwrap();
        }

        bytes.reverse();
        Ok(TraceId(bytes))
    }

    pub fn to_hex(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";

        let mut out = Vec::with_capacity(32);

        for &b in self.0.iter().rev() {
            out.push(HEX[(b >> 4) as usize]);
            out.push(HEX[(b & 0x0f) as usize]);
        }

        unsafe { String::from_utf8_unchecked(out) }
    }
}

impl Display for TraceId {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl Writable for TraceId {
    fn serialize(&self) -> Vec<u8> {
        self.0[..].to_vec()
    }
}

impl SpanId {
    pub fn as_bytes(&self) -> &[u8; 8] {
        &self.0
    }

    pub fn from_str(s: &str) -> Result<SpanId, TypeError> {
        if s.len() != 16 {
            return Err(TypeError::InvalidSpanId(format!(
                "Invalid length: {}",
                s.len()
            )));
        }

        let mut bytes = [0u8; 8];

        for i in 0..8 {
            let byte_str = &s[2 * i..2 * i + 2]; // take two chars at a time
            bytes[i] = u8::from_str_radix(byte_str, 16)
                .map_err(|e| format!("Invalid hex: {}", e))
                .unwrap();
        }

        bytes.reverse();
        Ok(SpanId(bytes))
    }

    pub fn to_hex(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";

        let mut out = Vec::with_capacity(16);

        for &b in self.0.iter().rev() {
            out.push(HEX[(b >> 4) as usize]);
            out.push(HEX[(b & 0x0f) as usize]);
        }

        unsafe { String::from_utf8_unchecked(out) }
    }
}

impl Display for SpanId {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl Span {
    ///
    /// | Field                      | Size (bytes)                    | Description                                         |
    /// |----------------------------|---------------------------------|-----------------------------------------------------|
    /// | trace_id                   | 16                              | Fixed 16-byte array                                 |
    /// | span_id                    | 8                               | Fixed 8-byte array                                  |
    /// | parent_span_id present flag| 1                               | 0 = None, 1 = Some                                  |
    /// | parent_span_id             | 8 (if present)                  | Fixed 8-byte array                                  |
    /// | name length                | 4                               | u32 length of name                                  |
    /// | name                       | variable                        | UTF-8 bytes of name                                 |
    /// | kind                       | 1                               | SpanKind as u8 (0 = Internal, 1 = Server ...)       |
    /// | timestamp                  | 8                               | u64, little-endian                                  |
    /// | duration                   | 8                               | u64, little-endian                                  |
    /// | attributes length          | 4                               | u32: number of key-value pairs                      |
    /// | attributes (repeated)      | variable                        | key length + key bytes + value length + value bytes |
    /// | events length              | 4                               | u32: number of events                               |
    /// | events (repeated)          | variable                        | serialized SpanEvent (length + data)                |
    /// | status_code present flag   | 1                               | 0 = None, 1 = Some                                  |
    /// | status_code                | 4 (if present)                  | u32                                                 |
    /// | status_message length      | 4 (if present)                  | u32 length of message                               |
    /// | status_message             | variable                        | UTF-8 bytes                                         |
    /// | local_service present flag | 1                               | 0 = None, 1 = Some                                  |
    /// | local_service length       | 4 (if present)                  | u32 length                                          |
    /// | local_service              | variable                        | UTF-8 bytes                                         |
    /// | remote_service present flag| 1                               | 0 = None, 1 = Some                                  |
    /// | remote_service length      | 4 (if present)                  | u32 length                                          |
    /// | remote_service             | variable                        | UTF-8 bytes                                         |
    pub fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        buffer.extend_from_slice(self.trace_id.as_bytes());
        buffer.extend_from_slice(self.span_id.as_bytes());

        // parent_span_id
        if let Some(parent) = &self.parent_span_id {
            // present
            buffer.push(1);
            buffer.extend_from_slice(parent.as_bytes());
        } else {
            buffer.push(0); // absent
        }

        let name_size = (self.name.len() as u32).to_le_bytes();
        buffer.extend(name_size);
        buffer.extend(self.name.as_bytes());
        let kind_byte: u8 = match self.kind {
            SpanKind::Internal => 0,
            SpanKind::Server => 1,
            SpanKind::Client => 2,
            SpanKind::Producer => 3,
            SpanKind::Consumer => 4,
        };
        buffer.push(kind_byte);
        buffer.extend(&self.timestamp.to_le_bytes());
        buffer.extend(&self.duration.to_le_bytes());

        // attributes
        let attr_len = self.attributes.len() as u32;
        buffer.extend(&attr_len.to_le_bytes());
        for (k, v) in &self.attributes {
            let key_len = k.len() as u32;
            buffer.extend(&key_len.to_le_bytes());
            buffer.extend(k.as_bytes());

            match v {
                AttributeValue::String(s) => {
                    buffer.push(0); // type marker
                    let val_len = s.len() as u32;
                    buffer.extend(&val_len.to_le_bytes());
                    buffer.extend(s.as_bytes());
                }
                AttributeValue::Int(i) => {
                    buffer.push(1);
                    buffer.extend(&i.to_le_bytes());
                }
                AttributeValue::Float(f) => {
                    buffer.push(2);
                    buffer.extend(&f.to_le_bytes());
                }
                AttributeValue::Bool(b) => {
                    buffer.push(3);
                    buffer.push(if *b { 1 } else { 0 });
                }
                AttributeValue::StringArray(_str_array) => {}
                AttributeValue::IntArray(_int_array) => {}
                AttributeValue::FloatArray(_float_array) => {}
                AttributeValue::BoolArray(_bool_array) => {}
            }
        }

        // events
        let events_len = self.events.len() as u32;
        buffer.extend(&events_len.to_le_bytes());
        for event in &self.events {
            let name_len = event.name.len() as u32;
            buffer.extend(&name_len.to_le_bytes());
            buffer.extend(event.name.as_bytes());
            buffer.extend(&event.timestamp.to_le_bytes());
        }

        // status_code
        if let Some(code) = self.status_code {
            buffer.push(1);
            buffer.extend(&code.to_le_bytes());
        } else {
            buffer.push(0);
        }

        // status_message
        if let Some(msg) = &self.status_message {
            buffer.push(1);
            let msg_len = msg.len() as u32;
            buffer.extend(&msg_len.to_le_bytes());
            buffer.extend(msg.as_bytes());
        } else {
            buffer.push(0);
        }

        // local_service
        if let Some(local) = &self.local_service {
            buffer.push(1);
            let len = local.len() as u32;
            buffer.extend(&len.to_le_bytes());
            buffer.extend(local.as_bytes());
        } else {
            buffer.push(0);
        }

        // remote_service
        if let Some(remote) = &self.remote_service {
            buffer.push(1);
            let len = remote.len() as u32;
            buffer.extend(&len.to_le_bytes());
            buffer.extend(remote.as_bytes());
        } else {
            buffer.push(0);
        }

        buffer
    }

    pub fn deserialize(vector: &[u8]) -> Span {
        let mut offset = 0;
        let trace_id = read_bytes::<16>(&vector, &mut offset);
        let span_id = read_bytes::<8>(&vector, &mut offset);

        let parent_span_id = if read_u8(&vector, &mut offset)
            .expect("Unable to deserialize parent span flag") == 1 {
            Some(SpanId(read_bytes::<8>(&vector, &mut offset)))
        } else {
            None
        };

        let name = read_string(&vector, &mut offset)
            .expect("Unable to deserialize name from data storage");

        let kind: SpanKind = match read_u8(&vector, &mut offset)
            .expect("Unable to deserialize span kind") {
            0 => SpanKind::Internal,
            1 => SpanKind::Server,
            2 => SpanKind::Client,
            3 => SpanKind::Producer,
            4 => SpanKind::Consumer,
            _ => SpanKind::Internal,
        };

        let timestamp = read_u64(&vector, &mut offset)
            .expect("Unable to deserialize timestamp from data storage");
        let duration = read_u64(&vector, &mut offset)
            .expect("Unable to deserialize duration from data storage");

        let attributes = read_attributes(&vector, &mut offset)
            .expect("Unable to deserialize attributes from data storage");
        let events = read_span_events(&vector, &mut offset)
            .expect("Unable to deserialize events from data storage");

        let status_code = read_status_code(&vector, &mut offset)
            .expect("Unable to deserialize status_code from data storage");

        let status_message = read_status_message(&vector, &mut offset)
            .expect("Unable to deserialize status_message from data storage");

        let local_service = read_local_service(&vector, &mut offset)
            .expect("Unable to deserialize local_service from data storage");

        Span {
            trace_id: TraceId(trace_id),
            span_id: SpanId(span_id),
            parent_span_id,
            name,
            kind,
            timestamp,
            duration,
            attributes,
            events,
            status_code,
            status_message,
            local_service,
            remote_service: None,
        }
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

}

pub trait SizeEstimator {
    fn estimated_size_bytes(&self) -> usize;
}

impl SizeEstimator for Span {

    fn estimated_size_bytes(&self) -> usize {
        let mut size = 0;
        size += size_of::<TraceId>();
        size += size_of::<SpanId>();

        if self.parent_span_id.is_some() {
            size += size_of::<SpanId>()
        }

        size += self.name.len();
        size += 1; //kind
        size += size_of::<u64>(); // timestamp
        size += size_of::<u64>(); // duration
        size += estimated_map_size(&self.attributes);

        for event in &self.events {
            size += event.estimated_size_bytes();
            size += 8; // approximate Vec overhead
        }

        if self.status_code.is_some() {
            size += size_of::<u32>();
        }

        if let Some(status_message) = &self.status_message {
            size += status_message.len();
        }

        if let Some(local_service) = &self.local_service {
            size += local_service.len();
        }

        if let Some(remote_service) = &self.remote_service {
            size += remote_service.len();
        }

        size
    }
}

impl SizeEstimator for AttributeValue {

    fn estimated_size_bytes(&self) -> usize {
        match self {
            AttributeValue::String(value) => value.len(),

            AttributeValue::Int(_) => size_of::<i64>(),
            AttributeValue::Float(_) => size_of::<f64>(),
            AttributeValue::Bool(_) => size_of::<bool>(),

            AttributeValue::StringArray(values) => {
                values.iter().map(|v| v.len()).sum()
            }

            AttributeValue::IntArray(values) => {
                values.len() * size_of::<i64>()
            }

            AttributeValue::FloatArray(values) => {
                values.len() * size_of::<f64>()
            }

            AttributeValue::BoolArray(values) => {
                values.len() * size_of::<bool>()
            }
        }
    }
}

impl SizeEstimator for SpanEvent {

    fn estimated_size_bytes(&self) -> usize {
        let mut size = 0;
        size += self.name.len();
        size += size_of::<u64>(); // timestamp
        size += estimated_map_size(&self.attributes);
        size
    }
}

fn estimated_map_size(map: &HashMap<String, AttributeValue>) -> usize {
    let mut size = 0;

    for (key, value) in map {
        size += key.len();
        size += value.estimated_size_bytes();

        // approximate per-entry overhead for the map itself
        size += 16;
    }
    size
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        // assert_eq!(result, 4);
    }
}
