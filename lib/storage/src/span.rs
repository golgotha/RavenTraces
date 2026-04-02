use std::collections::HashMap;
use std::fmt;

pub type TraceId = [u8; 16];
pub type SpanId = [u8; 8];

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

impl fmt::Display for SpanKind {
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

impl fmt::Display for AttributeValue {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        // assert_eq!(result, 4);
    }
}