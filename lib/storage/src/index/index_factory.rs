use std::path::PathBuf;
use crate::index::service_name_index::{LocalServiceNameIndexReader, LocalServiceNameIndexWriter, ServiceNameIndex};
use crate::index::span_name_index::{LocalSpanNameIndexReader, LocalSpanNameIndexWriter, SpanNameIndex};

pub fn local_service_name_index(path: impl Into<PathBuf>) -> ServiceNameIndex {
    let path = path.into();
    let service_name_index_reader = Box::new(LocalServiceNameIndexReader::new(&path));
    let service_name_index_writer = Box::new(LocalServiceNameIndexWriter::new(&path));

    ServiceNameIndex::new(
        service_name_index_reader,
        service_name_index_writer,
    )
}


pub fn local_span_name_index(path: impl Into<PathBuf>) -> SpanNameIndex {
    let path = path.into();
    let span_name_index_reader = Box::new(LocalSpanNameIndexReader::new(&path));
    let span_name_index_writer = Box::new(LocalSpanNameIndexWriter::new(&path));

    SpanNameIndex::new(
        span_name_index_reader,
        span_name_index_writer,
    )
}