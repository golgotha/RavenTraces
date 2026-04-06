use crate::span::SpanEvent;
use common::binary_readers::{read_string, read_u32, read_u64};

pub(crate) fn read_span_events(buffer: &[u8], offset: &mut usize) -> Result<Vec<SpanEvent>, String> {
    let events_len = read_event_length(buffer, offset)?;

    let mut events = Vec::with_capacity(events_len as usize);

    for _ in 0..events_len {
        let name = read_string(buffer, offset)?;
        let timestamp = read_u64(buffer, offset)?;

        events.push(SpanEvent {
            name,
            timestamp,
            attributes: Default::default(),
        });
    }

    Ok(events)
}

fn read_event_length(buffer: &[u8], offset: &mut usize) -> Result<u32, String> {
    if buffer.len() < *offset + 4 {
        return Err("Buffer too small for events_len".into());
    }
    let events_len = read_u32(buffer, offset)?;
    Ok(events_len)
}
