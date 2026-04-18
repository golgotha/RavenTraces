use common::binary_readers::{read_string, read_u8};

pub(crate) fn read_status_message(buffer: &[u8], offset: &mut usize) -> Result<Option<String>, String> {
    let status_message_exists = read_u8(buffer, offset)?;

    if status_message_exists > 0 {
        let message = read_string(buffer, offset);
        Ok(Some(message?))
    } else {
        Ok(None)
    }
}