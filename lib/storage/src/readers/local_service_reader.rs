use common::binary_readers::{read_string, read_u8};

pub(crate) fn read_local_service(buffer: &[u8], offset: &mut usize) -> Result<Option<String>, String> {
    let read_local_service_exists = read_u8(buffer, offset)?;

    if read_local_service_exists > 0 {
        let message = read_string(buffer, offset);
        Ok(Some(message?))
    } else {
        Ok(None)
    }
}
