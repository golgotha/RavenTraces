use common::binary_readers::read_u8;

pub(crate) fn read_status_code(buffer: &[u8], offset: &mut usize) -> Result<Option<u32>, String> {
    let status_code_exists = read_u8(buffer, offset)?;
    match status_code_exists {
        0 => Ok(None),
        1 => {
            let status_code = Some(u32::from_le_bytes(buffer[*offset..*offset + 4].try_into().unwrap()));
            *offset += 4;
            Ok(status_code)
        },
        _ => Ok(None)
    }
}