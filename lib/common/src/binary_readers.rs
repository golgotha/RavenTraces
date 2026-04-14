
pub fn read_bytes<const N: usize>(buf: &[u8], offset: &mut usize) -> [u8; N] {
    let val: [u8; N] = buf[*offset..*offset + N].try_into().unwrap();
    *offset += N;
    val
}

pub fn read_n_bytes(buf: &[u8], offset: &mut usize, length: usize) -> Vec<u8> {
    let end = *offset + length;
    let val = buf[*offset..end].to_vec();
    *offset = end;
    val
}

pub fn read_u8(buf: &[u8], offset: &mut usize) -> Result<u8, String> {
    if buf.len() < *offset + 1 {
        return Err("buffer too small for u8 value".into());
    }
    let val = buf[*offset];
    *offset += 1;
    Ok(val)
}

pub fn read_u32(buf: &[u8], offset: &mut usize) -> Result<u32, String> {
    if buf.len() < *offset + 4 {
        return Err("buffer too small for u32 value".into());
    }

    let val = u32::from_le_bytes(buf[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    Ok(val)
}

pub fn read_u64(buf: &[u8], offset: &mut usize) -> Result<u64, String> {
    if buf.len() < *offset + 8 {
        return Err("Buffer too small for u64 value".into());
    }
    let val = u64::from_le_bytes(buf[*offset..*offset + 8].try_into().unwrap());
    *offset += 8;
    Ok(val)
}

pub fn read_i64(buf: &[u8], offset: &mut usize) -> Result<i64, String> {
    if buf.len() < *offset + 8 {
        return Err("buffer too small for int value".into());
    }
    let val = i64::from_le_bytes(buf[*offset..*offset + 8].try_into().unwrap());
    *offset += 8;
    Ok(val)
}

pub fn read_f64(buf: &[u8], offset: &mut usize) -> Result<f64, String> {
    if buf.len() < *offset + 8 {
        return Err("buffer too small for float value".into());
    }
    let val = f64::from_le_bytes(buf[*offset..*offset + 8].try_into().unwrap());
    *offset += 8;
    Ok(val)
}

pub fn read_bool(buf: &[u8], offset: &mut usize) -> Result<bool, String> {
    if buf.len() < *offset + 1 {
        return Err("buffer too small for bool value".into());
    }
    let b = buf[*offset] != 0;
    *offset += 1;
    Ok(b)
}

pub fn read_string(buf: &[u8], offset: &mut usize) -> Result<String, String> {
    if buf.len() < *offset + 4 {
        return Err("buffer too small for string length".into());
    }

    let len = read_u32(buf, offset)? as usize;

    if buf.len() < *offset + len {
        return Err("buffer too small for string bytes".into());
    }

    let val = String::from_utf8(buf[*offset..*offset + len].to_vec())
        .map_err(|_| "invalid UTF-8 string value")?;
    *offset += len;
    Ok(val)
}