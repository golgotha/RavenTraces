use std::collections::HashMap;
use common::binary_readers::{read_bool, read_f64, read_i64, read_string, read_u32, read_u8};

use crate::span::AttributeValue;

pub(crate) fn read_attributes(
    buffer: &[u8],
    offset: &mut usize,
) -> Result<HashMap<String, AttributeValue>, String> {
    let attr_len = read_u32(buffer, offset)?;

    let mut attributes = HashMap::new();

    for _ in 0..attr_len {
        let key = read_string(buffer, offset)?;
        let type_marker = read_u8(buffer, offset)?;

        // Read the value based on type
        let value = match type_marker {
            0 => {
                let s = read_string(buffer, offset)?;
                AttributeValue::String(s)
            }
            1 => {
                let i = read_i64(buffer, offset)?;
                AttributeValue::Int(i)
            }
            2 => {
                let f = read_f64(buffer, offset)?;
                AttributeValue::Float(f)
            }
            3 => {
                let b = read_bool(buffer, offset)?;
                AttributeValue::Bool(b)
            }
            // TODO: implement arrays if needed
            _ => return Err(format!("unknown attribute type: {}", type_marker)),
        };

        attributes.insert(key, value);
    }

    Ok(attributes)
}