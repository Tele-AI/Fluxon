use crate::memholder::kvclient_encode::{FlatKvValueRange, flat_kv_decode_ranges};
use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult};
use crate::user_api::flat_dict::{FlatDict, FlatValue};

const FLAT_KV_TYPE_INT64: u8 = 1;
const FLAT_KV_TYPE_FLOAT64: u8 = 3;
const FLAT_KV_TYPE_STRING: u8 = 4;
const FLAT_KV_TYPE_BYTES: u8 = 5;
const FLAT_KV_TYPE_BOOL: u8 = 7;

fn invalid_arg(detail: impl Into<String>) -> KvError {
    KvError::Api(ApiError::InvalidArgument {
        detail: detail.into(),
    })
}

pub fn decode_flat_dict_bytes(data: &[u8]) -> KvResult<FlatDict> {
    let items = flat_kv_decode_ranges(data)
        .map_err(|e| invalid_arg(format!("flat dict decode failed: {}", e)))?;
    let mut out: FlatDict = FlatDict::new();
    for (k, v) in items {
        let vv = match v {
            FlatKvValueRange::Bool(b) => FlatValue::Bool(b),
            FlatKvValueRange::Int64(i) => FlatValue::Int64(i),
            FlatKvValueRange::Float64(f) => FlatValue::Float64(f),
            FlatKvValueRange::String(s) => FlatValue::String(s),
            FlatKvValueRange::BytesRange { start, len } => {
                if start + len > data.len() {
                    return Err(invalid_arg("flat dict bytes range out of bounds"));
                }
                FlatValue::Bytes(data[start..(start + len)].to_vec())
            }
        };
        out.insert(k, vv);
    }
    Ok(out)
}

/// Locate a bytes field in an encoded flat dict without materializing the dict or copying the
/// field payload.
///
/// The whole encoded value is still validated so malformed entries after the requested field do
/// not get silently accepted. The returned range points into `data`.
pub fn find_flat_dict_bytes_field_range(
    data: &[u8],
    field_key: &str,
) -> KvResult<Option<(usize, usize)>> {
    fn need(data: &[u8], pos: usize, len: usize, what: &str) -> KvResult<usize> {
        let end = pos
            .checked_add(len)
            .ok_or_else(|| invalid_arg(format!("flat dict {} range overflow", what)))?;
        if end > data.len() {
            return Err(invalid_arg(format!("flat dict truncated {}", what)));
        }
        Ok(end)
    }

    fn read_u32(data: &[u8], pos: &mut usize, what: &str) -> KvResult<u32> {
        let end = need(data, *pos, 4, what)?;
        let bytes: [u8; 4] = data[*pos..end]
            .try_into()
            .expect("flat dict u32 length checked");
        *pos = end;
        Ok(u32::from_le_bytes(bytes))
    }

    let mut pos = 0usize;
    let count = read_u32(data, &mut pos, "entry count header")? as usize;
    let mut found = None;

    for _ in 0..count {
        let key_len = read_u32(data, &mut pos, "key length")? as usize;
        let key_end = need(data, pos, key_len, "key bytes")?;
        let key = std::str::from_utf8(&data[pos..key_end])
            .map_err(|e| invalid_arg(format!("flat dict invalid UTF-8 key: {}", e)))?;
        pos = key_end;

        let type_end = need(data, pos, 1, "value type id")?;
        let type_id = data[pos];
        pos = type_end;

        let value_len = read_u32(data, &mut pos, "value length")? as usize;
        let value_start = pos;
        let value_end = need(data, value_start, value_len, "value bytes")?;
        let value_bytes = &data[value_start..value_end];

        match type_id {
            FLAT_KV_TYPE_BOOL if value_len != 1 => {
                return Err(invalid_arg(format!(
                    "flat dict bool length must be 1 (key={:?})",
                    key
                )));
            }
            FLAT_KV_TYPE_INT64 | FLAT_KV_TYPE_FLOAT64 if value_len != 8 => {
                return Err(invalid_arg(format!(
                    "flat dict scalar length must be 8 (key={:?})",
                    key
                )));
            }
            FLAT_KV_TYPE_STRING => {
                std::str::from_utf8(value_bytes).map_err(|e| {
                    invalid_arg(format!(
                        "flat dict invalid UTF-8 string value for key {:?}: {}",
                        key, e
                    ))
                })?;
            }
            FLAT_KV_TYPE_BOOL | FLAT_KV_TYPE_INT64 | FLAT_KV_TYPE_FLOAT64 | FLAT_KV_TYPE_BYTES => {}
            _ => {
                return Err(invalid_arg(format!(
                    "flat dict unknown type id {} for key {:?}",
                    type_id, key
                )));
            }
        }

        // Match full FlatDict decoding semantics for duplicate keys: the last entry wins, and a
        // last entry with a non-bytes type makes this lookup a type mismatch.
        if key == field_key {
            found = (type_id == FLAT_KV_TYPE_BYTES).then_some((value_start, value_len));
        }
        pos = value_end;
    }

    if pos != data.len() {
        return Err(invalid_arg("flat dict trailing bytes present"));
    }
    Ok(found)
}

pub fn encode_flat_dict_bytes(value: &FlatDict) -> KvResult<Vec<u8>> {
    if value.len() > (u32::MAX as usize) {
        return Err(invalid_arg("flat dict too large"));
    }

    let mut out: Vec<u8> = Vec::new();
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    for (k, v) in value.iter() {
        let kb = k.as_bytes();
        if kb.len() > (u32::MAX as usize) {
            return Err(invalid_arg("flat dict key too large"));
        }
        out.extend_from_slice(&(kb.len() as u32).to_le_bytes());
        out.extend_from_slice(kb);
        match v {
            FlatValue::Bool(b) => {
                out.push(FLAT_KV_TYPE_BOOL);
                out.extend_from_slice(&1u32.to_le_bytes());
                out.push(if *b { 1 } else { 0 });
            }
            FlatValue::Int64(i) => {
                out.push(FLAT_KV_TYPE_INT64);
                out.extend_from_slice(&8u32.to_le_bytes());
                out.extend_from_slice(&i.to_le_bytes());
            }
            FlatValue::Float64(f) => {
                out.push(FLAT_KV_TYPE_FLOAT64);
                out.extend_from_slice(&8u32.to_le_bytes());
                out.extend_from_slice(&f.to_le_bytes());
            }
            FlatValue::String(s) => {
                let vb = s.as_bytes();
                if vb.len() > (u32::MAX as usize) {
                    return Err(invalid_arg("flat dict string too large"));
                }
                out.push(FLAT_KV_TYPE_STRING);
                out.extend_from_slice(&(vb.len() as u32).to_le_bytes());
                out.extend_from_slice(vb);
            }
            FlatValue::Bytes(b) => {
                if b.len() > (u32::MAX as usize) {
                    return Err(invalid_arg("flat dict bytes too large"));
                }
                out.push(FLAT_KV_TYPE_BYTES);
                out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                out.extend_from_slice(b);
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_bytes_field_returns_range_into_encoded_value() {
        let value = FlatDict::from([
            ("count".to_string(), FlatValue::Int64(7)),
            (
                "payload".to_string(),
                FlatValue::Bytes(b"payload-without-copy".to_vec()),
            ),
        ]);
        let encoded = encode_flat_dict_bytes(&value).expect("encode flat dict");

        let (start, len) = find_flat_dict_bytes_field_range(&encoded, "payload")
            .expect("scan flat dict")
            .expect("bytes field");

        assert_eq!(&encoded[start..start + len], b"payload-without-copy");
    }

    #[test]
    fn find_bytes_field_preserves_missing_and_type_mismatch_semantics() {
        let value = FlatDict::from([(
            "payload".to_string(),
            FlatValue::String("not bytes".to_string()),
        )]);
        let encoded = encode_flat_dict_bytes(&value).expect("encode flat dict");

        assert_eq!(
            find_flat_dict_bytes_field_range(&encoded, "missing").expect("scan missing field"),
            None
        );
        assert_eq!(
            find_flat_dict_bytes_field_range(&encoded, "payload").expect("scan type mismatch"),
            None
        );
    }

    #[test]
    fn find_bytes_field_validates_the_complete_encoded_value() {
        let value = FlatDict::from([("payload".to_string(), FlatValue::Bytes(b"data".to_vec()))]);
        let mut encoded = encode_flat_dict_bytes(&value).expect("encode flat dict");
        encoded.push(0);

        assert!(find_flat_dict_bytes_field_range(&encoded, "payload").is_err());
    }
}
