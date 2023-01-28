use std::cmp::Ordering;

use crate::{
    util::coding::{append_var_u32, append_var_u64, decode_bytes_with_len, decode_var_u64},
    EikvError, EikvResult, Key, Value,
};

#[derive(Clone)]
pub(crate) struct Entry<K: Key, V: Value> {
    pub(crate) key: K,
    pub(crate) seq: u64,
    pub(crate) value: Option<V>,
}

impl<K: Key, V: Value> PartialEq for Entry<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.seq == other.seq
    }
}

impl<K: Key, V: Value> Eq for Entry<K, V> {}

impl<K: Key, V: Value> PartialOrd for Entry<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.key.partial_cmp(&other.key) {
            Some(core::cmp::Ordering::Equal) => self.seq.partial_cmp(&other.seq),
            ord => return ord,
        }
    }
}

impl<K: Key, V: Value> Ord for Entry<K, V> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Equal => self.seq.cmp(&other.seq),
            ord => ord,
        }
    }
}

impl<K: Key, V: Value> Entry<K, V> {
    pub(crate) fn encode(self, buf: &mut Vec<u8>) -> EikvResult<()> {
        let key_bytes = self.key.encode()?;
        let key_len = key_bytes.len() as u32;
        append_var_u32(buf, key_len);
        buf.extend(key_bytes);

        append_var_u64(buf, self.seq);

        match self.value {
            Some(value) => {
                buf.push(1);
                let value_bytes = value.encode()?;
                let value_len = value_bytes.len() as u32;
                append_var_u32(buf, value_len);
                buf.extend(value_bytes);
            }
            None => buf.push(2),
        }

        Ok(())
    }

    fn decode_to_vec_u8(buf: &[u8]) -> Option<(Entry<Vec<u8>, Vec<u8>>, usize)> {
        let (key, mut buf_off) = match decode_bytes_with_len(buf) {
            Some((key, n)) => (key, n),
            None => return None,
        };

        let seq = match decode_var_u64(&buf[buf_off..]) {
            Some((seq, n)) => {
                buf_off += n;
                seq
            }
            None => return None,
        };

        if buf_off == buf.len() {
            return None;
        }
        let mut entry = Entry {
            key,
            seq,
            value: None,
        };
        buf_off += 1;

        match buf[buf_off - 1] {
            1 => {
                let value = match decode_bytes_with_len(&buf[buf_off..]) {
                    Some((value, n)) => {
                        buf_off += n;
                        value
                    }
                    None => return None,
                };

                entry.value = Some(value);
                Some((entry, buf_off))
            }
            2 => Some((entry, buf_off)),
            _ => None,
        }
    }

    pub(crate) fn decode(buf: &[u8]) -> EikvResult<(Self, usize)> {
        match Self::decode_to_vec_u8(buf) {
            Some((vec_u8_entry, n)) => {
                let key = K::decode(vec_u8_entry.key)?;
                let mut entry = Self {
                    key,
                    seq: vec_u8_entry.seq,
                    value: None,
                };

                if let Some(value) = vec_u8_entry.value {
                    let value = Value::decode(value)?;
                    entry.value = Some(value)
                }

                Ok((entry, n))
            }
            None => {
                let reason = "entry is corrupt".to_owned();
                Err(EikvError::WalCorrpution(reason))
            }
        }
    }
}
