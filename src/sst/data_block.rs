use crate::{
    model::Entry,
    util::{
        checksum::crc32_checksum,
        coding::{
            append_fixed_u32, append_var_u32, append_var_u64, decode_bytes_with_len,
            decode_fixed_u32, decode_var_u32, decode_var_u64,
        },
    },
    Compressor, DBOptions, EikvError, EikvResult, Filter, FilterFactory, Key, Value,
};
use std::{
    cmp::{min, Ordering},
    sync::Arc,
};

struct SharePrefixEntry {
    shared_len: u32,
    unshared_key: Vec<u8>,
    seq: u64,
    value: Option<Vec<u8>>,
}

fn shared_len(s1: &[u8], s2: &[u8]) -> usize {
    let min_len = min(s1.len(), s2.len());
    for i in 0..min_len {
        if s1[i] != s2[i] {
            return i;
        }
    }
    min_len
}

impl SharePrefixEntry {
    fn new(key: &[u8], seq: u64, value: Option<Vec<u8>>, prev_key: &[u8]) -> SharePrefixEntry {
        let shared_len = shared_len(key, prev_key);
        let unshared_key = key[shared_len..].to_vec();
        let shared_len = shared_len as u32;
        SharePrefixEntry {
            shared_len,
            unshared_key,
            seq,
            value,
        }
    }

    fn encode(&self, buf: &mut Vec<u8>) {
        append_var_u32(buf, self.shared_len);
        append_var_u32(buf, self.unshared_key.len() as u32);
        buf.extend(&self.unshared_key);
        append_var_u64(buf, self.seq);
        match &self.value {
            Some(value) => {
                buf.push(1);
                append_var_u32(buf, value.len() as u32);
                buf.extend(value);
            }
            None => buf.push(2),
        }
    }

    fn decode(buf: &[u8], prev_key: Vec<u8>) -> Option<(Entry<Vec<u8>, Vec<u8>>, usize)> {
        let (shared_len, mut buf_off) = match decode_var_u32(buf) {
            Some((shared_len, n)) => (shared_len as usize, n),
            None => return None,
        };
        if shared_len > prev_key.len() {
            return None;
        }

        let unshared_key = match decode_bytes_with_len(&buf[buf_off..]) {
            Some((unshared_key, n)) => {
                buf_off += n;
                unshared_key
            }
            None => return None,
        };
        let mut key = prev_key;
        key.truncate(shared_len);
        key.extend(&unshared_key);

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
        buf_off += 1;
        let mut entry = Entry {
            key,
            seq,
            value: None,
        };
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
}

pub(super) struct Builder<K: Key, V: Value> {
    options: DBOptions,
    entries: Vec<u8>,
    restart_points: Vec<u32>,
    restart_index: usize,
    filter: Option<Box<dyn Filter>>,
    prev_key: Vec<u8>,
    min_entry: Option<Entry<K, V>>,
}

impl<K: Key, V: Value> Builder<K, V> {
    pub(super) fn new(options: DBOptions) -> Builder<K, V> {
        let filter = match &options.filter_factory {
            Some(filter_factory) => Some(filter_factory.create()),
            None => None,
        };
        Builder {
            options,
            entries: vec![],
            restart_points: vec![],
            restart_index: 0,
            filter,
            prev_key: vec![],
            min_entry: None,
        }
    }

    pub(super) fn full(&self) -> bool {
        self.entries.len() >= self.options.block_size
    }

    pub(super) fn append(&mut self, entry: Entry<K, V>) -> EikvResult<()> {
        if self.min_entry.is_none() {
            self.min_entry = Some(entry);
            return Ok(());
        }

        if self.restart_index == 0 {
            self.restart_points.push(self.entries.len() as u32);
            self.prev_key.clear();
        }
        self.restart_index = (self.restart_index + 1) % self.options.restart_interval;

        let key = entry.key.encode()?;
        let entry = SharePrefixEntry::new(
            &key,
            entry.seq,
            match entry.value {
                Some(value) => Some(value.encode()?),
                None => None,
            },
            &self.prev_key,
        );
        if let Some(filter) = &mut self.filter {
            filter.add(&key);
        }
        self.prev_key = key;

        entry.encode(&mut self.entries);
        Ok(())
    }

    pub(super) fn build(self) -> EikvResult<Vec<u8>> {
        let mut buf = self.entries;
        for restart_point in &self.restart_points {
            append_fixed_u32(&mut buf, *restart_point);
        }
        append_fixed_u32(&mut buf, self.restart_points.len() as u32);

        let mut block = match self.options.compressor {
            Some(compressor) => compressor.compress(&buf)?,
            None => buf,
        };

        let filter_offset = block.len() as u32;
        if let Some(filter) = &self.filter {
            filter.encode(&mut block)?;
        }

        let min_entry_offset = block.len() as u32;
        self.min_entry.unwrap().encode(&mut block)?;

        if let Some(_) = &self.filter {
            append_fixed_u32(&mut block, filter_offset);
        }
        append_fixed_u32(&mut block, min_entry_offset);
        let checksum = crc32_checksum(&block);
        append_fixed_u32(&mut block, checksum);

        Ok(block)
    }
}

fn verify_checksum(block: &[u8]) -> EikvResult<()> {
    let block_size = block.len();
    let checksum = decode_fixed_u32(&block[block_size - 4..]);
    if crc32_checksum(&block[..block_size - 4]) != checksum {
        let reason = "the checksum of the data block doesn't match".to_owned();
        return Err(EikvError::SstCorrpution(reason));
    }
    Ok(())
}

fn decode_min_entry_offset(block: &[u8]) -> EikvResult<usize> {
    let block_size = block.len();
    let min_entry_offset = decode_fixed_u32(&block[block_size - 8..block_size - 4]) as usize;
    if min_entry_offset > block_size - 8 {
        let reason = "data block is corrupt".to_owned();
        Err(EikvError::SstCorrpution(reason))
    } else {
        Ok(min_entry_offset)
    }
}

fn decode_min_entry<K: Key, V: Value>(
    block: &[u8],
    min_entry_offset: usize,
) -> EikvResult<Entry<K, V>> {
    let block_size = block.len();
    let min_entry_buf = &block[min_entry_offset..block_size - 8];
    let (min_entry, _) = Entry::decode(min_entry_buf)?;
    Ok(min_entry)
}

fn decode_filter_offset(block: &[u8], min_entry_offset: usize) -> EikvResult<usize> {
    let block_size = block.len();
    let filter_offset = decode_fixed_u32(&block[block_size - 12..block_size - 8]) as usize;
    if filter_offset > min_entry_offset {
        let reason = "data block is corrupt".to_owned();
        Err(EikvError::SstCorrpution(reason))
    } else {
        Ok(filter_offset)
    }
}

fn decode_filter(
    block: &[u8],
    min_entry_offset: usize,
    filter_factory: Arc<dyn FilterFactory>,
) -> EikvResult<Box<dyn Filter>> {
    let filter_offset = decode_filter_offset(block, min_entry_offset)?;
    let filter_buf = &block[filter_offset..min_entry_offset];
    let filter = filter_factory.decode(filter_buf)?;
    Ok(filter)
}

fn decode_payload<K: Key, V: Value>(
    payload: &[u8],
    entries: &mut Vec<Entry<K, V>>,
) -> EikvResult<()> {
    let payload_size = payload.len();
    let restart_point_count = decode_fixed_u32(&payload[payload_size - 4..]) as usize;
    let restart_point_start = payload_size - 4 - restart_point_count * 4;
    let entries_buf = &payload[..restart_point_start];

    let mut prev_key = vec![];
    let mut buf_off = 0;
    while buf_off < entries_buf.len() {
        match SharePrefixEntry::decode(&payload[buf_off..], prev_key) {
            Some((entry, n)) => {
                buf_off += n;
                prev_key = entry.key.clone();
                let key = K::decode(entry.key)?;
                let seq = entry.seq;
                let value = match entry.value {
                    Some(value) => Some(Value::decode(value)?),
                    None => None,
                };
                entries.push(Entry { key, seq, value });
            }
            None => {
                let reason = "data block is corrupt".to_owned();
                return Err(EikvError::SstCorrpution(reason));
            }
        }
    }

    Ok(())
}

pub(super) fn decode_block<K: Key, V: Value>(
    block: &[u8],
    compressor: Option<Arc<dyn Compressor>>,
    has_filter: bool,
) -> EikvResult<Vec<Entry<K, V>>> {
    let min_entry_offset = decode_min_entry_offset(block)?;
    let min_entry = decode_min_entry(block, min_entry_offset)?;
    let payload_end = if has_filter {
        decode_filter_offset(block, min_entry_offset)?
    } else {
        min_entry_offset
    };

    let payload = &block[..payload_end];
    let mut entries = vec![min_entry];
    match compressor {
        Some(compressor) => {
            let payload = compressor.uncompress(block)?;
            decode_payload(&payload, &mut entries)?;
        }
        None => decode_payload(payload, &mut entries)?,
    }

    Ok(entries)
}

fn find<K: Key, V: Value>(
    block: &[u8],
    key: &K,
    min_entry_offset: usize,
    compressor: Option<Arc<dyn Compressor>>,
    filter_factory: Option<Arc<dyn FilterFactory>>,
) -> EikvResult<Option<Entry<K, V>>> {
    let payload_end = match filter_factory {
        Some(filter_factory) => {
            let filter = decode_filter(block, min_entry_offset, filter_factory)?;
            let key_bytes = key.clone().encode()?;
            if !filter.may_match(&key_bytes) {
                return Ok(None);
            }
            decode_filter_offset(block, min_entry_offset)?
        }
        None => min_entry_offset,
    };

    let payload = &block[..payload_end];
    let entry = match compressor {
        Some(compressor) => {
            let payload = compressor.uncompress(block)?;
            find_in_payload(&payload, key)?
        }
        None => find_in_payload(&payload, key)?,
    };

    let min_entry = decode_min_entry(block, min_entry_offset)?;
    match entry {
        Some(entry) => {
            if min_entry.key != *key {
                Ok(Some(entry))
            } else {
                if min_entry.seq > entry.seq {
                    Ok(Some(min_entry))
                } else {
                    Ok(Some(entry))
                }
            }
        }
        None => {
            if min_entry.key == *key {
                Ok(Some(min_entry))
            } else {
                Ok(None)
            }
        }
    }
}

fn decode_restart_points(payload: &[u8]) -> EikvResult<Vec<u32>> {
    let payload_size = payload.len();
    let restart_point_count = decode_fixed_u32(&payload[payload_size - 4..]) as usize;
    let restart_point_start = payload_size - 4 - restart_point_count * 4;
    let mut restart_points = vec![];
    restart_points.reserve(restart_point_count);
    for off in (restart_point_start..payload_size - 4).step_by(4) {
        let restart_point = decode_fixed_u32(&payload[off..off + 4]);
        restart_points.push(restart_point);
    }
    Ok(restart_points)
}

fn find_in_payload<K: Key, V: Value>(payload: &[u8], key: &K) -> EikvResult<Option<Entry<K, V>>> {
    let mut restart_points = decode_restart_points(payload)?;
    let mut chunks = vec![];
    chunks.reserve(restart_points.len());
    let buf_end = payload.len() - (restart_points.len() + 1) * 4;
    restart_points.push(buf_end as u32);

    for i in 0..restart_points.len() - 1 {
        chunks.push((restart_points[i], restart_points[i + 1]));
    }

    let buf = &payload[..buf_end];
    find_dichotomic(buf, key, &chunks)
}

fn find_dichotomic<K: Key, V: Value>(
    buf: &[u8],
    key: &K,
    chunks: &[(u32, u32)],
) -> EikvResult<Option<Entry<K, V>>> {
    if chunks.len() == 1 {
        let start = chunks[0].0 as usize;
        let end = chunks[0].1 as usize;
        return find_in_sequence(&buf[start..end], key);
    }

    let mid = chunks.len() / 2;
    let start = chunks[mid].0 as usize;
    let end = chunks[mid].1 as usize;
    let entry = match SharePrefixEntry::decode(&buf[start..end], vec![]) {
        Some((entry, _)) => entry,
        None => {
            let reason = "data block is corrupt".to_owned();
            return Err(EikvError::SstCorrpution(reason));
        }
    };

    let k = K::decode(entry.key)?;
    match key.cmp(&k) {
        Ordering::Less => find_dichotomic(buf, key, &chunks[..mid]),
        _ => find_dichotomic(buf, key, &chunks[mid..]),
    }
}

fn find_in_sequence<K: Key, V: Value>(buf: &[u8], key: &K) -> EikvResult<Option<Entry<K, V>>> {
    let mut prev_key = vec![];
    let mut buf_off = 0;
    let mut target = None;

    while buf_off < buf.len() {
        match SharePrefixEntry::decode(&buf[buf_off..], prev_key) {
            Some((entry, n)) => {
                buf_off += n;

                let k = K::decode(entry.key)?;
                match k.cmp(key) {
                    Ordering::Less => {
                        prev_key = k.encode()?;
                        continue;
                    }
                    Ordering::Equal => {
                        let v: Option<V> = match entry.value {
                            Some(value) => Some(Value::decode(value)?),
                            None => None,
                        };
                        let entry = Entry {
                            key: k.clone(),
                            seq: entry.seq,
                            value: v,
                        };
                        target = Some(entry);

                        prev_key = k.encode()?;
                        continue;
                    }
                    Ordering::Greater => {
                        break;
                    }
                }
            }
            None => {
                let reason = "data block is corrupt".to_owned();
                return Err(EikvError::SstCorrpution(reason));
            }
        }
    }

    Ok(target)
}
