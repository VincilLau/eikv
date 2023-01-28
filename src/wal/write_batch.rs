use crate::{
    model::Entry,
    util::{
        checksum::crc32_checksum,
        coding::{append_fixed_u32, encode_fixed_u32},
    },
    EikvError, EikvResult, Key, Value,
};

pub struct WriteBatch<K: Key, V: Value> {
    entries: Vec<Entry<K, V>>,
}

impl<K: Key, V: Value> WriteBatch<K, V> {
    pub fn new() -> WriteBatch<K, V> {
        WriteBatch { entries: vec![] }
    }

    pub(crate) fn entries(&self) -> &Vec<Entry<K, V>> {
        &self.entries
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn extend(&mut self, other: Self) {
        self.entries.extend(other.entries);
    }

    pub(crate) fn set_seqs(&mut self, start: u64) {
        let mut seq = start;
        for entry in &mut self.entries {
            entry.seq = seq;
            seq += 1;
        }
    }

    pub fn put(&mut self, key: K, value: V) -> &mut Self {
        let entry = Entry {
            key,
            seq: 0,
            value: Some(value),
        };
        self.entries.push(entry);
        self
    }

    pub fn delete(&mut self, key: K) -> &mut Self {
        let entry = Entry {
            key,
            seq: 0,
            value: None,
        };
        self.entries.push(entry);
        self
    }

    pub(super) fn encode(self, buf: &mut Vec<u8>) -> EikvResult<()> {
        let old_len = buf.len();

        append_fixed_u32(buf, 0);
        append_fixed_u32(buf, 0);

        for entry in self.entries {
            entry.encode(buf)?;
        }

        let len = buf.len() - old_len;
        encode_fixed_u32(&mut buf[old_len + 4..old_len + 8], len as u32);
        let checksum = crc32_checksum(&buf[old_len..]);
        encode_fixed_u32(&mut buf[old_len..old_len + 4], checksum);

        Ok(())
    }

    pub(super) fn decode(buf: &[u8], checksum: u32) -> EikvResult<Self> {
        if checksum != crc32_checksum(&buf) {
            let reason = "the checksumes of the write batch doesn't match".to_owned();
            return Err(EikvError::WalCorrpution(reason));
        }

        let mut buf_off = 8;
        let mut entries = vec![];
        while buf_off != buf.len() {
            let (entry, n) = Entry::decode(&buf[buf_off..])?;
            entries.push(entry);
            buf_off += n;
        }

        let write_batch = WriteBatch { entries };
        Ok(write_batch)
    }
}
