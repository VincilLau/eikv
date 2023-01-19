use crate::{
    proto::wal::{Entry, WriteBatch as ProtoWriteBatch},
    util::{
        checksum::crc32_checksum,
        coding::{append_fixed_u32, encode_fixed_u32},
    },
    EikvResult,
};
use prost::Message;

pub struct WriteBatch {
    pub(super) pwb: ProtoWriteBatch,
}

impl WriteBatch {
    pub fn new() -> WriteBatch {
        WriteBatch {
            pwb: ProtoWriteBatch { entries: vec![] },
        }
    }

    pub(crate) fn entries(&self) -> &[Entry] {
        &self.pwb.entries
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> &mut Self {
        let entry = Entry {
            key,
            value: Some(value),
        };
        self.pwb.entries.push(entry);
        self
    }

    pub fn delete(&mut self, key: Vec<u8>) -> &mut Self {
        let entry = Entry { key, value: None };
        self.pwb.entries.push(entry);
        self
    }

    pub(super) fn append_to(&self, buf: &mut Vec<u8>) -> EikvResult<()> {
        let old_len = buf.len();

        let encoded_len = 4 + 4 + self.pwb.encoded_len();
        buf.repeat(encoded_len);

        append_fixed_u32(buf, 0);
        append_fixed_u32(buf, 0);
        self.pwb.encode(buf)?;

        let len = buf.len() - old_len;
        encode_fixed_u32(&mut buf[old_len + 4..old_len + 8], len as u32);
        let checksum = crc32_checksum(&buf[old_len..]);
        encode_fixed_u32(&mut buf[old_len..old_len + 4], checksum);

        Ok(())
    }
}
