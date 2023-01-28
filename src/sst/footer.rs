use std::{
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom},
};

use crate::{
    model::Entry,
    util::{
        checksum::crc32_checksum,
        coding::{append_fixed_u32, append_fixed_u64, decode_fixed_u32, decode_fixed_u64},
    },
    EikvError, EikvResult, Key, Value,
};

pub(crate) struct Footer<K: Key, V: Value> {
    pub(crate) min_entry: Entry<K, V>,
    pub(crate) max_entry: Entry<K, V>,
    pub(crate) data_block_end: u64,
    pub(crate) data_block_count: u32,
}

impl<K: Key, V: Value> Footer<K, V> {
    pub(super) fn encode(self, buf: &mut Vec<u8>) -> EikvResult<()> {
        self.min_entry.encode(buf)?;
        self.max_entry.encode(buf)?;
        append_fixed_u64(buf, self.data_block_end);
        append_fixed_u32(buf, self.data_block_count);
        Ok(())
    }

    fn decode(buf: &[u8]) -> EikvResult<Footer<K, V>> {
        let (min_entry, mut buf_off) = Entry::decode(buf)?;
        let (max_entry, n) = Entry::decode(&buf[buf_off..])?;
        buf_off += n;
        let data_block_end = decode_fixed_u64(&buf[buf_off..buf_off + 8]);
        buf_off += 8;
        let data_block_count = decode_fixed_u32(&buf[buf_off..buf_off + 4]);
        let footer = Footer {
            min_entry,
            max_entry,
            data_block_end,
            data_block_count: data_block_count,
        };
        Ok(footer)
    }

    pub(crate) fn load(path: &str) -> EikvResult<Footer<K, V>> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        file.seek(SeekFrom::End(-8))?;
        let mut buf = [0; 8];
        let n = file.read(&mut buf)?;
        if n != buf.len() {
            return Err(EikvError::SstCorrpution(
                "footer size and checksum is corrupt".to_owned(),
            ));
        }
        let footer_size = decode_fixed_u32(&buf[..4]) as usize;
        let checksum = decode_fixed_u32(&buf[4..]);

        file.seek(SeekFrom::End(-(footer_size as i64 + 8)))?;
        let mut buf = vec![0; footer_size + 4];
        let n = file.read(&mut buf)?;
        if n != buf.len() {
            let reason = format!("footer size is {}, read {} bytes", buf.len(), n);
            return Err(EikvError::SstCorrpution(reason));
        }
        let expect_checksum = crc32_checksum(&buf);
        if expect_checksum != checksum {
            let reason = "the checksums of the footer doesn't match".to_owned();
            return Err(EikvError::SstCorrpution(reason));
        }

        let footer = Footer::decode(&buf[..footer_size])?;
        Ok(footer)
    }
}
