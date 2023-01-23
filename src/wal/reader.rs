use crate::{util::coding::decode_fixed_u32, EikvError, EikvResult, Key, Value, WriteBatch};
use std::{fs::File, io::Read};

pub(crate) struct Reader {
    file: File,
}

impl Reader {
    pub(crate) fn open(path: &str) -> EikvResult<Reader> {
        let file = File::open(path)?;
        let reader = Reader { file };
        Ok(reader)
    }

    pub(crate) fn next<K: Key, V: Value>(&mut self) -> EikvResult<Option<WriteBatch<K, V>>> {
        let mut buf = [0; 8];
        let n = self.file.read(&mut buf)?;
        if n == 0 {
            return Ok(None);
        } else if n < 8 {
            let reason = format!("the size of write batch header is 8, read {} bytes", n);
            return Err(EikvError::WalCorrpution(reason));
        }

        let checksum = decode_fixed_u32(&buf[..4]);
        let len = decode_fixed_u32(&buf[4..]) as usize;

        let mut wb_buf = vec![0; len];
        wb_buf[4..8].copy_from_slice(&buf[4..]);
        let n = self.file.read(&mut wb_buf[8..])?;
        if n != len - 8 {
            let reason = format!("the size of write batch is {}, read {} bytes", len - 8, n);
            return Err(EikvError::WalCorrpution(reason));
        }

        let write_batch = WriteBatch::decode(&wb_buf[8..], checksum)?;
        Ok(Some(write_batch))
    }
}
