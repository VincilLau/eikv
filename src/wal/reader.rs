use crate::{
    proto::wal::WriteBatch as ProtoWriteBatch,
    util::{
        checksum::crc32_checksum,
        coding::{decode_fixed_u32, decode_fixed_u64},
    },
    EikvError, EikvResult, WriteBatch,
};
use prost::Message;
use std::{
    fs::{File, OpenOptions},
    io::Read,
};

pub(crate) struct Reader {
    file: File,
}

impl Reader {
    pub(crate) fn open(path: &str) -> EikvResult<(Reader, u64)> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut buf = [0; 8];
        let n = file.read(&mut buf)?;
        if n != 8 {
            return Err(EikvError::WalCorrpution(
                "the seq of wal is incomplete".to_owned(),
            ));
        }
        let seq = decode_fixed_u64(&buf);
        let reader = Reader { file };
        Ok((reader, seq))
    }

    pub(crate) fn next(&mut self) -> EikvResult<Option<WriteBatch>> {
        let mut buf = [0; 8];
        let n = self.file.read(&mut buf)?;
        if n == 0 {
            return Ok(None);
        } else if n < 8 {
            return Err(EikvError::WalCorrpution(
                "write batch header is incomplete".to_owned(),
            ));
        }

        let checksum = decode_fixed_u32(&buf[..4]);
        let len = decode_fixed_u32(&buf[4..]) as usize;

        let mut wb_buf = vec![0; len];
        wb_buf[4..8].copy_from_slice(&buf[4..]);
        let n = self.file.read(&mut wb_buf[8..])?;
        if n != wb_buf.len() - 8 {
            return Err(EikvError::WalCorrpution(
                "write batch is incomplete".to_owned(),
            ));
        }

        let expect_checksum = crc32_checksum(&wb_buf);
        if checksum != expect_checksum {
            return Err(EikvError::ChecksumError {
                owner: "write batch",
            });
        }

        let pwb = ProtoWriteBatch::decode(&wb_buf[8..])?;
        let wb = WriteBatch { pwb };

        Ok(Some(wb))
    }
}
