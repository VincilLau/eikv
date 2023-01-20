use prost::Message;

use crate::{
    proto::sst::Footer,
    util::{
        checksum::crc32_checksum,
        coding::{decode_fixed_u32, decode_fixed_u64},
    },
    EikvError, EikvResult,
};
use std::{
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom},
};

pub(crate) struct Cache {
    pub(crate) footer: Footer,
    pub(crate) index_block_sampling: Vec<u64>,
}

impl Cache {
    pub(crate) fn load(path: &str, block_size: usize) -> EikvResult<Cache> {
        let footer = Cache::load_footer(path)?;
        let sampling = Cache::load_sampling(path, block_size, &footer)?;
        let cache = Cache {
            footer,
            index_block_sampling: sampling,
        };
        Ok(cache)
    }

    fn load_footer(path: &str) -> EikvResult<Footer> {
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
            return Err(EikvError::SstCorrpution("footer is incomplete".to_owned()));
        }
        let expect_checksum = crc32_checksum(&buf);
        if expect_checksum != checksum {
            return Err(EikvError::ChecksumError { owner: "footer" });
        }

        let footer = Footer::decode(&buf[..footer_size])?;
        Ok(footer)
    }

    fn load_sampling(path: &str, block_size: usize, footer: &Footer) -> EikvResult<Vec<u64>> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let index_block_count =
            (footer.index_block_end - footer.index_block_start) % block_size as u64;
        let mut index_block = vec![0; block_size];
        let mut sampling = vec![];
        sampling.reserve(index_block_count as usize);

        for i in 0..index_block_count {
            let index_block_offset = footer.index_block_start + i * index_block_count;
            file.seek(SeekFrom::Start(index_block_offset))?;
            let n = file.read(&mut index_block)?;
            if n != block_size {
                return Err(EikvError::SstCorrpution(
                    "index block is incomplete".to_owned(),
                ));
            }
            let checksum = decode_fixed_u32(&index_block[block_size - 4..]);
            let expect_checksum = crc32_checksum(&index_block[..block_size - 4]);
            if expect_checksum != checksum {
                return Err(EikvError::ChecksumError {
                    owner: "index block",
                });
            }
            sampling.push(decode_fixed_u64(&index_block[..8]));
        }

        Ok(sampling)
    }
}
