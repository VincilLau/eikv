use crate::{model::SstMeta, util::coding::decode_fixed_u64, EikvError, EikvResult, Key, Value};
use std::{
    cmp::min,
    fs::File,
    io::{Read, Seek, SeekFrom},
};

pub(super) struct Iterator {
    block_size: usize,
    data_block_count: u32,
    data_block_end: u64,
    index_block_start: u64,
    index_block_end: u64,
    index_block_offset: u64,
    index_block: Vec<u64>,
    index_block_index: usize,
}

impl Iterator {
    pub(super) fn new<K: Key, V: Value>(sst_meta: SstMeta<K, V>) -> Iterator {
        Iterator {
            block_size: sst_meta.block_size,
            data_block_count: sst_meta.data_block_count,
            data_block_end: sst_meta.data_block_end,
            index_block_start: sst_meta.index_block_start,
            index_block_end: sst_meta.index_block_end,
            index_block_offset: sst_meta.index_block_start,
            index_block: vec![],
            index_block_index: 0,
        }
    }

    pub(super) fn seek_to_first(&mut self, file: &mut File) -> EikvResult<()> {
        file.seek(SeekFrom::Start(self.index_block_start))?;
        let mut block = vec![0; self.block_size];
        let n = file.read(&mut block)?;
        if n != self.block_size {
            let reason = format!("index block size is {}, read {} bytes", self.block_size, n);
            return Err(EikvError::SstCorrpution(reason));
        }

        let offset_count = self.block_size / 8 - 1;
        let offset_count = min(self.data_block_count as usize, offset_count);
        self.index_block.clear();
        self.index_block.reserve(offset_count);
        for i in 0..offset_count {
            let buf_off = i * 8;
            let offset = decode_fixed_u64(&block[buf_off..buf_off + 8]);
            self.index_block.push(offset);
        }

        self.index_block_offset = self.index_block_start;
        self.index_block_index = 0;
        Ok(())
    }

    pub(super) fn next(&mut self, file: &mut File) -> EikvResult<Option<(u64, u64)>> {
        if self.index_block_index == self.index_block.len() {
            return Ok(None);
        }

        if self.index_block_index < self.index_block.len() - 1 {
            let start = self.index_block[self.index_block_index];
            self.index_block_index += 1;
            let end = self.index_block[self.index_block_index];
            return Ok(Some((start, end)));
        }

        let start = self.index_block[self.index_block_index];
        if self.index_block_offset + self.block_size as u64 == self.index_block_end {
            self.index_block_index += 1;
            let end = self.data_block_end;
            return Ok(Some((start, end)));
        }

        self.index_block_offset += self.block_size as u64;
        file.seek(SeekFrom::Start(self.index_block_offset))?;
        let mut block = vec![0; self.block_size];
        let n = file.read(&mut block)?;
        if n != self.block_size {
            let reason = format!("index block size is {}, read {} bytes", self.block_size, n);
            return Err(EikvError::SstCorrpution(reason));
        }

        let offset_count = self.block_size / 8 - 1;
        let offset_count =
            if self.index_block_offset + self.block_size as u64 == self.index_block_end {
                self.data_block_count as usize % offset_count
            } else {
                offset_count
            };
        self.index_block.clear();
        self.index_block.reserve(offset_count);
        for i in 0..offset_count {
            let buf_off = i * 8;
            let offset = decode_fixed_u64(&block[buf_off..buf_off + 8]);
            self.index_block.push(offset);
        }

        self.index_block_index = 0;
        let end = self.index_block[0];
        return Ok(Some((start, end)));
    }
}
