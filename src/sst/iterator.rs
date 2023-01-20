use prost::Message;

use super::cache::Cache;
use crate::{
    proto::sst::{DataBlock, DataBlockPayload, Entry, EntryGroup},
    util::{
        checksum::crc32_checksum,
        coding::{decode_fixed_u32, decode_fixed_u64},
    },
    Compressor, EikvError, EikvResult, FilterFactory,
};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    sync::Arc,
};

pub(crate) struct IteratorOptions {
    pub(crate) block_size: usize,
    pub(crate) cache: Arc<Cache>,
    pub(crate) compressor: Option<Arc<dyn Compressor>>,
    pub(crate) filter_factory: Option<Arc<dyn FilterFactory>>,
}

pub(crate) struct Iterator {
    options: IteratorOptions,
    file: File,
    index_block_offset: u64,
    index_block: Vec<u64>,
    index_block_index: usize,
    entries: Vec<Entry>,
    entry_index: usize,
}

impl Iterator {
    pub(crate) fn new(path: &str, options: IteratorOptions) -> EikvResult<Iterator> {
        let file = OpenOptions::new().read(true).open(path)?;
        let iterator = Iterator {
            options,
            file,
            index_block_offset: 0,
            index_block: vec![],
            index_block_index: 0,
            entries: vec![],
            entry_index: 0,
        };
        Ok(iterator)
    }

    fn read_index_block(&mut self) -> EikvResult<()> {
        let data_block_count = self.options.cache.footer.data_block_count;
        let block_size = self.options.block_size;
        let next_offset = self.index_block_offset + block_size as u64;
        let index_block_end = self.options.cache.footer.index_block_end;
        debug_assert!(next_offset <= index_block_end);

        let count_in_block = (block_size / 8) - 1;
        let count_in_block = if next_offset == index_block_end {
            data_block_count as usize % count_in_block
        } else {
            count_in_block
        };

        self.file.seek(SeekFrom::Start(self.index_block_offset))?;
        let mut index_block = vec![0; block_size];
        let n = self.file.read(&mut index_block)?;
        if n != index_block.len() {
            return Err(EikvError::SstCorrpution(
                "index block is complete".to_owned(),
            ));
        }

        self.index_block.clear();
        for i in 0..count_in_block {
            let block_offset = i * 8;
            let offset = decode_fixed_u64(&index_block[block_offset..block_offset + 8]);
            self.index_block.push(offset);
        }
        self.index_block_index = 0;

        Ok(())
    }

    fn read_data_block(
        &mut self,
        data_block_offset: u64,
        data_block_size: usize,
    ) -> EikvResult<()> {
        self.file.seek(SeekFrom::Start(data_block_offset))?;
        let mut data_block = vec![0; data_block_size];
        let n = self.file.read(&mut data_block)?;
        if n != data_block.len() {
            return Err(EikvError::SstCorrpution(
                "data block is complete".to_owned(),
            ));
        }

        let checksum = decode_fixed_u32(&data_block[data_block_size - 4..]);
        let expect_checksum = crc32_checksum(&data_block[..data_block_size - 4]);
        if expect_checksum != checksum {
            return Err(EikvError::ChecksumError {
                owner: "data block",
            });
        }

        let data_block = DataBlock::decode(&data_block[..data_block_size - 4])?;
        let mut payload = DataBlockPayload::decode(data_block.payload.as_slice())?;
        let entrygroups = match &self.options.compressor {
            Some(compressor) => compressor.uncompress(&payload.entrygroups)?,
            None => payload.entrygroups,
        };
        payload.restart_points.push(entrygroups.len() as u32);

        self.entries.clear();
        let mut prev_user_key = vec![];
        for i in 0..payload.restart_points.len() - 1 {
            let start = payload.restart_points[i] as usize;
            let end = payload.restart_points[i + 1] as usize;
            let mut entry_group = EntryGroup::decode(&entrygroups[start..end])?;

            for entry in entry_group.entries.iter_mut() {
                if entry.shared_len == 0 {
                    prev_user_key = entry.unshared_key.clone();
                    continue;
                }

                let mut user_key = prev_user_key;
                user_key.truncate(entry.shared_len as usize);
                user_key.extend(&entry.unshared_key);
                entry.shared_len = 0;
                entry.unshared_key = user_key.clone();
                prev_user_key = user_key;
            }
            self.entries.extend(entry_group.entries);
        }
        self.entry_index = 0;

        Ok(())
    }

    pub(crate) fn seek_to_first(&mut self) -> EikvResult<()> {
        self.index_block_offset = self.options.cache.footer.index_block_start;
        self.read_index_block()?;

        let data_block_offset = self.index_block[0];
        let footer = &self.options.cache.footer;
        let next_block_offset = if footer.data_block_count == 1 {
            footer.data_block_end
        } else {
            self.index_block[1]
        };
        let data_block_size = (next_block_offset - data_block_offset) as usize;
        self.read_data_block(data_block_offset, data_block_size)?;

        Ok(())
    }

    pub(crate) fn next(&mut self) -> EikvResult<Option<Entry>> {
        if self.entry_index < self.entries.len() {
            let entry = self.entries[self.entry_index].clone();
            self.entry_index += 1;
            return Ok(Some(entry));
        }

        if self.index_block_index < self.index_block.len() - 1 {
            let data_block_offset = self.index_block[self.index_block_index];
            let next_block_offset = self.index_block[self.index_block_index + 1];
            let data_block_size = (next_block_offset - data_block_offset) as usize;
            self.read_data_block(data_block_offset, data_block_size)?;
            self.index_block_index += 1;
            let entry = self.entries[0].clone();
            return Ok(Some(entry));
        }

        let index_block_end = self.options.cache.footer.index_block_end;
        let block_size = self.options.block_size as u64;
        if self.index_block_offset == index_block_end - block_size {
            return Ok(None);
        }

        self.index_block_offset += block_size;
        let data_block_offset = self.index_block[self.index_block.len() - 1];
        self.read_index_block()?;
        let data_block_size = (self.index_block[0] - data_block_offset) as usize;
        self.read_data_block(data_block_offset, data_block_size)?;
        let entry = self.entries[0].clone();
        Ok(Some(entry))
    }
}
