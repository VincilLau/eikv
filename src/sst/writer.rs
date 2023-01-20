use crate::{
    proto::sst::{DataBlock, DataBlockPayload, Entry, EntryGroup, Footer},
    util::{
        checksum::crc32_checksum,
        coding::{append_fixed_u32, append_fixed_u64},
    },
    Compressor, EikvResult, Filter, FilterFactory,
};
use prost::Message;
use std::{
    cmp::min,
    fs::{File, OpenOptions},
    io::{Seek, Write},
    mem::swap,
    sync::Arc,
};

struct DataBlockBuilder {
    options: WriterOptions,
    entry_group: EntryGroup,
    payload: DataBlockPayload,
    restart_index: usize,
    min_user_key: Option<Vec<u8>>,
    prev_user_key: Option<Vec<u8>>,
    filter: Option<Box<dyn Filter>>,
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

impl DataBlockBuilder {
    fn new(options: WriterOptions) -> DataBlockBuilder {
        let filter = match &options.filter_factory {
            Some(filter_factory) => Some(filter_factory.create()),
            None => None,
        };

        DataBlockBuilder {
            options,
            entry_group: EntryGroup { entries: vec![] },
            payload: DataBlockPayload {
                entrygroups: vec![],
                restart_points: vec![],
            },
            restart_index: 0,
            min_user_key: None,
            prev_user_key: None,
            filter,
        }
    }

    fn is_empty(&self) -> bool {
        self.min_user_key.is_none()
    }

    fn append(&mut self, mut entry: Entry) -> EikvResult<bool> {
        debug_assert_eq!(entry.shared_len, 0);

        if self.min_user_key.is_none() {
            self.min_user_key = Some(entry.unshared_key.clone());
        }

        if self.restart_index == 0 {
            self.payload
                .restart_points
                .push(self.payload.entrygroups.len() as u32);
            self.prev_user_key = None;
        }
        self.restart_index = (self.restart_index + 1) % self.options.restart_interval;
        if self.restart_index == 0 {
            let mut buf = vec![];
            buf.reserve(self.entry_group.encoded_len());
            self.entry_group.encode(&mut buf)?;
            self.payload.entrygroups.extend(buf);
            self.entry_group.entries.clear();
        }

        let user_key = match &self.prev_user_key {
            Some(prev_user_key) => {
                let shared_len = shared_len(&entry.unshared_key, &prev_user_key);
                entry.shared_len = shared_len as u32;
                let user_key = entry.unshared_key;
                entry.unshared_key = user_key[shared_len..].to_vec();
                self.entry_group.entries.push(entry);
                user_key
            }
            None => {
                self.entry_group.entries.push(entry.clone());
                entry.unshared_key
            }
        };

        if let Some(filter) = &mut self.filter {
            filter.add(&user_key);
        }
        self.prev_user_key = Some(user_key);

        let full = self.payload.entrygroups.len() >= self.options.block_size;
        Ok(full)
    }

    fn build(mut self, buf: &mut Vec<u8>) -> EikvResult<()> {
        let mut entry_group_buf = vec![];
        entry_group_buf.reserve(self.entry_group.encoded_len());
        self.entry_group.encode(&mut entry_group_buf)?;
        self.payload.entrygroups.extend(entry_group_buf);
        self.entry_group.entries.clear();

        let old_len = buf.len();

        let mut payload = vec![];
        payload.reserve(self.payload.encoded_len());
        self.payload.encode(&mut payload)?;
        let payload = match self.options.compressor {
            Some(compressor) => compressor.compress(&payload)?,
            None => payload,
        };

        let filter = match self.filter {
            Some(filter) => {
                let mut buf = vec![];
                filter.append_to(&mut buf)?;
                Some(buf)
            }
            None => None,
        };

        let data_block = DataBlock {
            payload,
            min_user_key: self.min_user_key.unwrap(),
            filter,
        };

        buf.reserve(data_block.encoded_len());
        data_block.encode(buf)?;

        let checksum = crc32_checksum(&buf[old_len..]);
        append_fixed_u32(buf, checksum);

        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct WriterOptions {
    pub(crate) block_size: usize,
    pub(crate) restart_interval: usize,
    pub(crate) compressor: Option<Arc<dyn Compressor>>,
    pub(crate) filter_factory: Option<Arc<dyn FilterFactory>>,
}

pub(crate) struct Writer {
    options: WriterOptions,
    file: File,
    data_block_builder: DataBlockBuilder,
    data_block_offsets: Vec<u64>,
}

impl Writer {
    pub(crate) fn new(path: &str, options: WriterOptions) -> EikvResult<Writer> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let writer = Writer {
            options: options.clone(),
            file,
            data_block_builder: DataBlockBuilder::new(options),
            data_block_offsets: vec![],
        };
        Ok(writer)
    }

    pub(crate) fn append(&mut self, entry: Entry) -> EikvResult<()> {
        if self.data_block_builder.append(entry)? {
            let offset = self.file.stream_position()?;
            self.data_block_offsets.push(offset);

            let mut buf = vec![];
            let mut data_block_builder = DataBlockBuilder::new(self.options.clone());
            swap(&mut self.data_block_builder, &mut data_block_builder);
            data_block_builder.build(&mut buf)?;
            self.file.write(&buf)?;
        }

        Ok(())
    }

    pub(super) fn stream_position(&mut self) -> EikvResult<u64> {
        let pos = self.file.stream_position()?;
        Ok(pos)
    }

    fn build_index_block(&mut self, data_block_end: u64) -> EikvResult<u64> {
        debug_assert_eq!(self.options.block_size % 8, 0);

        let block_size = self.options.block_size as u64;
        let index_block_start = if data_block_end % block_size != 0 {
            (data_block_end / block_size + 1) * block_size
        } else {
            data_block_end
        };
        let padding_size = (index_block_start - data_block_end) as usize;
        let padding = vec![0; padding_size];
        self.file.write(&padding)?;

        let offset_count_one_block = self.options.block_size / 8 - 1;
        let index_block_count =
            (self.data_block_offsets.len() + offset_count_one_block - 1) / offset_count_one_block;
        let mut index_block = vec![];
        index_block.reserve(self.options.block_size);
        for i in 0..index_block_count {
            index_block.clear();
            for j in 0..offset_count_one_block {
                let k = i * offset_count_one_block + j;
                let offset = if k < self.data_block_offsets.len() {
                    self.data_block_offsets[k]
                } else {
                    0
                };
                append_fixed_u64(&mut index_block, offset);
            }
            append_fixed_u32(&mut index_block, 0);
            let checksum = crc32_checksum(&index_block);
            append_fixed_u32(&mut index_block, checksum);
            self.file.write(&index_block)?;
        }

        Ok(index_block_start)
    }

    pub(crate) fn finish(mut self, min_user_key: Vec<u8>, max_user_key: Vec<u8>) -> EikvResult<()> {
        if !self.data_block_builder.is_empty() {
            let offset = self.file.stream_position()?;
            self.data_block_offsets.push(offset);

            let mut buf = vec![];
            let mut data_block_builder = DataBlockBuilder::new(self.options.clone());
            swap(&mut self.data_block_builder, &mut data_block_builder);
            data_block_builder.build(&mut buf)?;
            self.file.write(&buf)?;
        }

        let data_block_end = self.file.stream_position()?;
        let index_block_start = self.build_index_block(data_block_end)?;
        let index_block_end = self.file.stream_position()?;

        let footer = Footer {
            data_block_end,
            data_block_count: self.data_block_offsets.len() as u32,
            index_block_start,
            index_block_end,
            min_user_key,
            max_user_key,
        };
        let mut buf = vec![];
        buf.reserve(footer.encoded_len() + 8);
        footer.encode(&mut buf)?;
        let footer_size = buf.len() as u32;
        append_fixed_u32(&mut buf, footer_size);
        let checksum = crc32_checksum(&buf);
        append_fixed_u32(&mut buf, checksum);
        self.file.write(&buf)?;

        Ok(())
    }
}
