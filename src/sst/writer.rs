use super::{data_block::Builder, Footer};
use crate::{
    model::Entry,
    util::{
        checksum::crc32_checksum,
        coding::{append_fixed_u32, append_fixed_u64},
    },
    DBOptions, EikvResult, Key, Value,
};
use std::{
    fs::{File, OpenOptions},
    io::{Seek, Write},
    mem::swap,
};

pub(crate) struct Writer<K: Key, V: Value> {
    options: DBOptions,
    file: File,
    block_builder: Builder<K, V>,
    block_offsets: Vec<u64>,
    size_limit: u64,
    min_entry: Option<Entry<K, V>>,
    max_entry: Option<Entry<K, V>>,
}

impl<K: Key, V: Value> Writer<K, V> {
    pub(crate) fn new(path: &str, options: DBOptions, size_limit: u64) -> EikvResult<Writer<K, V>> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let writer = Writer {
            options: options.clone(),
            file,
            block_builder: Builder::new(options),
            block_offsets: vec![],
            size_limit,
            min_entry: None,
            max_entry: None,
        };
        Ok(writer)
    }

    pub(crate) fn append(&mut self, entry: Entry<K, V>) -> EikvResult<()> {
        if self.block_builder.full() {
            let offset = self.file.stream_position()?;
            self.block_offsets.push(offset);

            let mut block_builder = Builder::new(self.options.clone());
            swap(&mut self.block_builder, &mut block_builder);

            let buf = block_builder.build()?;
            self.file.write(&buf)?;
        }

        if self.min_entry.is_none() {
            self.min_entry = Some(entry.clone());
        }
        self.max_entry = Some(entry.clone());

        self.block_builder.append(entry)
    }

    pub(crate) fn full(&mut self) -> EikvResult<bool> {
        let offset = self.file.stream_position()?;
        Ok(offset >= self.size_limit)
    }

    fn build_index_block(&mut self, data_block_end: u64) -> EikvResult<u64> {
        debug_assert_eq!(self.options.block_size % 8, 0);

        let block_size = self.options.block_size as u64;
        let index_block_start = if data_block_end % block_size == 0 {
            data_block_end
        } else {
            let padding_size = (block_size - data_block_end % block_size) as usize;
            let padding = vec![0; padding_size];
            self.file.write(&padding)?;
            data_block_end + padding_size as u64
        };

        let offset_count_one_block = self.options.block_size / 8 - 1;
        let index_block_count =
            (self.block_offsets.len() + offset_count_one_block - 1) / offset_count_one_block;

        let mut index_block = vec![];
        index_block.reserve(self.options.block_size);

        for i in 0..index_block_count {
            index_block.clear();

            for j in 0..offset_count_one_block {
                let k = i * offset_count_one_block + j;
                let offset = if k < self.block_offsets.len() {
                    self.block_offsets[k]
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

    pub(crate) fn finish(mut self) -> EikvResult<()> {
        let offset = self.file.stream_position()?;
        self.block_offsets.push(offset);

        let mut block_builder = Builder::new(self.options.clone());
        swap(&mut self.block_builder, &mut block_builder);
        let buf = block_builder.build()?;
        self.file.write(&buf)?;

        let data_block_end = self.file.stream_position()?;
        self.build_index_block(data_block_end)?;

        let footer = Footer {
            min_entry: self.min_entry.unwrap(),
            max_entry: self.max_entry.unwrap(),
            data_block_end,
            data_block_count: self.block_offsets.len() as u32,
        };
        let mut buf = vec![];
        footer.encode(&mut buf)?;
        let footer_size = buf.len() as u32;
        append_fixed_u32(&mut buf, footer_size);
        let checksum = crc32_checksum(&buf);
        append_fixed_u32(&mut buf, checksum);
        self.file.write(&buf)?;

        Ok(())
    }
}
