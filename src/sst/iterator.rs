use crate::{
    model::{Entry, SstMeta},
    DBOptions, EikvError, EikvResult, Key, Value,
};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    sync::Arc,
};

use super::{data_block::decode_block, index_block};

pub(crate) struct Iterator<K: Key, V: Value> {
    entries: Vec<Entry<K, V>>,
    entry_index: usize,
    file: File,
    index_block_iterator: index_block::Iterator,
    options: DBOptions,
}

impl<K: Key, V: Value> Iterator<K, V> {
    pub(crate) fn new(
        path: &str,
        options: DBOptions,
        sst_meta: SstMeta<K, V>,
    ) -> EikvResult<Iterator<K, V>> {
        let file = OpenOptions::new().read(true).open(path)?;
        let index_block_iterator = index_block::Iterator::new(sst_meta);
        let iterator = Iterator {
            entry_index: 0,
            entries: vec![],
            file,
            index_block_iterator,
            options,
        };
        Ok(iterator)
    }

    fn next_block(&mut self) -> EikvResult<()> {
        let data_block_pos = match self.index_block_iterator.next(&mut self.file)? {
            Some(data_block_pos) => data_block_pos,
            None => {
                self.entry_index += 1;
                return Ok(());
            }
        };

        let start = data_block_pos.0;
        let block_size = (data_block_pos.1 - data_block_pos.0) as usize;
        let mut block = vec![0; block_size];
        self.file.seek(SeekFrom::Start(start))?;
        let n = self.file.read(&mut block)?;
        if n != block_size {
            let reason = format!("data block size is {}, read {} bytes", block_size, n);
            return Err(EikvError::SstCorrpution(reason));
        }

        let has_filter = self.options.filter_factory.is_some();
        self.entries = decode_block(&block, self.options.compressor.clone(), has_filter)?;
        self.entry_index = 0;
        Ok(())
    }

    pub(crate) fn seek_to_first(&mut self) -> EikvResult<()> {
        self.index_block_iterator.seek_to_first(&mut self.file)?;
        self.next_block()?;
        Ok(())
    }

    pub(crate) fn entry(&self) -> Option<&Entry<K, V>> {
        if self.entries.len() == self.entry_index {
            None
        } else {
            Some(&self.entries[self.entry_index])
        }
    }

    pub(crate) fn next(&mut self) -> EikvResult<()> {
        if self.entry_index == self.entries.len() {
            return Ok(());
        }
        if self.entry_index < self.entries.len() - 1 {
            self.entry_index += 1;
            return Ok(());
        }
        self.next_block()?;
        Ok(())
    }
}
