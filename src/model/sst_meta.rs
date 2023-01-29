use std::fs::metadata;

use super::Entry;
use crate::{sst::Footer, EikvResult, Key, Value};

#[derive(Clone)]
pub(crate) struct SstMeta<K: Key, V: Value> {
    pub(crate) file_size: u64,
    pub(crate) block_size: usize,
    pub(crate) data_block_count: u32,
    pub(crate) data_block_end: u64,
    pub(crate) index_block_start: u64,
    pub(crate) index_block_end: u64,
    pub(crate) min_entry: Entry<K, V>,
    pub(crate) max_entry: Entry<K, V>,
}

impl<K: Key, V: Value> SstMeta<K, V> {
    pub(crate) fn new(path: &str, block_size: usize) -> EikvResult<SstMeta<K, V>> {
        let footer = Footer::load(path)?;

        let padding_size = if footer.data_block_end % block_size as u64 == 0 {
            0
        } else {
            let block_size = block_size as u64;
            block_size - footer.data_block_end % block_size
        };
        let index_block_start = footer.data_block_end + padding_size;

        let offset_count_one_block = block_size as usize / 8 - 1;
        let index_block_count = (footer.data_block_count as usize + offset_count_one_block - 1)
            / offset_count_one_block;
        let index_block_end = index_block_start + index_block_count as u64 * block_size as u64;

        let file_size = metadata(path)?.len();

        let sst_meta = SstMeta {
            block_size,
            data_block_count: footer.data_block_count,
            data_block_end: footer.data_block_end,
            index_block_start,
            index_block_end,
            min_entry: footer.min_entry,
            max_entry: footer.max_entry,
            file_size,
        };
        Ok(sst_meta)
    }
}
