mod mem_table;
mod write_queue;

use self::{mem_table::MemTable, write_queue::WriteQueue};
use crate::{model::Entry, wal, DBOptions, EikvResult, Key, Value, WriteBatch};
use std::{mem, sync::atomic::AtomicU64};

pub(crate) struct Wal {
    file_seq: u64,
    writer: wal::Writer,
}

impl Wal {
    pub(crate) fn new(file_seq: u64, writer: wal::Writer) -> Wal {
        Wal { file_seq, writer }
    }
}

pub(crate) struct MemDB<K: Key, V: Value> {
    immut_wal: Option<Wal>,
    mem_table: MemTable<K, V>,
    mut_wal: Wal,
    options: DBOptions,
    write_queue: WriteQueue<K, V>,
}

impl<K: Key, V: Value> MemDB<K, V> {
    pub(crate) fn new(options: DBOptions, next_seq: AtomicU64, wal: Wal) -> MemDB<K, V> {
        MemDB {
            immut_wal: None,
            mem_table: MemTable::new(),
            mut_wal: wal,
            options,
            write_queue: WriteQueue::new(next_seq),
        }
    }

    pub(crate) fn get(&self, key: K) -> Option<Entry<K, V>> {
        self.mem_table.get(key, u64::MAX)
    }

    pub(crate) fn write(&mut self, write_batch: WriteBatch<K, V>) -> EikvResult<bool> {
        let write_batch = match self.write_queue.line_up(write_batch) {
            Some(write_batch) => write_batch,
            None => return Ok(false),
        };

        self.mem_table.update(&write_batch);
        self.mut_wal.writer.append(write_batch)?;
        let full = self.mut_wal.writer.file_size()? > self.options.wal_size_limit;
        Ok(full)
    }

    pub(crate) fn freeze(&mut self, mut wal: Wal) {
        self.mem_table.freeze();
        mem::swap(&mut wal, &mut self.mut_wal);
        self.immut_wal = Some(wal);
    }
}
