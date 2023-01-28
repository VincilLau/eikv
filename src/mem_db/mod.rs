mod mem_table;
mod write_queue;

use self::{mem_table::MemTable, write_queue::WriteQueue};
use crate::{model::Entry, wal::Writer, DBOptions, EikvResult, Key, Value, WriteBatch};
pub(crate) use mem_table::Table;
use std::{
    mem,
    sync::{atomic::AtomicU64, Mutex},
};

pub(crate) struct MemDB<K: Key, V: Value> {
    immut_wal: Mutex<Option<Writer>>,
    mem_table: MemTable<K, V>,
    mut_wal: Mutex<Writer>,
    options: DBOptions,
    write_queue: WriteQueue<K, V>,
}

impl<K: Key, V: Value> MemDB<K, V> {
    pub(crate) fn new(options: DBOptions, next_seq: AtomicU64, mut_wal: Writer) -> MemDB<K, V> {
        MemDB {
            immut_wal: Mutex::new(None),
            mem_table: MemTable::new(),
            mut_wal: Mutex::new(mut_wal),
            options,
            write_queue: WriteQueue::new(next_seq),
        }
    }

    pub(crate) fn get(&self, key: K) -> Option<Entry<K, V>> {
        self.mem_table.get(key, u64::MAX)
    }

    pub(crate) fn write(&self, write_batch: WriteBatch<K, V>) -> EikvResult<bool> {
        let write_batch = match self.write_queue.line_up(write_batch) {
            Some(write_batch) => write_batch,
            None => return Ok(false),
        };

        self.mem_table.update(&write_batch);
        self.mut_wal.lock().unwrap().append(write_batch)?;
        let full = self.mut_wal.lock().unwrap().file_offset()? > self.options.wal_size_limit;
        Ok(full)
    }

    pub(crate) fn recover_mut_table(&mut self, table: Table<K, V>) {
        self.mem_table.recover_mut_table(table);
    }

    pub(crate) fn recover_immut_table(&mut self, table: Table<K, V>) {
        self.mem_table.recover_immut_table(table);
    }

    pub(crate) fn freeze(&self, mut wal: Writer) {
        self.mem_table.freeze();
        mem::swap(&mut wal, &mut self.mut_wal.lock().unwrap());
        *self.immut_wal.lock().unwrap() = Some(wal);
    }

    pub(crate) fn write_finished(&self) {
        self.write_queue.notify_waiters();
    }
}
