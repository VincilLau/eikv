mod mem_table;
mod write_queue;

use self::{mem_table::MemTable, write_queue::WriteQueue};
use crate::{model::Entry, sst, wal::Writer, DBOptions, EikvResult, Key, Value, WriteBatch};
pub(crate) use mem_table::Table;
use std::{
    mem,
    sync::{atomic::AtomicU64, Condvar, Mutex},
    time::Duration,
};

pub(crate) struct MemDB<K: Key, V: Value> {
    immut_wal: Mutex<Option<Writer>>,
    mem_table: MemTable<K, V>,
    mut_wal: Mutex<Writer>,
    options: DBOptions,
    write_queue: WriteQueue<K, V>,
    minor_compaction: Condvar,
    has_immut: Condvar,
}

impl<K: Key, V: Value> MemDB<K, V> {
    pub(crate) fn new(options: DBOptions, next_seq: AtomicU64, mut_wal: Writer) -> MemDB<K, V> {
        MemDB {
            immut_wal: Mutex::new(None),
            mem_table: MemTable::new(),
            mut_wal: Mutex::new(mut_wal),
            options,
            write_queue: WriteQueue::new(next_seq),
            minor_compaction: Condvar::new(),
            has_immut: Condvar::new(),
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
        let mut immut_wal = self.immut_wal.lock().unwrap();
        while immut_wal.is_some() {
            immut_wal = self.minor_compaction.wait(immut_wal).unwrap();
        }

        self.mem_table.freeze();
        mem::swap(&mut wal, &mut self.mut_wal.lock().unwrap());
        *immut_wal = Some(wal);
        self.has_immut.notify_one();
    }

    pub(crate) fn write_finished(&self) {
        self.write_queue.notify_waiters();
    }

    pub(crate) fn has_immut(&self) -> bool {
        let immut_wal = self.immut_wal.lock().unwrap();
        immut_wal.is_some()
    }

    pub(crate) fn wait_immut(&self) -> bool {
        let mut immut_wal = self.immut_wal.lock().unwrap();
        while immut_wal.is_none() {
            let res = self
                .has_immut
                .wait_timeout(immut_wal, Duration::from_secs(1))
                .unwrap();
            if res.1.timed_out() {
                return true;
            }
            immut_wal = res.0;
        }
        false
    }

    pub(crate) fn dump(&self, writer: sst::Writer<K, V>) -> EikvResult<()> {
        let mut immut_wal = self.immut_wal.lock().unwrap();
        self.mem_table.dump(writer)?;
        *immut_wal = None;
        Ok(())
    }
}
