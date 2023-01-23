use crate::{model::Entry, Key, Value, WriteBatch};
use std::{
    collections::BTreeSet,
    mem,
    sync::{Arc, Mutex, RwLock},
};

type Table<K, V> = BTreeSet<Entry<K, V>>;

pub(super) struct MemTable<K: Key, V: Value> {
    mut_table: Mutex<Table<K, V>>,
    immut_table: RwLock<Arc<Table<K, V>>>,
}

impl<K: Key, V: Value> MemTable<K, V> {
    pub(super) fn new() -> MemTable<K, V> {
        MemTable {
            mut_table: Mutex::new(Table::new()),
            immut_table: RwLock::new(Arc::new(Table::new())),
        }
    }

    pub(super) fn update(&mut self, write_batch: &WriteBatch<K, V>) {
        let mut guard = self.mut_table.lock().unwrap();
        for entry in write_batch.entries() {
            guard.insert(entry.clone());
        }
    }

    pub(super) fn freeze(&mut self) {
        let mut mut_table = self.mut_table.lock().unwrap();
        let mut immut_table = self.immut_table.write().unwrap();
        let mut tmp_table = Table::new();
        mem::swap(&mut *mut_table, &mut tmp_table);
        *immut_table = Arc::new(tmp_table);
    }

    pub(super) fn get(&self, key: K, seq_guard: u64) -> Option<Entry<K, V>> {
        let max_entry = Entry {
            key,
            seq: seq_guard,
            value: None,
        };

        {
            let mut_table = self.mut_table.lock().unwrap();
            if let Some(entry) = mut_table.range(..=&max_entry).next_back() {
                return Some(entry.clone());
            }
        }

        let immut_table = { self.immut_table.read().unwrap().clone() };
        match immut_table.range(..=&max_entry).next_back() {
            Some(entry) => Some(entry.clone()),
            None => None,
        }
    }
}
