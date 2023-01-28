use super::{Iterator, Writer};
use crate::{model::Entry, util::time::unix_now, DBOptions, EikvResult, Key, Value};
use std::cmp::{min, Ordering};

pub(crate) struct Merger<K: Key, V: Value> {
    iterators: Vec<Iterator<K, V>>,
    options: DBOptions,
    seq_guard: u64,
    time_limit: usize,
    writer: Writer<K, V>,
}

pub(crate) enum MergeResult {
    Timeout,
    Full,
    Finish,
}

impl<K: Key, V: Value> Merger<K, V> {
    pub(crate) fn new(
        path: &str,
        iterators: Vec<Iterator<K, V>>,
        options: DBOptions,
        seq_guard: u64,
        size_limit: u64,
        time_limit: usize,
    ) -> EikvResult<Merger<K, V>> {
        let writer = Writer::new(path, options.clone(), size_limit)?;
        let merger = Merger {
            iterators,
            options,
            seq_guard,
            time_limit,
            writer,
        };
        Ok(merger)
    }

    fn finished(&self) -> bool {
        for iterator in &self.iterators {
            if iterator.entry().is_some() {
                return false;
            }
        }
        true
    }

    fn get_min_entry(&self) -> Entry<K, V> {
        debug_assert!(!self.finished());

        let mut min_entry = None;
        for iterator in &self.iterators {
            let entry = match iterator.entry() {
                Some(entry) => entry,
                None => continue,
            };
            min_entry = match min_entry {
                Some(min_entry) => Some(min(entry, min_entry)),
                None => Some(entry),
            }
        }

        min_entry.unwrap().clone()
    }

    fn read_some(&mut self) -> EikvResult<Vec<Entry<K, V>>> {
        let min_entry = self.get_min_entry();
        let mut last_before_guard = None;
        let mut entries = vec![];
        for iterator in self.iterators.iter_mut() {
            loop {
                let entry = match iterator.entry() {
                    Some(entry) => entry,
                    None => break,
                };
                if entry.key != min_entry.key {
                    break;
                }
                if entry.seq <= self.seq_guard {
                    last_before_guard = Some(entry.clone());
                } else {
                    entries.push(entry.clone());
                }
                iterator.next()?;
            }
        }

        if let Some(last_before_guard) = last_before_guard {
            entries.push(last_before_guard);
        }

        entries.sort_unstable();
        Ok(entries)
    }

    pub(crate) fn merge(&mut self) -> EikvResult<MergeResult> {
        let start_at = unix_now();
        loop {
            if self.finished() {
                return Ok(MergeResult::Finish);
            }

            let entries = self.read_some()?;
            debug_assert!(!entries.is_empty());
            for entry in entries {
                self.writer.append(entry)?;
            }

            let now = unix_now();
            if now - start_at > self.time_limit as u128 {
                return Ok(MergeResult::Timeout);
            }
        }
    }
}
