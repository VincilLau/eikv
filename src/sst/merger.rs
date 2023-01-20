use std::{
    cmp::Ordering,
    sync::Arc,
};

use super::{writer::WriterOptions, Iterator, Writer};
use crate::{proto::sst::Entry, util::time::unix_now, Comparator, EikvResult};

pub(crate) struct MergerOptions {
    pub(crate) writer_options: WriterOptions,
    pub(crate) time_limit: usize,
    pub(crate) file_size_limit: u64,
    pub(crate) version_guard: u64,
    pub(crate) comparator: Arc<dyn Comparator>,
    pub(crate) min_user_key: Vec<u8>,
    pub(crate) max_user_key: Vec<u8>,
}

pub(crate) struct Merger {
    options: MergerOptions,
    writer: Writer,
    iterators: Vec<Iterator>,
}

pub(crate) enum MergeResult {
    Timeout,
    Full,
    Finish,
}

impl Merger {
    pub(crate) fn new(
        path: &str,
        iterators: Vec<Iterator>,
        options: MergerOptions,
    ) -> EikvResult<Merger> {
        let writer = Writer::new(path, options.writer_options.clone())?;
        let merger = Merger {
            options,
            writer,
            iterators,
        };
        Ok(merger)
    }

    fn compare(&self, e1: &Entry, e2: &Entry) -> Ordering {
        let ordering = self
            .options
            .comparator
            .compare(&e1.unshared_key, &e2.unshared_key);
        match ordering {
            Ordering::Equal => e1.seq.cmp(&e2.seq),
            _ => ordering,
        }
    }

    fn finished(&self) -> bool {
        for iterator in &self.iterators {
            if iterator.entry().is_some() {
                return false;
            }
        }
        true
    }

    fn get_min_user_key(&self) -> Vec<u8> {
        assert!(!self.finished());
        let mut min_user_key = None;
        for iterator in &self.iterators {
            let entry = match iterator.entry() {
                Some(entry) => entry,
                None => continue,
            };
            min_user_key = match min_user_key {
                Some(min_user_key) => {
                    let min_user_key = if entry.unshared_key < min_user_key {
                        entry.unshared_key.clone()
                    } else {
                        min_user_key
                    };
                    Some(min_user_key)
                }
                None => Some(entry.unshared_key.clone()),
            }
        }
        min_user_key.unwrap()
    }

    fn read_some(&mut self) -> EikvResult<Vec<Entry>> {
        let min_user_key = self.get_min_user_key();
        let mut last_before_guard = None;
        let mut entries = vec![];
        for iterator in self.iterators.iter_mut() {
            loop {
                let entry = match iterator.entry() {
                    Some(entry) => entry,
                    None => break,
                };
                if entry.unshared_key != min_user_key {
                    break;
                }
                if entry.seq <= self.options.version_guard {
                    last_before_guard = Some(entry.clone());
                } else {
                    entries.push(entry.clone());
                }
                iterator.next()?;
            }
        }
        match last_before_guard {
            Some(last_before_guard) => entries.push(last_before_guard),
            None => (),
        }
        entries.sort_unstable_by(|e1, e2| e1.seq.cmp(&e2.seq));
        Ok(entries)
    }

    pub(crate) fn merge(&mut self) -> EikvResult<MergeResult> {
        let start_at = unix_now()?;
        loop {
            if self.finished() {
                self.writer.finish(
                    self.options.min_user_key.clone(),
                    self.options.max_user_key.clone(),
                )?;
                return Ok(MergeResult::Finish);
            }

            if self.writer.stream_position()? > self.options.file_size_limit {
                self.writer.finish(
                    self.options.min_user_key.clone(),
                    self.options.max_user_key.clone(),
                )?;
                return Ok(MergeResult::Full);
            }

            let entries = self.read_some()?;
            debug_assert!(!entries.is_empty());
            for entry in entries {
                self.writer.append(entry)?;
            }

            let now = unix_now()?;
            if now - start_at > self.options.time_limit as u128 {
                return Ok(MergeResult::Timeout);
            }
        }
    }
}
