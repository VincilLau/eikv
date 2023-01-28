use crate::{Key, Value, WriteBatch};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Condvar, Mutex,
    },
    thread::{self, ThreadId},
};

struct WriteOp<K: Key, V: Value> {
    thread_id: ThreadId,
    write_batch: WriteBatch<K, V>,
}

impl<K: Key, V: Value> WriteOp<K, V> {
    fn new(write_batch: WriteBatch<K, V>) -> WriteOp<K, V> {
        WriteOp {
            write_batch,
            thread_id: thread::current().id(),
        }
    }
}

pub(super) struct WriteQueue<K: Key, V: Value> {
    finished: Condvar,
    queue: Mutex<VecDeque<WriteOp<K, V>>>,
    next_seq: AtomicU64,
}

impl<K: Key, V: Value> WriteQueue<K, V> {
    pub(super) fn new(next_seq: AtomicU64) -> WriteQueue<K, V> {
        WriteQueue {
            queue: Mutex::new(VecDeque::new()),
            finished: Condvar::new(),
            next_seq,
        }
    }

    pub(super) fn line_up(&self, write_batch: WriteBatch<K, V>) -> Option<WriteBatch<K, V>> {
        let write_op = WriteOp::new(write_batch);
        let mut guard = self.queue.lock().unwrap();
        guard.push_back(write_op);
        loop {
            if guard.front().unwrap().thread_id == thread::current().id() {
                break;
            }
            let queue = self.finished.wait(guard).unwrap();
            for write_op in queue.iter() {
                if write_op.thread_id == thread::current().id() {
                    continue;
                }
            }
            return None;
        }

        let len = guard.len();
        let mut write_batch = WriteBatch::new();
        for _ in 0..len {
            write_batch.extend(guard.pop_front().unwrap().write_batch);
        }

        let start_seq = self
            .next_seq
            .fetch_add(write_batch.len() as u64, Ordering::Relaxed);
        write_batch.set_seqs(start_seq);

        Some(write_batch)
    }

    pub(super) fn notify_waiters(&self) {
        self.finished.notify_all();
    }
}
