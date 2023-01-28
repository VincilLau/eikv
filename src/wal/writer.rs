use crate::{EikvResult, Key, Value, WriteBatch};
use std::{
    fs::{File, OpenOptions},
    io::{Seek, Write},
    sync::Mutex,
};

pub(crate) struct Writer {
    file: Mutex<File>,
}

impl Writer {
    pub(crate) fn create(path: &str) -> EikvResult<Writer> {
        let file = File::create(path)?;
        let writer = Writer {
            file: Mutex::new(file),
        };
        Ok(writer)
    }

    pub(crate) fn open(path: &str) -> EikvResult<Writer> {
        let file = OpenOptions::new().append(true).open(path)?;
        let writer = Writer {
            file: Mutex::new(file),
        };
        Ok(writer)
    }

    pub(crate) fn append<K: Key, V: Value>(&self, write_batch: WriteBatch<K, V>) -> EikvResult<()> {
        let mut buf = vec![];
        write_batch.encode(&mut buf)?;
        self.file.lock().unwrap().write(&buf)?;
        Ok(())
    }

    pub(crate) fn file_offset(&self) -> EikvResult<u64> {
        let pos = self.file.lock().unwrap().stream_position()?;
        Ok(pos)
    }
}
