use crate::{EikvResult, Key, Value, WriteBatch};
use std::{
    fs::File,
    io::{Seek, Write},
};

pub(crate) struct Writer {
    file: File,
}

impl Writer {
    pub(crate) fn create(path: &str) -> EikvResult<Writer> {
        let file = File::create(path)?;
        let writer = Writer { file };
        Ok(writer)
    }

    pub(crate) fn append<K: Key, V: Value>(
        &mut self,
        write_batch: WriteBatch<K, V>,
    ) -> EikvResult<()> {
        let mut buf = vec![];
        write_batch.encode(&mut buf)?;
        self.file.write(&buf)?;
        Ok(())
    }

    pub(crate) fn file_size(&mut self) -> EikvResult<u64> {
        let pos = self.file.stream_position()?;
        Ok(pos)
    }
}
