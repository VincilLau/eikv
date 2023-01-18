use crate::{util::coding::append_fixed_u64, EikvResult, WriteBatch};
use std::{
    fs::{File, OpenOptions},
    io::Write,
};

pub(crate) struct Writer {
    file: File,
}

impl Writer {
    pub(crate) fn create(path: &str, seq: u64) -> EikvResult<Writer> {
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        let mut buf = vec![];
        append_fixed_u64(&mut buf, seq);
        file.write(&buf)?;
        let writer = Writer { file };
        Ok(writer)
    }

    pub(crate) fn append(&mut self, wb: &WriteBatch) -> EikvResult<()> {
        let mut buf = vec![];
        wb.append_to(&mut buf)?;
        self.file.write(&buf)?;
        Ok(())
    }
}
