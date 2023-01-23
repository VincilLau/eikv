pub(crate) mod path;

use self::path::{
    lock_file_path, manifest_dir_path, sst_dir_path, sst_level_dir_path, wal_dir_path, wal_path,
};
use crate::{
    limit::{LEVEL_MAX, LEVEL_MIN},
    mem_db::{MemDB, Wal},
    model::Manifest,
    wal::Writer,
    EikvResult, Key, Value, WriteBatch,
};
use fs2::FileExt;
use std::{
    fs::{create_dir, File},
    sync::atomic::AtomicU64,
};

#[derive(Clone)]
pub struct DBOptions {
    pub block_size: usize,
    pub create_if_missing: bool,
    pub wal_size_limit: u64,
}

impl Default for DBOptions {
    fn default() -> Self {
        Self {
            block_size: 4096,
            create_if_missing: true,
            wal_size_limit: 2 * 1024 * 1024,
        }
    }
}

pub struct DB<K: Key, V: Value> {
    _lock_file: File,
    db_path: String,
    manifest: Manifest,
    mem_db: MemDB<K, V>,
}

impl<K: Key, V: Value> DB<K, V> {
    pub fn new(path: &str, options: DBOptions) -> EikvResult<DB<K, V>> {
        create_db(path, options)
    }

    pub fn write(&mut self, write_batch: WriteBatch<K, V>) -> EikvResult<()> {
        if write_batch.is_empty() {
            return Ok(());
        }
        if self.mem_db.write(write_batch)? {
            let wal = new_wal(&self.db_path, &mut self.manifest)?;
            self.mem_db.freeze(wal);
        }
        Ok(())
    }

    pub fn put(&mut self, key: K, value: V) -> EikvResult<()> {
        let mut write_batch = WriteBatch::new();
        write_batch.put(key, value);
        self.write(write_batch)
    }

    pub fn delete(&mut self, key: K) -> EikvResult<()> {
        let mut write_batch = WriteBatch::new();
        write_batch.delete(key);
        self.write(write_batch)
    }

    pub fn get(&mut self, key: K) -> EikvResult<Option<V>> {
        match self.mem_db.get(key) {
            Some(entry) => Ok(entry.value),
            None => Ok(None),
        }
    }
}

fn new_wal(db_path: &str, manifest: &mut Manifest) -> EikvResult<Wal> {
    let file_seq = manifest.alloc_wal();
    let writer = Writer::create(&wal_path(db_path, file_seq)?)?;
    let wal = Wal::new(file_seq, writer);
    Ok(wal)
}

fn create_db<K: Key, V: Value>(db_path: &str, options: DBOptions) -> EikvResult<DB<K, V>> {
    init_db_dir(db_path)?;

    let mut manifest = Manifest::new();
    let wal = new_wal(db_path, &mut manifest)?;
    let mem_db = MemDB::new(options, AtomicU64::new(1), wal);
    manifest.dump(db_path)?;

    let lock_file = File::create(lock_file_path(db_path)?)?;
    lock_file.lock_exclusive()?;

    let db = DB {
        _lock_file: lock_file,
        db_path: db_path.to_owned(),
        manifest,
        mem_db,
    };
    Ok(db)
}

fn init_db_dir(db_path: &str) -> EikvResult<()> {
    create_dir(db_path)?;
    create_dir(manifest_dir_path(db_path)?)?;
    create_dir(sst_dir_path(db_path)?)?;
    create_dir(wal_dir_path(db_path)?)?;
    for level in LEVEL_MIN..=LEVEL_MAX {
        let sst_level_dir = sst_level_dir_path(db_path, level)?;
        create_dir(sst_level_dir)?;
    }
    Ok(())
}
