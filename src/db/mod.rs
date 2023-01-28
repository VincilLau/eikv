pub(crate) mod path;

use self::path::{
    lock_file_path, manifest_dir_path, sst_dir_path, sst_level_dir_path, wal_dir_path, wal_path,
};
use crate::{
    limit::{LEVEL_MAX, LEVEL_MIN},
    mem_db::{MemDB, Table},
    model::Manifest,
    wal::{Reader, Writer},
    Compressor, EikvResult, FilterFactory, Key, Value, WriteBatch,
};
use fs2::FileExt;
use std::{
    cmp::max,
    fs::{create_dir, File},
    path::Path,
    sync::{atomic::AtomicU64, Arc, Mutex},
};

#[derive(Clone)]
pub struct DBOptions {
    pub block_size: usize,
    pub compressor: Option<Arc<dyn Compressor>>,
    pub create_if_missing: bool,
    pub filter_factory: Option<Arc<dyn FilterFactory>>,
    pub restart_interval: usize,
    pub wal_size_limit: u64,
}

impl Default for DBOptions {
    fn default() -> Self {
        Self {
            block_size: 4096,
            compressor: None,
            create_if_missing: true,
            filter_factory: None,
            restart_interval: 16,
            wal_size_limit: 2 * 1024 * 1024,
        }
    }
}

pub struct DB<K: Key, V: Value> {
    _lock_file: File,
    db_path: String,
    manifest: Arc<Mutex<Manifest>>,
    mem_db: MemDB<K, V>,
}

impl<K: Key, V: Value> DB<K, V> {
    pub fn new(path: &str, options: DBOptions) -> EikvResult<DB<K, V>> {
        if options.create_if_missing && !Path::new(path).try_exists()? {
            create_db(path, options)
        } else {
            open_db(path, options)
        }
    }

    pub fn write(&self, write_batch: WriteBatch<K, V>) -> EikvResult<()> {
        if write_batch.is_empty() {
            return Ok(());
        }
        if self.mem_db.write(write_batch)? {
            let wal = new_wal(&self.db_path, self.manifest.clone())?;
            self.mem_db.freeze(wal);
        }
        self.mem_db.write_finished();
        Ok(())
    }

    pub fn put(&self, key: K, value: V) -> EikvResult<()> {
        let mut write_batch = WriteBatch::new();
        write_batch.put(key, value);
        self.write(write_batch)
    }

    pub fn delete(&self, key: K) -> EikvResult<()> {
        let mut write_batch = WriteBatch::new();
        write_batch.delete(key);
        self.write(write_batch)
    }

    pub fn get(&self, key: K) -> EikvResult<Option<V>> {
        match self.mem_db.get(key) {
            Some(entry) => Ok(entry.value),
            None => Ok(None),
        }
    }
}

fn new_wal(db_path: &str, manifest: Arc<Mutex<Manifest>>) -> EikvResult<Writer> {
    let file_seq = manifest.lock().unwrap().alloc_wal();
    let writer = Writer::create(&wal_path(db_path, file_seq)?)?;
    Ok(writer)
}

fn create_db<K: Key, V: Value>(db_path: &str, options: DBOptions) -> EikvResult<DB<K, V>> {
    init_db_dir(db_path)?;

    let mut manifest = Arc::new(Mutex::new(Manifest::new()));
    let wal = new_wal(db_path, manifest.clone())?;
    let mem_db = MemDB::new(options, AtomicU64::new(1), wal);
    manifest.lock().unwrap().dump(db_path)?;

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

fn read_wal<K: Key, V: Value>(db_path: &str, file_seq: u64) -> EikvResult<(Table<K, V>, u64)> {
    let wal_path = wal_path(db_path, file_seq)?;
    let mut reader = Reader::open(&wal_path)?;
    let mut table = Table::new();
    let mut max_seq = 0;
    loop {
        let write_batch = match reader.next()? {
            Some(write_batch) => write_batch,
            None => break,
        };
        for entry in write_batch.entries() {
            max_seq = max(max_seq, entry.seq);
            table.insert(entry.clone());
        }
    }
    Ok((table, max_seq))
}

fn load_mem_db<K: Key, V: Value>(
    db_path: &str,
    options: DBOptions,
    manifest: &Manifest,
) -> EikvResult<MemDB<K, V>> {
    let mut iter = manifest.wals().iter();
    let file_seq = *iter.next().unwrap();
    let (mut_table, max_seq) = read_wal::<K, V>(db_path, file_seq)?;
    let immut_table: Table<K, V> = match iter.next() {
        Some(file_seq) => {
            let (immut_table, _) = read_wal(db_path, *file_seq)?;
            immut_table
        }
        None => Table::new(),
    };

    let wal_path = wal_path(db_path, file_seq)?;
    let mut_wal = Writer::open(&wal_path)?;

    let next_seq = AtomicU64::new(max_seq + 1);
    let mut mem_db = MemDB::new(options, next_seq, mut_wal);
    mem_db.recover_mut_table(mut_table);
    mem_db.recover_immut_table(immut_table);
    Ok(mem_db)
}

fn open_db<K: Key, V: Value>(db_path: &str, options: DBOptions) -> EikvResult<DB<K, V>> {
    let lock_file_path = lock_file_path(db_path)?;
    let lock_file = File::open(&lock_file_path)?;
    lock_file.lock_exclusive()?;

    let manifest = Manifest::load(db_path)?;
    let mem_db = load_mem_db(db_path, options, &manifest)?;
    let manifest = Arc::new(Mutex::new(manifest));

    let db = DB {
        _lock_file: lock_file,
        db_path: db_path.to_owned(),
        manifest,
        mem_db,
    };
    Ok(db)
}
