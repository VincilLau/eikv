pub(crate) mod path;

use self::path::{
    lock_file_path, manifest_dir_path, sst_dir_path, sst_level_dir_path, sst_path, wal_dir_path,
    wal_path,
};
use crate::{
    limit::{LEVEL_MAX, LEVEL_MIN},
    mem_db::{MemDB, Table},
    model::Manifest,
    sst,
    wal::{Reader, Writer},
    Compressor, EikvResult, FilterFactory, Key, Value, WriteBatch,
};
use fs2::FileExt;
use std::{
    cmp::max,
    fs::{create_dir, remove_file, File},
    path::Path,
    sync::{atomic::AtomicU64, Arc, Mutex, Weak},
    thread,
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
    mem_db: Arc<MemDB<K, V>>,
}

impl<K: Key + 'static, V: Value + 'static> DB<K, V> {
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
            self.manifest.lock().unwrap().dump(&self.db_path)?;
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

fn create_db<K: Key + 'static, V: Value + 'static>(
    db_path: &str,
    options: DBOptions,
) -> EikvResult<DB<K, V>> {
    init_db_dir(db_path)?;

    let manifest = Arc::new(Mutex::new(Manifest::new()));
    let wal = new_wal(db_path, manifest.clone())?;
    let mem_db = Arc::new(MemDB::new(options.clone(), AtomicU64::new(1), wal));
    manifest.lock().unwrap().dump(db_path)?;

    let lock_file = File::create(lock_file_path(db_path)?)?;
    lock_file.lock_exclusive()?;

    let manifest_weak = Arc::downgrade(&manifest);
    let mem_db_weak = Arc::downgrade(&mem_db);
    let db_path_string = db_path.to_owned();
    let db_options = options.clone();
    thread::spawn(move || {
        background_thread(db_path_string, db_options, mem_db_weak, manifest_weak).unwrap();
    });

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
    let mut file_seqs = vec![];
    for file_seq in manifest.wals().iter() {
        file_seqs.push(*file_seq);
    }
    file_seqs.sort_unstable();

    let mut_wal_file_seq = file_seqs[file_seqs.len() - 1];
    let (mut_table, max_seq) = read_wal::<K, V>(db_path, mut_wal_file_seq)?;
    let wal_path = wal_path(db_path, mut_wal_file_seq)?;
    let mut_wal = Writer::open(&wal_path)?;

    let immut_table: Table<K, V> = if file_seqs.len() == 1 {
        Table::new()
    } else {
        let (immut_table, _) = read_wal(db_path, file_seqs[0])?;
        immut_table
    };

    let next_seq = AtomicU64::new(max_seq + 1);
    let mut mem_db = MemDB::new(options, next_seq, mut_wal);
    mem_db.recover_mut_table(mut_table);
    mem_db.recover_immut_table(immut_table);
    Ok(mem_db)
}

fn open_db<K: Key + 'static, V: Value + 'static>(
    db_path: &str,
    options: DBOptions,
) -> EikvResult<DB<K, V>> {
    let lock_file_path = lock_file_path(db_path)?;
    let lock_file = File::open(&lock_file_path)?;
    lock_file.lock_exclusive()?;

    let manifest = Manifest::load(db_path)?;
    let mem_db = Arc::new(load_mem_db(db_path, options.clone(), &manifest)?);
    let manifest = Arc::new(Mutex::new(manifest));

    let manifest_weak = Arc::downgrade(&manifest);
    let mem_db_weak = Arc::downgrade(&mem_db);
    let db_path_string = db_path.to_owned();
    let db_options = options.clone();
    thread::spawn(move || {
        background_thread(db_path_string, db_options, mem_db_weak, manifest_weak).unwrap();
    });

    let db = DB {
        _lock_file: lock_file,
        db_path: db_path.to_owned(),
        manifest,
        mem_db,
    };
    Ok(db)
}

fn background_thread<K: Key, V: Value>(
    db_path: String,
    db_options: DBOptions,
    mem_db: Weak<MemDB<K, V>>,
    manifest: Weak<Mutex<Manifest>>,
) -> EikvResult<()> {
    loop {
        let manifest = match manifest.upgrade() {
            Some(manifest) => manifest,
            None => break,
        };
        let mem_db = match mem_db.upgrade() {
            Some(mem_db) => mem_db,
            None => break,
        };
        if mem_db.wait_immut() {
            continue;
        }

        let mut manifest = manifest.lock().unwrap();
        let file_seq = manifest.alloc_sst(1);
        let sst_path = sst_path(&db_path, 1, file_seq)?;
        let writer: sst::Writer<K, V> =
            sst::Writer::new(&sst_path, db_options.clone(), db_options.wal_size_limit)?;
        mem_db.dump(writer)?;
        let file_seq = manifest.remove_wal();
        manifest.dump(&db_path)?;
        let wal_path = wal_path(&db_path, file_seq)?;
        remove_file(wal_path)?;
    }

    Ok(())
}
