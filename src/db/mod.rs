pub(crate) mod path;

use self::path::{
    lock_file_path, manifest_dir_path, sst_dir_path, sst_level_dir_path, sst_major_tmp_path,
    sst_minor_tmp_path, sst_path, sst_tmp_dir_path, wal_dir_path, wal_path,
};
use crate::{
    limit::{LEVEL_MAX, LEVEL_MIN},
    mem_db::{MemDB, Table},
    model::{Manifest, SstMeta},
    sst::{self, Iterator, MergeResult, Merger},
    util::time::unix_now,
    wal::{Reader, Writer},
    Compressor, EikvResult, FilterFactory, Key, Value, WriteBatch,
};
use fs2::FileExt;
use std::{
    cmp::max,
    fs::{create_dir, remove_file, rename, File},
    path::Path,
    sync::{atomic::AtomicU64, Arc, Condvar, Mutex},
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
    manifest: Arc<Mutex<Manifest<K, V>>>,
    mem_db: Arc<MemDB<K, V>>,
    request_close: Arc<Mutex<bool>>,
    background_thread_exited: Arc<Condvar>,
}

impl<K: Key, V: Value> Drop for DB<K, V> {
    fn drop(&mut self) {
        let mut request_close = self.request_close.lock().unwrap();
        *request_close = true;
        while *request_close {
            request_close = self.background_thread_exited.wait(request_close).unwrap();
        }
    }
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

fn new_wal<K: Key, V: Value>(
    db_path: &str,
    manifest: Arc<Mutex<Manifest<K, V>>>,
) -> EikvResult<Writer> {
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

    let manifest_copy = manifest.clone();
    let mem_db_copy = mem_db.clone();
    let db_path_copy = db_path.to_owned();
    let options_copy = options.clone();
    let request_close = Arc::new(Mutex::new(false));
    let request_close_copy = request_close.clone();
    let background_thread_exited = Arc::new(Condvar::new());
    let background_thread_exited_copy = background_thread_exited.clone();

    thread::spawn(move || {
        background_thread(
            db_path_copy,
            options_copy,
            mem_db_copy,
            manifest_copy,
            request_close_copy,
            background_thread_exited_copy,
        )
        .unwrap();
    });

    let db = DB {
        _lock_file: lock_file,
        db_path: db_path.to_owned(),
        manifest,
        mem_db,
        request_close,
        background_thread_exited,
    };
    Ok(db)
}

fn init_db_dir(db_path: &str) -> EikvResult<()> {
    create_dir(db_path)?;
    create_dir(manifest_dir_path(db_path)?)?;
    create_dir(sst_dir_path(db_path)?)?;
    create_dir(wal_dir_path(db_path)?)?;
    create_dir(sst_tmp_dir_path(db_path)?)?;
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
    manifest: &Manifest<K, V>,
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

    let manifest = Manifest::load(db_path, options.block_size)?;
    let mem_db = Arc::new(load_mem_db(db_path, options.clone(), &manifest)?);
    let manifest = Arc::new(Mutex::new(manifest));

    let manifest_copy = manifest.clone();
    let mem_db_copy = mem_db.clone();
    let db_path_copy = db_path.to_owned();
    let options_copy = options.clone();
    let request_close = Arc::new(Mutex::new(false));
    let request_close_copy = request_close.clone();
    let background_thread_exited = Arc::new(Condvar::new());
    let background_thread_exited_copy = background_thread_exited.clone();

    thread::spawn(move || {
        background_thread(
            db_path_copy,
            options_copy,
            mem_db_copy,
            manifest_copy,
            request_close_copy,
            background_thread_exited_copy,
        )
        .unwrap();
    });

    let db = DB {
        _lock_file: lock_file,
        db_path: db_path.to_owned(),
        manifest,
        mem_db,
        request_close,
        background_thread_exited,
    };
    Ok(db)
}

struct MergerState<K: Key, V: Value> {
    merger: Merger<K, V>,
    level: usize,
    major_seqs: Vec<u64>,
    this_level_file_seq: Vec<u64>,
    next_level_file_seq: Vec<u64>,
}

fn get_merger<K: Key, V: Value>(
    manifest: Arc<Mutex<Manifest<K, V>>>,
    db_path: &str,
    db_options: DBOptions,
    wal_size_limit: u64,
) -> EikvResult<Option<MergerState<K, V>>> {
    let manifest = manifest.lock().unwrap();

    let mut target_level = 0;
    for level in LEVEL_MIN..=LEVEL_MAX {
        let size_max = wal_size_limit * 5_u64.pow(level as u32);
        if manifest.level_sst_count(level) > 6 || manifest.level_size(level) > size_max {
            target_level = level;
            break;
        }
    }
    if target_level == 0 || target_level == LEVEL_MAX {
        return Ok(None);
    }

    let file_seq = manifest.min_file_seq(target_level);
    let (files, this_level_file_seq, next_level_file_seq) =
        manifest.should_merge(db_path, target_level, file_seq)?;

    let mut sst_paths = vec![];
    let mut iterators = vec![];
    for (sst_path, sst_meta) in files {
        let sst_meta = (*sst_meta).clone();
        let mut iterator = Iterator::new(&sst_path, db_options.clone(), sst_meta)?;
        iterator.seek_to_first()?;
        iterators.push(iterator);
        sst_paths.push(sst_path);
    }

    let level = target_level + 1;
    let major_path = sst_major_tmp_path(db_path, 1)?;
    let size_limit = wal_size_limit * 5_u64.pow(level as u32 - 1);
    let merger = Merger::new(
        &major_path,
        iterators,
        db_options,
        u64::MAX,
        size_limit,
        100,
    )?;

    let merger_state = MergerState {
        merger,
        level,
        major_seqs: vec![1],
        this_level_file_seq,
        next_level_file_seq,
    };
    Ok(Some(merger_state))
}

fn minor_compaction<K: Key, V: Value>(
    db_path: &str,
    db_options: DBOptions,
    mem_db: Arc<MemDB<K, V>>,
    manifest: Arc<Mutex<Manifest<K, V>>>,
) -> EikvResult<()> {
    let block_size = db_options.block_size;
    let minor_path = sst_minor_tmp_path(db_path)?;
    let writer: sst::Writer<K, V> =
        sst::Writer::new(&minor_path, db_options.clone(), db_options.wal_size_limit)?;
    mem_db.dump(writer)?;

    let mut manifest = manifest.lock().unwrap();

    let file_seq = manifest.alloc_sst(LEVEL_MIN);
    let sst_path = sst_path(&db_path, LEVEL_MIN, file_seq)?;
    rename(&minor_path, &sst_path)?;
    let sst_meta = SstMeta::new(&sst_path, block_size)?;
    manifest.set_sst_meta(LEVEL_MIN, file_seq, sst_meta);

    let file_seq = manifest.remove_wal();
    manifest.dump(&db_path)?;
    let wal_path = wal_path(&db_path, file_seq)?;
    remove_file(wal_path)?;

    Ok(())
}

fn background_thread<K: Key, V: Value>(
    db_path: String,
    db_options: DBOptions,
    mem_db: Arc<MemDB<K, V>>,
    manifest: Arc<Mutex<Manifest<K, V>>>,
    request_close: Arc<Mutex<bool>>,
    background_thread_exited: Arc<Condvar>,
) -> EikvResult<()> {
    let mut merger_state: Option<MergerState<K, V>> = None;

    loop {
        {
            let mut request_close = request_close.lock().unwrap();
            if *request_close {
                *request_close = false;
                background_thread_exited.notify_one();
                break;
            }
        }

        if mem_db.has_immut() {
            minor_compaction(
                &db_path,
                db_options.clone(),
                mem_db.clone(),
                manifest.clone(),
            )?;
            continue;
        }

        if merger_state.is_none() {
            merger_state = get_merger(
                manifest.clone(),
                &db_path,
                db_options.clone(),
                db_options.wal_size_limit,
            )?;
        }

        if let Some(mut state) = merger_state {
            match state.merger.merge()? {
                MergeResult::Full => {
                    let major_seq = state.major_seqs.len() as u64 + 1;
                    let major_path = sst_major_tmp_path(&db_path, major_seq)?;
                    let size_limit = db_options.wal_size_limit * 5_u64.pow(state.level as u32 - 1);
                    let writer: sst::Writer<K, V> =
                        sst::Writer::new(&major_path, db_options.clone(), size_limit)?;
                    state.merger.set_writer(writer)?;
                    merger_state = Some(state)
                }
                MergeResult::Finish => {
                    state.merger.finish()?;

                    let mut manifest = manifest.lock().unwrap();
                    for major_seq in state.major_seqs {
                        let major_path = sst_major_tmp_path(&db_path, major_seq)?;
                        let file_seq = manifest.alloc_sst(state.level);
                        let sst_path = sst_path(&db_path, state.level, file_seq)?;
                        rename(&major_path, &sst_path)?;
                        let sst_meta = SstMeta::new(&sst_path, db_options.block_size)?;
                        manifest.set_sst_meta(state.level, file_seq, sst_meta);
                    }

                    for file_seq in &state.this_level_file_seq {
                        manifest.remove_sst(state.level - 1, *file_seq);
                    }
                    for file_seq in &state.next_level_file_seq {
                        manifest.remove_sst(state.level, *file_seq);
                    }
                    manifest.dump(&db_path)?;

                    for file_seq in state.this_level_file_seq {
                        let sst_path = sst_path(&db_path, state.level - 1, file_seq)?;
                        remove_file(sst_path).unwrap();
                    }
                    for file_seq in state.next_level_file_seq {
                        let sst_path = sst_path(&db_path, state.level, file_seq)?;
                        remove_file(sst_path).unwrap();
                    }

                    merger_state = None
                }
                MergeResult::Timeout => merger_state = Some(state),
            }
            continue;
        }

        if mem_db.wait_immut() {
            merger_state = get_merger(
                manifest.clone(),
                &db_path,
                db_options.clone(),
                db_options.wal_size_limit,
            )?;
        }
    }

    Ok(())
}
