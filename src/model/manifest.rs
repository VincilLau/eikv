use super::{Entry, SstMeta};
use crate::{
    db::path::{current_path, current_tmp_path, manifest_path, sst_level_dir_path, sst_path},
    limit::{LEVEL_MAX, LEVEL_MIN},
    EikvError, EikvResult, Key, Value,
};
use std::{
    cmp::min,
    collections::{HashMap, HashSet},
    fs::{self, remove_file, rename, File},
    io::{BufRead, BufReader, Read, Write},
    path::Path,
};

pub(crate) struct Manifest<K: Key, V: Value> {
    next_file_seq: u64,
    wals: HashSet<u64>,
    sstables: Vec<HashMap<u64, Option<SstMeta<K, V>>>>,
}

impl<K: Key, V: Value> Manifest<K, V> {
    pub(crate) fn new() -> Manifest<K, V> {
        let mut sstables = Vec::new();
        sstables.reserve(LEVEL_MAX);
        for _ in LEVEL_MIN..=LEVEL_MAX {
            sstables.push(HashMap::new());
        }
        Manifest {
            next_file_seq: 1,
            wals: HashSet::new(),
            sstables,
        }
    }

    pub(crate) fn get_level(&self, level: usize) -> &HashMap<u64, Option<SstMeta<K, V>>> {
        &self.sstables[level - 1]
    }

    pub(crate) fn get_mut_level(
        &mut self,
        level: usize,
    ) -> &mut HashMap<u64, Option<SstMeta<K, V>>> {
        &mut self.sstables[level - 1]
    }

    pub(crate) fn level_sst_count(&self, level: usize) -> usize {
        self.get_level(level).len()
    }

    pub(crate) fn level_size(&self, level: usize) -> u64 {
        let mut size = 0;
        for sst_meta in self.get_level(level).values() {
            size += sst_meta.as_ref().unwrap().file_size;
        }
        size
    }

    pub(crate) fn min_file_seq(&self, level: usize) -> u64 {
        let mut min_seq = u64::MAX;
        for seq in self.get_level(level).keys() {
            min_seq = min(min_seq, *seq);
        }
        min_seq
    }

    fn sst_meta(&self, level: usize, file_seq: u64) -> &SstMeta<K, V> {
        self.get_level(level)
            .get(&file_seq)
            .unwrap()
            .as_ref()
            .unwrap()
    }

    fn min_and_max_entries(&self, level: usize, file_seq: u64) -> (&Entry<K, V>, &Entry<K, V>) {
        let sst_meta = self.sst_meta(level, file_seq);
        let min_entry = &sst_meta.min_entry;
        let max_entry = &sst_meta.max_entry;
        (min_entry, max_entry)
    }

    pub(crate) fn should_merge(
        &self,
        db_path: &str,
        level: usize,
        file_seq: u64,
    ) -> EikvResult<(HashMap<String, &SstMeta<K, V>>, Vec<u64>, Vec<u64>)> {
        let (mut min_entry, mut max_entry) = self.min_and_max_entries(level, file_seq);
        let mut files = HashMap::new();
        let sstable_path = sst_path(db_path, level, file_seq)?;
        files.insert(sstable_path, self.sst_meta(level, file_seq));
        let next_level = level + 1;
        let mut this_level_file_seqs = vec![file_seq];
        let mut next_level_file_seqs = vec![];

        loop {
            let mut changed = false;

            for file_seq in self.get_level(next_level).keys() {
                let (sst_min_entry, sst_max_entry) =
                    self.min_and_max_entries(next_level, *file_seq);
                if (*sst_min_entry >= *min_entry && *sst_min_entry <= *max_entry)
                    || (*sst_max_entry >= *min_entry && *sst_max_entry <= *max_entry)
                {
                    let sstable_path = sst_path(db_path, next_level, *file_seq)?;
                    if files.get(&sstable_path).is_none() {
                        changed = true;
                        files.insert(sstable_path, self.sst_meta(next_level, *file_seq));
                        next_level_file_seqs.push(*file_seq);
                    }
                }
                min_entry = min(min_entry, sst_min_entry);
                max_entry = min(max_entry, sst_max_entry);
            }

            for file_seq in self.get_level(level).keys() {
                let (sst_min_entry, sst_max_entry) = self.min_and_max_entries(level, *file_seq);
                if (*sst_min_entry >= *min_entry && *sst_min_entry <= *max_entry)
                    || (*sst_max_entry >= *min_entry && *sst_max_entry <= *max_entry)
                {
                    let sstable_path = sst_path(db_path, level, *file_seq)?;
                    if files.get(&sstable_path).is_none() {
                        changed = true;
                        files.insert(sstable_path, self.sst_meta(level, *file_seq));
                        this_level_file_seqs.push(*file_seq);
                    }
                }
                min_entry = min(min_entry, sst_min_entry);
                max_entry = min(max_entry, sst_max_entry);
            }

            if !changed {
                break;
            }
        }

        Ok((files, this_level_file_seqs, next_level_file_seqs))
    }

    pub(crate) fn wals(&self) -> &HashSet<u64> {
        &self.wals
    }

    pub(crate) fn alloc_wal(&mut self) -> u64 {
        let file_seq = self.next_file_seq;
        self.next_file_seq += 1;
        self.wals.insert(file_seq);
        file_seq
    }

    pub(crate) fn alloc_sst(&mut self, level: usize) -> u64 {
        let file_seq = self.next_file_seq;
        self.next_file_seq += 1;
        self.get_mut_level(level).insert(file_seq, None);
        file_seq
    }

    pub(crate) fn remove_wal(&mut self) -> u64 {
        let mut file_seqs: Vec<&u64> = self.wals.iter().collect();
        file_seqs.sort_unstable();
        let file_seq = *file_seqs[0];
        self.wals.remove(&file_seq);
        file_seq
    }

    pub(crate) fn remove_sst(&mut self, level: usize, file_seq: u64) {
        self.get_mut_level(level).remove(&file_seq);
    }

    pub(crate) fn dump(&self, db_path: &str) -> EikvResult<()> {
        let current_path = current_path(db_path)?;
        if !Path::new(&current_path).try_exists()? {
            Manifest::<K, V>::write_current(&current_path, 0)?;
        }
        let manifest_seq = Manifest::<K, V>::read_current(db_path)?;

        let old_manifest_path = manifest_path(db_path, manifest_seq)?;
        let manifest_path = manifest_path(db_path, manifest_seq + 1)?;
        let mut file = File::create(manifest_path)?;
        for file_seq in &self.wals {
            let line = format!("{:06}.wal\n", file_seq);
            file.write(line.as_bytes())?;
        }
        for sst_level in &self.sstables {
            for file_seq in sst_level.keys() {
                let line = format!("{:06}.sst\n", file_seq);
                file.write(line.as_bytes())?;
            }
        }

        Manifest::<K, V>::atomic_increase_current(db_path)?;
        if Path::new(&old_manifest_path).try_exists()? {
            remove_file(old_manifest_path)?;
        }

        Ok(())
    }

    fn write_current(current_path: &str, manifest_seq: u64) -> EikvResult<()> {
        let mut file = File::create(current_path)?;
        let content = format!("{:06}", manifest_seq);
        file.write(content.as_bytes())?;
        Ok(())
    }

    fn read_current(db_path: &str) -> EikvResult<u64> {
        let current_path = current_path(db_path)?;
        let mut file = File::open(current_path)?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        let manifest_seq: u64 = match buf.parse() {
            Ok(manifest_seq) => manifest_seq,
            Err(err) => {
                let reason = format!(
                    "failed to parse mainifest sequence in the current file: {}",
                    err
                );
                return Err(EikvError::ManifestError(reason));
            }
        };
        Ok(manifest_seq)
    }

    fn atomic_increase_current(db_path: &str) -> EikvResult<()> {
        let manifest_seq = Manifest::<K, V>::read_current(db_path)? + 1;
        let current_path = current_path(db_path)?;
        let current_tmp_path = current_tmp_path(db_path)?;
        Manifest::<K, V>::write_current(&current_tmp_path, manifest_seq)?;
        rename(current_tmp_path, current_path)?;
        Ok(())
    }

    pub(crate) fn load(db_path: &str, block_size: usize) -> EikvResult<Manifest<K, V>> {
        let manifest_seq = Manifest::<K, V>::read_current(db_path)?;
        let manifest_path = manifest_path(db_path, manifest_seq)?;
        let file = File::open(&manifest_path).unwrap();
        let mut manifest = Manifest::new();
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.ends_with(".wal") {
                let file_seq = match line[..line.len() - 4].parse() {
                    Ok(file_seq) => file_seq,
                    Err(err) => {
                        let reason =
                            format!("failed to parse manifest line: line={line}, err={err}");
                        return Err(EikvError::ManifestError(reason));
                    }
                };
                manifest.wals.insert(file_seq);
                continue;
            }

            if line.ends_with(".sst") {
                let file_seq = match line[..line.len() - 4].parse() {
                    Ok(file_seq) => file_seq,
                    Err(err) => {
                        let reason =
                            format!("failed to parse manifest line: line={line}, err={err}");
                        return Err(EikvError::ManifestError(reason));
                    }
                };
                let level = get_level(db_path, file_seq)?;
                let sst_path = sst_path(db_path, level, file_seq)?;
                let sst_meta = SstMeta::new(&sst_path, block_size)?;
                manifest
                    .get_mut_level(level)
                    .insert(file_seq, Some(sst_meta));
                continue;
            }
        }

        Ok(manifest)
    }

    pub(crate) fn set_sst_meta(&mut self, level: usize, file_seq: u64, sst_meta: SstMeta<K, V>) {
        self.get_mut_level(level).insert(file_seq, Some(sst_meta));
    }
}

fn get_level(db_path: &str, file_seq: u64) -> EikvResult<usize> {
    for level in LEVEL_MIN..=LEVEL_MAX {
        let sst_dir = sst_level_dir_path(db_path, level)?;
        let sst_name = format!("{:06}.sst", file_seq);
        for entry in fs::read_dir(sst_dir)? {
            let entry = entry?;
            match entry.file_name().to_str() {
                Some(file_name) => {
                    if file_name == sst_name {
                        return Ok(level);
                    }
                }
                None => {
                    return Err(EikvError::PathError(
                        "failed to read sstable dir".to_owned(),
                    ))
                }
            }
        }
    }
    let reason = format!("can't find the file seq {file_seq}");
    Err(EikvError::ManifestError(reason))
}
