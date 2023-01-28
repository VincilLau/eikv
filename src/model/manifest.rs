use crate::{
    db::path::{current_path, current_tmp_path, manifest_path},
    limit::{LEVEL_MAX, LEVEL_MIN},
    EikvError, EikvResult,
};
use std::{
    collections::HashSet,
    fs::{remove_file, rename, File},
    io::{BufRead, BufReader, Read, Write},
    path::Path,
};

pub(crate) struct Manifest {
    next_file_seq: u64,
    wals: HashSet<u64>,
    sstables: Vec<HashSet<u64>>,
}

impl Manifest {
    pub(crate) fn new() -> Manifest {
        let mut sstables = vec![];
        sstables.reserve(LEVEL_MAX);
        for _ in LEVEL_MIN..=LEVEL_MAX {
            sstables.push(HashSet::new());
        }
        Manifest {
            next_file_seq: 1,
            wals: HashSet::new(),
            sstables,
        }
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
        self.sstables[level].insert(file_seq);
        file_seq
    }

    pub(crate) fn remove_wal(&mut self) -> u64 {
        let mut file_seqs: Vec<&u64> = self.wals.iter().collect();
        file_seqs.sort_unstable();
        let file_seq = *file_seqs[0];
        self.wals.remove(&file_seq);
        file_seq
    }

    pub(crate) fn dump(&self, db_path: &str) -> EikvResult<()> {
        let current_path = current_path(db_path)?;
        if !Path::new(&current_path).try_exists()? {
            Manifest::write_current(&current_path, 0)?;
        }
        let manifest_seq = Manifest::read_current(db_path)?;

        let old_manifest_path = manifest_path(db_path, manifest_seq)?;
        let manifest_path = manifest_path(db_path, manifest_seq + 1)?;
        let mut file = File::create(manifest_path)?;
        for file_seq in &self.wals {
            let line = format!("{:06}.wal\n", file_seq);
            file.write(line.as_bytes())?;
        }
        for sst_level in &self.sstables {
            for file_seq in sst_level {
                let line = format!("{:06}.sst\n", file_seq);
                file.write(line.as_bytes())?;
            }
        }

        Manifest::atomic_increase_current(db_path)?;
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
        let manifest_seq = Manifest::read_current(db_path)? + 1;
        let current_path = current_path(db_path)?;
        let current_tmp_path = current_tmp_path(db_path)?;
        Manifest::write_current(&current_tmp_path, manifest_seq)?;
        rename(current_tmp_path, current_path)?;
        Ok(())
    }

    pub(crate) fn load(db_path: &str) -> EikvResult<Manifest> {
        let manifest_seq = Manifest::read_current(db_path)?;
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
                manifest.wals.insert(file_seq);
                continue;
            }
        }

        Ok(manifest)
    }
}
