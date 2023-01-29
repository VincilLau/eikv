use crate::{EikvError, EikvResult};
use std::path::Path;

fn join_path(path1: &str, path2: &str) -> EikvResult<String> {
    let path = Path::new(path1).join(path2);
    let path = path.to_str();
    match path {
        Some(path) => Ok(path.to_owned()),
        None => {
            let reason = format!("can't join paths: path1={}, path2={}", path1, path2);
            Err(EikvError::PathError(reason))
        }
    }
}

pub(crate) fn current_path(db_path: &str) -> EikvResult<String> {
    join_path(db_path, "current")
}

pub(crate) fn current_tmp_path(db_path: &str) -> EikvResult<String> {
    join_path(db_path, "current.tmp")
}

pub(crate) fn lock_file_path(db_path: &str) -> EikvResult<String> {
    join_path(db_path, "lock")
}

pub(crate) fn manifest_dir_path(db_path: &str) -> EikvResult<String> {
    join_path(db_path, "manifest")
}

pub(crate) fn manifest_path(db_path: &str, manifest_seq: u64) -> EikvResult<String> {
    let file_name = format!("{:06}.manifest", manifest_seq);
    join_path(&manifest_dir_path(db_path)?, &file_name)
}

pub(crate) fn sst_dir_path(db_path: &str) -> EikvResult<String> {
    join_path(db_path, "sst")
}

pub(crate) fn sst_level_dir_path(db_path: &str, level: usize) -> EikvResult<String> {
    join_path(&sst_dir_path(db_path)?, &level.to_string())
}

pub(crate) fn sst_tmp_dir_path(db_path: &str) -> EikvResult<String> {
    let sst_dir_path = sst_dir_path(db_path)?;
    join_path(&sst_dir_path, "tmp")
}

pub(crate) fn sst_minor_tmp_path(db_path: &str) -> EikvResult<String> {
    let sst_tmp_dir_path = sst_tmp_dir_path(db_path)?;
    join_path(&sst_tmp_dir_path, "minor.sst")
}

pub(crate) fn sst_major_tmp_path(db_path: &str, major_seq: u64) -> EikvResult<String> {
    let sst_tmp_dir_path = sst_tmp_dir_path(db_path)?;
    let file_name = format!("major_{:06}.sst", major_seq);
    join_path(&sst_tmp_dir_path, &file_name)
}

pub(crate) fn sst_path(db_path: &str, level: usize, file_seq: u64) -> EikvResult<String> {
    let sst_level_dir_path = join_path(&sst_dir_path(db_path)?, &level.to_string())?;
    let file_name = format!("{:06}.sst", file_seq);
    join_path(&sst_level_dir_path, &file_name)
}

pub(crate) fn wal_dir_path(db_path: &str) -> EikvResult<String> {
    join_path(db_path, "wal")
}

pub(crate) fn wal_path(db_path: &str, file_seq: u64) -> EikvResult<String> {
    let wal_dir_path = wal_dir_path(db_path)?;
    let file_name = format!("{:06}.wal", file_seq);
    join_path(&wal_dir_path, &file_name)
}

#[cfg(test)]
mod tests {
    use super::lock_file_path;
    use super::manifest_dir_path;
    use super::sst_dir_path;
    use super::sst_level_dir_path;
    use super::wal_dir_path;

    #[test]
    fn test_lock_file_path() {
        let db_path = "/tmp/eikv";
        let want = "/tmp/eikv/lock";
        let res = lock_file_path(db_path).unwrap();
        assert_eq!(want, res);
    }

    #[test]
    fn test_manifest_dir_path() {
        let db_path = "/tmp/eikv";
        let want = "/tmp/eikv/manifest";
        let res = manifest_dir_path(db_path).unwrap();
        assert_eq!(want, res);
    }

    #[test]
    fn test_sst_dir_path() {
        let db_path = "/tmp/eikv";
        let want = "/tmp/eikv/sst";
        let res = sst_dir_path(db_path).unwrap();
        assert_eq!(want, res);
    }

    #[test]
    fn test_sst_level_dir_path() {
        let db_path = "/tmp/eikv";
        let want = "/tmp/eikv/sst/1";
        let res = sst_level_dir_path(db_path, 1).unwrap();
        assert_eq!(want, res);
    }

    #[test]
    fn test_wal_dir_path() {
        let db_path = "/tmp/eikv";
        let want = "/tmp/eikv/wal";
        let res = wal_dir_path(db_path).unwrap();
        assert_eq!(want, res);
    }
}
