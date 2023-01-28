use std::io::Error as IoError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EikvError {
    #[error("io error: {0}")]
    IoError(#[from] IoError),
    #[error("std error: {0}")]
    StdError(#[from] Box<dyn std::error::Error>),
    #[error("wal file is corrupt: {0}")]
    WalCorrpution(String),
    #[error("sstable file is corrupt: {0}")]
    SstCorrpution(String),
    #[error("path error: {0}")]
    PathError(String),
    #[error("manifest error: {0}")]
    ManifestError(String),
}

pub type EikvResult<T> = Result<T, EikvError>;
