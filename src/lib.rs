mod comparator;
mod proto;
mod sst;
mod util;
mod wal;

pub use comparator::Comparator;
pub use sst::{Compressor, Filter, FilterFactory};
pub use wal::WriteBatch;

use prost::{DecodeError, EncodeError};
use std::{io::Error as IoError, time::SystemTimeError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EikvError {
    #[error("io error: {0}")]
    IoError(#[from] IoError),
    #[error("encode error: {0}")]
    EncodeError(#[from] EncodeError),
    #[error("decode error: {0}")]
    DecodeError(#[from] DecodeError),
    #[error("checksum error: the checksumes of {owner} doesn't match")]
    ChecksumError { owner: &'static str },
    #[error("std error: {0}")]
    StdError(#[from] Box<dyn std::error::Error>),
    #[error("wal file is corrupt: {0}")]
    WalCorrpution(String),
    #[error("sstable file is corrupt: {0}")]
    SstCorrpution(String),
    #[error("system time error: {0}")]
    SystemTimeError(#[from] SystemTimeError),
}

pub type EikvResult<T> = Result<T, EikvError>;
