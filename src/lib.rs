mod proto;
mod util;
mod wal;

pub use wal::WriteBatch;

use prost::{DecodeError, EncodeError};
use std::io::Error as IoError;
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
    #[error("wal file is corrupt: {0}")]
    WalCorrpution(String),
}

pub type EikvResult<T> = Result<T, EikvError>;
