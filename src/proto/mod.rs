pub(crate) mod wal {
    include!(concat!(env!("OUT_DIR"), "/eikv.wal.rs"));
}

pub(crate) mod sst {
    include!(concat!(env!("OUT_DIR"), "/eikv.sst.rs"));
}
