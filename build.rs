fn main() {
    prost_build::compile_protos(&["src/proto/wal.proto"], &["src/"]).unwrap();
}
