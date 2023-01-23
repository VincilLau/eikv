fn main() {
    let protos = ["src/proto/sst.proto"];
    prost_build::compile_protos(&protos, &["src/"]).unwrap();
}
