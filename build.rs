//! Generates Rust types for the GTFS-Realtime protobuf schema at build time.
//!
//! We deliberately compile the `.proto` ourselves rather than depending on a
//! pre-built bindings crate: it keeps the build self-contained (the protoc
//! binary is supplied by the `protoc-bin-vendored` crate, so no system protoc
//! is required) and lets us track the upstream schema explicitly in `proto/`.

fn main() {
    // Point prost-build's protoc invocation at the vendored binary.
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc available");

    println!("cargo:rerun-if-changed=proto/gtfs-realtime.proto");

    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    config
        .compile_protos(&["proto/gtfs-realtime.proto"], &["proto/"])
        .expect("failed to compile gtfs-realtime.proto");
}
