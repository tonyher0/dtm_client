fn main() -> Result<(), Box<dyn std::error::Error>> {
    let grpc_enabled = std::env::var("CARGO_FEATURE_GRPC").is_ok();
    let workflow_enabled = std::env::var("CARGO_FEATURE_WORKFLOW").is_ok();

    println!("cargo:rerun-if-changed=proto/dtmgimp.proto");
    println!("cargo:rerun-if-changed=proto/wf.proto");
    println!("cargo:rerun-if-changed=proto/google/protobuf/empty.proto");

    if !(grpc_enabled || workflow_enabled) {
        return Ok(());
    }

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/dtmgimp.proto", "proto/wf.proto"], &["proto"])?;

    Ok(())
}
