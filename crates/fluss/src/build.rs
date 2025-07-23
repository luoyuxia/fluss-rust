use std::io::Result;

fn main() -> Result<()> {
    prost_build::compile_protos(&["src/proto/fluss_api.proto"], &["src/proto"])?;
    Ok(())
}
