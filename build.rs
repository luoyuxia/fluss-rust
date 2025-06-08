use std::io::Result;

fn main() -> Result<()> {
    prost_build::compile_protos(&["src/rpc/FlussApi.proto"], &["src/rpc"])?;
    Ok(())
}
