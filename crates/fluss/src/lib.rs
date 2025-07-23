pub mod client;
pub mod metadata;
pub mod record;
pub mod row;
pub mod rpc;

mod cluster;

pub mod error;
pub mod config;

mod util;



pub type TableId = u64;
pub type PartitionId = u64;
pub type BucketId = i32;


pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}
