use clap::Parser;
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug, Clone, Deserialize, Serialize, Default)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bootstrap_server: Option<String>,

    #[arg(long, default_value_t = 10 * 1024 * 1024)]
    pub request_max_size: i32,

    #[arg(long, default_value_t = String::from("all"))]
    pub writer_acks: String,

    #[arg(long, default_value_t = i32::MAX)]
    pub writer_retries: i32,

    #[arg(long, default_value_t = 2 * 1024 * 1024)]
    pub writer_batch_size: i32,
}