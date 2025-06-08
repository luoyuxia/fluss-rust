use std::time::Duration;

use clap::Parser;
use serde::{Deserialize, Serialize};

const DEFAULT_CONNECTION_RW_TIMEOUT_MILLS: u64 = 120 * 1000;

#[derive(Parser, Debug, Clone, Deserialize, Serialize, Default)]
#[command(author, version, about, long_about = None)]
pub struct ArgsOptional {
    /// The optional bootstrap server address for Fluss
    ///
    /// [default: 127.0.0.1:9123]

    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bootstrap_server: Option<String>,

    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rw_tiemout_mills: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct Args {
    // the bootstrap server for Fluss
    pub bootstrap_server: String,
    pub rw_timeout: Duration,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            bootstrap_server: "127.0.0.1:9123".to_string(),
            rw_timeout: Duration::from_millis(DEFAULT_CONNECTION_RW_TIMEOUT_MILLS),
        }
    }
}

impl From<Vec<ArgsOptional>> for Args {
    fn from(args_set: Vec<ArgsOptional>) -> Self {
        let mut args = Args::default();
        for optional_args in args_set {
            if let Some(bootstrap_server) = optional_args.bootstrap_server {
                args.bootstrap_server = bootstrap_server;
            }
        }
        args
    }
}
