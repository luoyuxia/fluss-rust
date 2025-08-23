use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
#[allow(dead_code)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
}
