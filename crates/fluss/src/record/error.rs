use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {

    #[error(transparent)]
    Io(#[from] io::Error),
}