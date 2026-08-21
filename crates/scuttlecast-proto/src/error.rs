use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to encode message: {0}")]
    Encode(#[from] bincode::error::EncodeError),

    #[error("failed to decode message: {0}")]
    Decode(#[from] bincode::error::DecodeError),
}
