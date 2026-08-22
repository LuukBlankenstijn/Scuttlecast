use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtoError {
    #[error("network I/O error: {0}")]
    Socket(#[from] std::io::Error),

    #[error("file I/O error: {0}")]
    File(std::io::Error),

    #[error(transparent)]
    Protocol(#[from] proto::Error),

    #[error("invalid address: {0}")]
    AddrParse(#[from] std::net::AddrParseError),

    #[error("expected {expected} bytes, received {received}")]
    ByteCountMismatch { expected: u64, received: u64 },

    #[error("Timeout: {0}")]
    Timeout(String),
}
