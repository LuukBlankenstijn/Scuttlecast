use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtoError {
    #[error("network I/O error: {0}")]
    Socket(#[from] std::io::Error),

    #[error("file I/O error: {0}")]
    File(std::io::Error),

    #[error(transparent)]
    Protocol(#[from] crate::proto::Error),

    #[error("invalid erasure-coding configuration: {0}")]
    Fec(#[from] reed_solomon_erasure::Error),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("the pacer stopped consuming outbound blocks")]
    EgressClosed,

    #[error("the sender went silent with {held} of {total:?} slices written")]
    SenderSilent { held: u32, total: Option<u32> },

    #[error("expected {expected} bytes, received {received}")]
    ByteCountMismatch { expected: u64, received: u64 },

    #[error("evicted by the sender: {0}")]
    Evicted(String),

    #[error("the output stopped accepting blocks")]
    SinkClosed,

    #[error("{complete} of {participants} receivers completed the transfer")]
    TransferIncomplete {
        complete: usize,
        participants: usize,
    },

    #[error("no receivers joined the transfer")]
    NoParticipants,
}
