use std::{net::Ipv4Addr, path::PathBuf, time::Duration};

use bon::Builder;
use bytes::Bytes;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::info;

use crate::{
    error::ProtoError,
    receiver::session::{Session, join_session},
    transport::{Losing, MessageSocket},
};

mod assembler;
mod naks;
mod session;
mod sink;

const BLOCK_CHANNEL_CAPACITY: usize = 256;

#[derive(Builder)]
pub struct Receiver {
    #[builder(with = |local_address: Ipv4Addr, group_address: Ipv4Addr, port: u16,| -> Result<_, ProtoError> {
        MessageSocket::receiving(local_address, group_address, port)
    } )]
    socket: MessageSocket,
    max_wait: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransferSummary {
    pub total_bytes: u64,
    pub total_blocks: u64,
    pub received: u64,
    pub expected: u64,
    /// Shards that arrived twice, which means something was repaired more
    /// than it needed to be
    pub duplicates: u64,
    /// Shards for slices already written out, which is the normal cost of
    /// reconstructing a slice before its last shards arrive
    pub late: u64,
    pub naks_sent: u64,
}

impl TransferSummary {
    pub fn loss(&self) -> f64 {
        if self.expected == 0 {
            return 0.0;
        }
        1.0 - self.received as f64 / self.expected as f64
    }
}

/// A transfer in flight. Blocks arrive in order on the handle; the outcome
/// arrives from `finish`, so a consumer never has to unwrap an error out of
/// the data it is reading.
pub struct Transfer {
    blocks: mpsc::Receiver<Bytes>,
    session: JoinHandle<Result<TransferSummary, ProtoError>>,
}

impl Transfer {
    pub async fn recv(&mut self) -> Option<Bytes> {
        self.blocks.recv().await
    }

    pub async fn finish(self) -> Result<TransferSummary, ProtoError> {
        drop(self.blocks);
        self.session
            .await
            .map_err(|error| ProtoError::Timeout(error.to_string()))?
    }
}

impl Receiver {
    /// Applies a loss rule to this receiver's socket, so a test can decide
    /// exactly which datagrams it never sees
    pub fn losing(mut self, losing: Losing) -> Self {
        self.socket = self.socket.losing(losing);
        self
    }

    pub async fn recv_file(self, path: PathBuf) -> Result<TransferSummary, ProtoError> {
        let file = tokio::fs::File::create(path)
            .await
            .map_err(ProtoError::File)?;
        self.recv_to(file).await
    }

    pub async fn recv_to(
        self,
        writer: impl AsyncWrite + Unpin,
    ) -> Result<TransferSummary, ProtoError> {
        let transfer = self.recv_stream();
        sink::pump(transfer.blocks, writer).await?;

        transfer
            .session
            .await
            .map_err(|error| ProtoError::Timeout(error.to_string()))?
    }

    pub fn recv_stream(self) -> Transfer {
        let (blocks_tx, blocks) = mpsc::channel(BLOCK_CHANNEL_CAPACITY);
        let max_wait = self.max_wait;
        let socket = self.socket;

        let session = tokio::spawn(async move {
            let receiver_id = rand::random();
            let (hello, sender) = join_session(&socket, max_wait, receiver_id).await?;
            info!(
                transfer_id = hello.transfer_id,
                receiver_id, "joined session"
            );

            Session::new(socket, sender, receiver_id, &hello)
                .run(blocks_tx)
                .await
        });

        Transfer { blocks, session }
    }
}
