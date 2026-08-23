use std::{
    fs::File,
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use crate::{
    BLOCK_SIZE,
    error::ProtoError,
    receiver::{
        counter::BlockCounter,
        reorderer::Reorderer,
        sink::{FileSink, Sink, StreamSink},
    },
    transport::MessageSocket,
};
use bon::Builder;
use proto::{
    Done, Hello,
    Message::{self, Join},
    Stats,
};
use tokio::{sync::mpsc, time::Instant};
use tracing::info;
mod counter;
mod reorderer;
mod sink;

#[derive(Builder)]
pub struct Receiver {
    #[builder(with = |local_address: Ipv4Addr, group_address: Ipv4Addr, port: u16,| -> Result<_, ProtoError> {
        MessageSocket::receiving(local_address, group_address, port)
    } )]
    socket: MessageSocket,
    max_wait: Duration,
}

impl Receiver {
    pub async fn recv_file(&self, path: PathBuf) -> Result<(), ProtoError> {
        let sink = FileSink::new(File::create(path).map_err(ProtoError::File)?);
        self.recv(sink).await
    }

    pub async fn recv_stream(&self) -> Result<mpsc::Receiver<Vec<u8>>, ProtoError> {
        let (reorderer, rx) = Reorderer::new();
        let sink = StreamSink::new(reorderer);
        self.recv(sink).await?;
        Ok(rx)
    }

    async fn recv(&self, mut sink: impl Sink) -> Result<(), ProtoError> {
        let receiver_id = rand::random();
        let (hello, sender_socket) = self.join_session(receiver_id).await?;
        let transfer_id = hello.transfer_id;
        info!(transfer_id=%transfer_id, receiver_id, "joined session");

        let mut stats_tick = tokio::time::interval(Duration::from_millis(200));
        let mut block_counter = BlockCounter::default();
        let mut received_bytes = 0;
        let done = loop {
            tokio::select! {
            _ = stats_tick.tick() => {
                let message = Message::Stats(Stats {
                    transfer_id,
                    receiver_id,
                    blocks_received: block_counter.number_of_blocks_seen(),
                    blocks_expected: block_counter.highest_block_seen().map(|n| n + 1).unwrap_or(0)
                });
                self.socket.send_to(message, sender_socket).await?
            }

            r = self.socket.recv_in_transfer(transfer_id) => {
                let (message, _) = r?;
                match message {
                    Message::Hello(_) => {
                        self.socket
                            .send_to(Join(transfer_id, receiver_id), sender_socket)
                            .await?
                    }
                    Message::Data(data) => {
                        let block_no = data.block_no(hello.blocks_per_slice)?;
                        let offset = block_no
                            .checked_mul(BLOCK_SIZE as u64)
                            .ok_or(ProtoError::BlockOutOfRange { block_no })?;
                        received_bytes += data.payload.len() as u64;
                        sink.write(block_no, offset, &data.payload)
                            .await
                            .map_err(ProtoError::File)?;
                        block_counter.insert(block_no);
                        }
                        Message::Done(done) => {
                            break done;
                        }
                        _ => {}
                    };
                }
            }
        };

        if done.total_bytes != received_bytes {
            return Err(ProtoError::ByteCountMismatch {
                expected: done.total_bytes,
                received: received_bytes,
            });
        }
        sink.finish().await.map_err(ProtoError::File)?;

        Ok(())
    }

    async fn join_session(&self, receiver_id: u64) -> Result<(Hello, SocketAddr), ProtoError> {
        let deadline = Instant::now() + self.max_wait;

        let (hello, sender_socket) = loop {
            tokio::select! {
                r = self.socket.recv_from() => {
                    if let (Message::Hello(hello), socket) = r? {
                        break (hello, socket);
                    }
                },

                _ = tokio::time::sleep_until(deadline) => {
                    return Err(ProtoError::Timeout("Timed out listening for hello message".to_string()));
                },
            }
        };

        self.socket
            .send_to(Join(hello.transfer_id, receiver_id), sender_socket)
            .await?;

        Ok((hello, sender_socket))
    }
}
