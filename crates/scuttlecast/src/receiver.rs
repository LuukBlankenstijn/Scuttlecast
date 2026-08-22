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
        reorderer::Reorderer,
        sink::{FileSink, Sink, StreamSink},
    },
    transport::MessageSocket,
};
use bon::Builder;
use proto::{
    Hello,
    Message::{self, Join},
};
use tokio::{sync::mpsc, time::Instant};
use tracing::info;
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
        let (hello, sender_socket) = self.recv_hello().await?;
        let receiver_id = rand::random();
        info!("got hello message {hello}");
        self.socket
            .send_to(Join(hello.transfer_id, receiver_id), sender_socket)
            .await?;

        let mut received_bytes = 0;
        let done = loop {
            let (message, _) = self.socket.recv_in_transfer(hello.transfer_id).await?;
            match message {
                Message::Hello(_) => {
                    self.socket
                        .send_to(Join(hello.transfer_id, receiver_id), sender_socket)
                        .await?
                }
                Message::Data(data) => {
                    let block_no = data.block_no(hello.blocks_per_slice);
                    received_bytes += data.payload.len() as u64;
                    sink.write(block_no, block_no as u64 * BLOCK_SIZE as u64, &data.payload)
                        .await
                        .map_err(ProtoError::File)?;
                }
                Message::Done(done) => {
                    break done;
                }
                _ => {}
            };
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

    async fn recv_hello(&self) -> Result<(Hello, SocketAddr), ProtoError> {
        let deadline = Instant::now() + self.max_wait;

        loop {
            tokio::select! {
                r = self.socket.recv_from() => {
                    if let (Message::Hello(hello), socket) = r? {
                        return Ok((hello, socket));
                    }
                },

                _ = tokio::time::sleep_until(deadline) => {
                    return Err(ProtoError::Timeout("Timed out listening for hello message".to_string()));
                },
            }
        }
    }
}
