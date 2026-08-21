use std::{fs::File, net::Ipv4Addr, path::PathBuf};

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
use proto::{Hello, Message};
use tokio::sync::mpsc;
mod reorderer;
mod sink;

#[derive(Builder)]
pub struct Receiver {
    #[builder(with = |local_address: Ipv4Addr, group_address: Ipv4Addr, port: u16,| -> Result<_, ProtoError> { 
        MessageSocket::receiving(local_address, group_address, port)
    } )]
    socket: MessageSocket,
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
        let hello = self.recv_hello().await?;
        println!("got hello message {hello}",);

        let mut received_bytes = 0;
        let done = loop {
            let message = self.recv_transfer_message(hello.transfer_id).await?;
            match message {
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

    async fn recv_hello(&self) -> Result<Hello, ProtoError> {
        Ok(loop {
            if let (Message::Hello(hello), _) = self.socket.recv_from().await? {
                break hello;
            }
        })
    }

    async fn recv_transfer_message(&self, transfer_id: u64) -> Result<Message, ProtoError> {
        loop {
            let (message, _) = self.socket.recv_from().await?;
            if message.transfer_id() == transfer_id {
                return Ok(message);
            }
        }
    }
}
