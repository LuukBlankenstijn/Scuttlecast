use std::{
    fs::File,
    net::{Ipv4Addr, SocketAddrV4},
    path::PathBuf,
};

use crate::{
    BLOCK_SIZE,
    error::ProtoError,
    receiver::{
        reorderer::Reorderer,
        sink::{FileSink, Sink, StreamSink},
    },
};
use proto::{Hello, Message};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
mod reorderer;
mod sink;

pub struct Receiver {
    socket: UdpSocket,
}

impl Receiver {
    pub fn new(
        local_address: Ipv4Addr,
        group_address: Ipv4Addr,
        group_port: u16,
    ) -> Result<Self, ProtoError> {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_reuse_address(true)?;
        socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, group_port).into())?;
        socket.join_multicast_v4(&group_address, &local_address)?;
        socket.set_nonblocking(true)?;
        let std_socket: std::net::UdpSocket = socket.into();
        let tokio_socket = UdpSocket::from_std(std_socket)?;

        Ok(Self {
            socket: tokio_socket,
        })
    }

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
            if let Message::Hello(hello) = self.recv_message().await? {
                break hello;
            }
        })
    }

    async fn recv_message(&self) -> Result<Message, ProtoError> {
        let mut buf = [0u8; proto::MAX_DATAGRAM_SIZE];
        let (len, _src) = self.socket.recv_from(&mut buf).await?;
        Ok(Message::decode(&buf[..len])?)
    }

    async fn recv_transfer_message(&self, transfer_id: u64) -> Result<Message, ProtoError> {
        loop {
            let message = self.recv_message().await?;
            if message.transfer_id() == transfer_id {
                return Ok(message);
            }
        }
    }
}
