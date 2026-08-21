use std::{
    net::{Ipv4Addr, SocketAddrV4},
    path::PathBuf,
};

use futures_util::StreamExt;
use proto::{Data, Done, Hello, Message};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{io::AsyncRead, net::UdpSocket};

use crate::{BLOCK_SIZE, error::ProtoError};

mod blocks;

pub struct Sender {
    socket: UdpSocket,
    group_address: Ipv4Addr,
    group_port: u16,
}

impl Sender {
    pub fn new(
        local_address: Ipv4Addr,
        group_address: Ipv4Addr,
        group_port: u16,
    ) -> Result<Self, ProtoError> {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0).into())?;
        socket.set_multicast_if_v4(&local_address)?;
        socket.set_nonblocking(true)?;
        let std_socket: std::net::UdpSocket = socket.into();
        let tokio_socket = UdpSocket::from_std(std_socket)?;

        Ok(Self {
            socket: tokio_socket,
            group_address,
            group_port,
        })
    }

    pub async fn send_file(&self, path: PathBuf) -> Result<(), ProtoError> {
        let stream = tokio::fs::File::open(path)
            .await
            .map_err(ProtoError::File)?;
        self.send(stream).await?;
        Ok(())
    }

    pub async fn send(&self, reader: impl AsyncRead + Unpin) -> Result<(), ProtoError> {
        let transfer_id = rand::random();
        let blocks_per_slice = 32;

        let hello_message = Message::Hello(Hello {
            transfer_id,
            blocks_per_slice,
        });
        self.send_message(hello_message).await?;

        let mut block_no = 0;
        let mut stream = Box::pin(blocks::split(reader, BLOCK_SIZE));
        let mut total_bytes = 0;
        while let Some(block) = stream.next().await {
            let block = block.map_err(ProtoError::File)?;
            total_bytes += block.len() as u64;
            let message = Message::Data(Data {
                transfer_id,
                slice_no: Data::slice_no(block_no, blocks_per_slice),
                block_in_slice: Data::block_in_slice(block_no, blocks_per_slice),
                payload: block,
            });
            self.send_message(message).await?;
            block_no += 1;
        }

        let done_message = Message::Done(Done {
            transfer_id,
            total_bytes,
            total_blocks: block_no,
        });
        self.send_message(done_message).await?;

        Ok(())
    }

    async fn send_message(&self, message: Message) -> Result<(), ProtoError> {
        let bytes = message.encode()?;
        self.socket
            .send_to(&bytes, (self.group_address, self.group_port))
            .await?;
        Ok(())
    }
}
