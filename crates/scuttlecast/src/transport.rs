use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

use proto::Message;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;

use crate::error::ProtoError;

pub struct MessageSocket {
    socket: UdpSocket,
    group_address: SocketAddr,
    losing: Losing,
}

impl MessageSocket {
    pub fn sending(local_ip: Ipv4Addr, group_ip: Ipv4Addr, port: u16) -> Result<Self, ProtoError> {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port).into())?;
        socket.set_multicast_if_v4(&local_ip)?;
        socket.set_nonblocking(true)?;
        let std_socket: std::net::UdpSocket = socket.into();
        let tokio_socket = UdpSocket::from_std(std_socket)?;

        Ok(Self {
            socket: tokio_socket,
            group_address: (group_ip, port + 1).into(),
            losing: Losing::default(),
        })
    }

    pub fn receiving(
        local_ip: Ipv4Addr,
        group_ip: Ipv4Addr,
        port: u16,
    ) -> Result<Self, ProtoError> {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_reuse_address(true)?;
        socket.set_reuse_port(true)?;
        socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port + 1).into())?;
        socket.join_multicast_v4(&group_ip, &local_ip)?;
        socket.set_nonblocking(true)?;
        let std_socket: std::net::UdpSocket = socket.into();
        let tokio_socket = UdpSocket::from_std(std_socket)?;

        Ok(Self {
            socket: tokio_socket,
            group_address: (group_ip, port + 1).into(),
            losing: Losing::default(),
        })
    }

    /// Discards datagrams the loss rule rejects, which is how tests reproduce
    /// a lossy network exactly rather than hoping for one
    pub fn losing(mut self, losing: Losing) -> Self {
        self.losing = losing;
        self
    }

    pub async fn recv_from(&self) -> Result<(Message, SocketAddr), ProtoError> {
        loop {
            let mut buf = [0u8; proto::MAX_DATAGRAM_SIZE];
            let (len, src) = self.socket.recv_from(&mut buf).await?;
            let message = Message::decode(&buf[..len])?;

            if !self.losing.swallows(&message) {
                return Ok((message, src));
            }
        }
    }

    /// Returns the first message with equal transfer_id, discards the rest
    pub async fn recv_in_transfer(
        &self,
        transfer_id: u64,
    ) -> Result<(Message, SocketAddr), ProtoError> {
        loop {
            let (message, socket) = self.recv_from().await?;
            if message.transfer_id() == transfer_id {
                return Ok((message, socket));
            }
        }
    }

    pub async fn send_to(&self, message: Message, to: SocketAddr) -> Result<(), ProtoError> {
        let bytes = message.encode()?;
        self.socket.send_to(&bytes, to).await?;
        Ok(())
    }

    pub async fn send_to_group(&self, message: Message) -> Result<(), ProtoError> {
        let bytes = message.encode()?;
        self.socket.send_to(&bytes, self.group_address).await?;
        Ok(())
    }
}

/// A rule deciding which arriving datagrams to pretend never arrived
#[derive(Clone, Default)]
pub struct Losing(Option<Arc<dyn Fn(&Message) -> bool + Send + Sync>>);

impl Losing {
    pub fn every(rule: impl Fn(&Message) -> bool + Send + Sync + 'static) -> Self {
        Self(Some(Arc::new(rule)))
    }

    fn swallows(&self, message: &Message) -> bool {
        self.0.as_ref().is_some_and(|rule| rule(message))
    }
}
