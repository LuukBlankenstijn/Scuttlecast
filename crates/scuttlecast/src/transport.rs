use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::os::fd::AsRawFd;
use std::sync::Arc;

use bytes::Bytes;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use tokio::io::Interest;
use tokio::net::UdpSocket;

use crate::error::ProtoError;
use crate::proto::{
    CONTROL_TAG, Error as ProtocolError, Frame, FrameBatch, MAX_CONTROL_SIZE, Message,
};

mod offload;

/// Asked for on both sockets. The default of a couple of hundred kilobytes is
/// a few milliseconds of buffering at LAN speed, so a receiver pausing to
/// write to disk drops datagrams that were never lost in transit. The kernel
/// clamps this to `net.core.rmem_max` and `net.core.wmem_max`.
const BUFFER_SIZE: usize = 4 * 1024 * 1024;

const RECV_BUFFER_SIZE: usize = u16::MAX as usize + 1;

#[derive(Debug)]
pub enum Incoming {
    Shard(Frame),
    Control(Message),
}

impl Incoming {
    pub fn belongs_to(&self, transfer_id: u64) -> bool {
        match self {
            Incoming::Shard(frame) => frame.transfer_id == transfer_id as u32,
            Incoming::Control(message) => message.transfer_id() == transfer_id,
        }
    }
}

pub struct MessageSocket {
    socket: UdpSocket,
    group_address: SocketAddr,
    losing: Losing,
    buffer: Box<[u8]>,
}

impl MessageSocket {
    pub fn sending(local_ip: Ipv4Addr, group_ip: Ipv4Addr, port: u16) -> Result<Self, ProtoError> {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port).into())?;
        socket.set_multicast_if_v4(&local_ip)?;
        socket.set_send_buffer_size(BUFFER_SIZE)?;
        socket.set_nonblocking(true)?;

        Self::wrap(socket, (group_ip, port + 1).into())
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
        socket.set_recv_buffer_size(BUFFER_SIZE)?;
        socket.set_nonblocking(true)?;
        offload::enable_coalescing(socket.as_raw_fd())?;

        Self::wrap(socket, (group_ip, port + 1).into())
    }

    fn wrap(socket: Socket, group_address: SocketAddr) -> Result<Self, ProtoError> {
        let std_socket: std::net::UdpSocket = socket.into();

        Ok(Self {
            socket: UdpSocket::from_std(std_socket)?,
            group_address,
            losing: Losing::default(),
            buffer: vec![0; RECV_BUFFER_SIZE].into_boxed_slice(),
        })
    }

    /// Discards datagrams the loss rule rejects, which is how tests reproduce
    /// a lossy network exactly rather than hoping for one
    pub fn losing(mut self, losing: Losing) -> Self {
        self.losing = losing;
        self
    }

    pub async fn send_batch(&self, batch: &mut FrameBatch) -> Result<(), ProtoError> {
        if batch.is_empty() {
            return Ok(());
        }

        let group = SockAddr::from(self.group_address);
        let body = batch.filled();
        let segment_size = batch.segment_size();
        loop {
            self.socket.writable().await?;
            let attempt = self.socket.try_io(Interest::WRITABLE, || {
                offload::send_segmented(self.socket.as_raw_fd(), body, segment_size, &group)
            });

            match attempt {
                Ok(_) => break,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) => return Err(error.into()),
            }
        }
        batch.clear();

        Ok(())
    }

    pub async fn send_control(&self, message: Message, to: SocketAddr) -> Result<(), ProtoError> {
        let mut buf = [0u8; MAX_CONTROL_SIZE];
        let len = message.encode_into(&mut buf)?;
        self.socket.send_to(&buf[..len], to).await?;

        Ok(())
    }

    pub async fn send_to_group(&self, message: Message) -> Result<(), ProtoError> {
        self.send_control(message, self.group_address).await
    }

    pub async fn recv_control(
        &self,
        transfer: Option<u64>,
    ) -> Result<(Message, SocketAddr), ProtoError> {
        let mut buf = [0u8; MAX_CONTROL_SIZE];

        loop {
            let (len, from) = self.socket.recv_from(&mut buf).await?;
            let incoming = classify(Bytes::copy_from_slice(&buf[..len]))?;

            if self.losing.swallows(&incoming) {
                continue;
            }
            if let Incoming::Control(message) = incoming
                && transfer.is_none_or(|id| message.transfer_id() == id)
            {
                return Ok((message, from));
            }
        }
    }

    pub async fn recv_batch(
        &mut self,
        transfer_id: u64,
        out: &mut Vec<Incoming>,
    ) -> Result<(), ProtoError> {
        loop {
            let (len, stride) = self.read_coalesced().await?;
            let read = Bytes::copy_from_slice(&self.buffer[..len]);
            let stride = stride.unwrap_or(len).min(len);

            for at in (0..len).step_by(stride) {
                let incoming = classify(read.slice(at..(at + stride).min(len)))?;
                if incoming.belongs_to(transfer_id) && !self.losing.swallows(&incoming) {
                    out.push(incoming);
                }
            }

            if !out.is_empty() {
                return Ok(());
            }
        }
    }

    async fn read_coalesced(&mut self) -> Result<(usize, Option<usize>), ProtoError> {
        let Self { socket, buffer, .. } = self;

        loop {
            socket.readable().await?;
            let fd = socket.as_raw_fd();

            match socket.try_io(Interest::READABLE, || offload::recv_coalesced(fd, buffer)) {
                Ok(read) => return Ok(read),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
}

fn classify(datagram: Bytes) -> Result<Incoming, ProtocolError> {
    match datagram.first() {
        Some(&tag) if tag == CONTROL_TAG => Message::decode(&datagram).map(Incoming::Control),
        Some(_) => Frame::parse(datagram).map(Incoming::Shard),
        None => Err(ProtocolError::FrameTooShort(0)),
    }
}

type Rule = Arc<dyn Fn(&Incoming) -> bool + Send + Sync>;

/// A rule deciding which arriving datagrams to pretend never arrived
#[derive(Clone, Default)]
pub struct Losing(Option<Rule>);

impl Losing {
    pub fn every(rule: impl Fn(&Incoming) -> bool + Send + Sync + 'static) -> Self {
        Self(Some(Arc::new(rule)))
    }

    fn swallows(&self, incoming: &Incoming) -> bool {
        self.0.as_ref().is_some_and(|rule| rule(incoming))
    }
}
