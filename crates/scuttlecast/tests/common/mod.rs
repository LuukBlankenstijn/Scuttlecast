#![allow(dead_code)]

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::time::Duration;

use scuttlecast::receiver::Receiver;
use scuttlecast::sender::Sender;
use tempfile::TempDir;

pub const LOCAL: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);
pub const MAX_WAIT: Duration = Duration::from_secs(10);

pub fn group(id: u8) -> Ipv4Addr {
    Ipv4Addr::new(239, 255, 42, id)
}

pub fn payload(len: usize) -> Vec<u8> {
    let mut state = 0x2545_f491_4f6c_dd1du64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 33) as u8
        })
        .collect()
}

pub fn receiver(group_ip: Ipv4Addr, port: u16) -> Receiver {
    Receiver::builder()
        .socket(LOCAL, group_ip, port)
        .expect("bind receiver")
        .max_wait(MAX_WAIT)
        .build()
}

pub fn sender(group_ip: Ipv4Addr, port: u16, min_receivers: usize) -> Sender {
    Sender::builder()
        .socket(LOCAL, group_ip, port)
        .expect("bind sender")
        .min_receivers(min_receivers)
        .max_wait(MAX_WAIT)
        .build()
}

pub fn sender_with(
    group_ip: Ipv4Addr,
    port: u16,
    min_receivers: usize,
    max_wait: Duration,
) -> Sender {
    Sender::builder()
        .socket(LOCAL, group_ip, port)
        .expect("bind sender")
        .min_receivers(min_receivers)
        .max_wait(max_wait)
        .build()
}

pub struct Output {
    dir: TempDir,
}

impl Output {
    pub fn new() -> Self {
        Self {
            dir: tempfile::tempdir().expect("tempdir"),
        }
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }
}

pub async fn transfer_to_file(group_id: u8, port: u16, bytes: &[u8]) -> Vec<u8> {
    let output = Output::new();
    let path = output.path("received.bin");
    let group_ip = group(group_id);

    let receiver = receiver(group_ip, port);
    let receiving = tokio::spawn({
        let path = path.clone();
        async move { receiver.recv_file(path).await }
    });

    let sender = sender(group_ip, port, 1);
    sender
        .send_stream(bytes.to_vec().as_slice())
        .await
        .expect("send");

    receiving.await.expect("join").expect("receive");
    std::fs::read(&path).expect("read output")
}
