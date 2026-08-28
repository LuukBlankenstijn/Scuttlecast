use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use bon::Builder;
use futures_util::StreamExt;
use proto::{Data, Done, Hello, Message};
use tokio::{io::AsyncRead, time::Instant};
use tracing::debug;

use crate::{
    BLOCK_SIZE,
    error::ProtoError,
    sender::pacer::{Pacer, RateController, TICK_INTERVAL},
    transport::MessageSocket,
};

mod pacer;
mod slicer;

#[derive(Builder)]
pub struct Sender {
    #[builder(with = |local_ip: Ipv4Addr, group_ip: Ipv4Addr, port: u16,| -> Result<_, ProtoError> {
        MessageSocket::sending(local_ip, group_ip, port)
    } )]
    socket: MessageSocket,
    #[builder(default = Duration::new(5 * 60, 0))]
    max_wait: Duration,
    min_receivers: Option<usize>,
    #[builder(default = 32)]
    blocks_per_slice: u32,
}

impl Sender {
    pub async fn send_file(&self, path: PathBuf) -> Result<(), ProtoError> {
        let stream = tokio::fs::File::open(path)
            .await
            .map_err(ProtoError::File)?;
        self.send_stream(stream).await?;
        Ok(())
    }

    pub async fn send_stream(&self, reader: impl AsyncRead + Unpin) -> Result<(), ProtoError> {
        let transfer_id = rand::random();

        let mut participants = self.gather_participants(transfer_id).await?;
        debug!("starting send with {} participants", participants.len());

        let mut block_no = 0;
        let mut stream = Box::pin(slicer::blocks::split(reader, BLOCK_SIZE));
        let mut total_bytes = 0;
        let mut rate_controller = RateController::new();
        let mut pacer = Pacer::new();
        let mut tick = tokio::time::interval(TICK_INTERVAL);
        loop {
            let rate = rate_controller.rate();
            pacer.refill(rate);
            let has_credit = pacer.has_credit();
            let until_credit = pacer.time_until_credit(rate);

            tokio::select! {
                s = stream.next(), if has_credit => {
                    let Some(block) = s else {
                        break;
                    };
                    pacer.consume();

                    let block = block.map_err(ProtoError::File)?;
                    total_bytes += block.len() as u64;
                    let message = Message::Data(Data {
                        transfer_id,
                        slice_no: Data::slice_no(block_no, self.blocks_per_slice),
                        block_in_slice: Data::block_in_slice(block_no, self.blocks_per_slice),
                        payload: block.into(),
                    });
                    self.socket.send_to_group(message).await?;
                    block_no += 1;
                }

                _ = tokio::time::sleep(until_credit), if !has_credit => {}

                _ = tick.tick() => {
                    rate_controller.tick(pacer.take_starvation());
                },

                m = self.socket.recv_in_transfer(transfer_id) => {
                    let (message, _) = m?;
                    match message {
                        Message::Stats(stats) => {
                            if !participants.contains_key(&stats.receiver_id) {
                                continue;
                            }
                            rate_controller.on_report(
                                stats.receiver_id,
                                stats.blocks_received,
                                stats.blocks_expected
                            );
                        }
                        Message::Leave(_, participant_id) => {
                            participants.remove(&participant_id);
                        }
                        _ => {}
                    }
                }
            }
        }

        let done_message = Message::Done(Done {
            transfer_id,
            total_bytes,
            total_blocks: block_no,
        });
        self.socket.send_to_group(done_message).await?;

        Ok(())
    }

    async fn gather_participants(
        &self,
        transfer_id: u64,
    ) -> Result<HashMap<u64, SocketAddr>, ProtoError> {
        let mut participants = HashMap::new();
        let start = Instant::now();
        let deadline = start + self.max_wait;
        let mut hello_tick = tokio::time::interval(Duration::from_millis(200));

        loop {
            tokio::select! {
                _ = hello_tick.tick() => {
                    self.socket
                        .send_to_group(Message::Hello(Hello {
                            transfer_id,
                            blocks_per_slice: self.blocks_per_slice,
                        }))
                        .await?
                }

                r = self.socket.recv_in_transfer(transfer_id) => {
                    let (message, socket) = r?;
                    if let Message::Join(_, receiver_id) = message && participants.insert(receiver_id, socket).is_none() {
                        tracing::debug!(socket=%socket, receiver_id, "receiver joined");
                    }
                    if let Message::Leave(_, receiver_id) = message && participants.remove(&receiver_id).is_some() {
                        tracing::debug!(socket=%socket, receiver_id, "receiver left");
                    }
                },

                _ = tokio::time::sleep_until(deadline) => break,
            }

            if Some(participants.len()) >= self.min_receivers {
                break;
            }
        }
        Ok(participants)
    }
}
