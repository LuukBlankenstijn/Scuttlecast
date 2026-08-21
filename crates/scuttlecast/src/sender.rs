use std::{net::Ipv4Addr, path::PathBuf};

use bon::Builder;
use futures_util::StreamExt;
use proto::{Data, Done, Hello, Message};
use tokio::io::AsyncRead;

use crate::{BLOCK_SIZE, error::ProtoError, transport::MessageSocket};

mod blocks;

#[derive(Builder)]
pub struct Sender {
    #[builder(with = |local_ip: Ipv4Addr, group_ip: Ipv4Addr, port: u16,| -> Result<_, ProtoError> { 
        MessageSocket::sending(local_ip, group_ip, port)
    } )]
    socket: MessageSocket,
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
        let blocks_per_slice = 32;

        let hello_message = Message::Hello(Hello {
            transfer_id,
            blocks_per_slice,
        });
        self.socket.send_to_group(hello_message).await?;

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
            self.socket.send_to_group(message).await?;
            block_no += 1;
        }

        let done_message = Message::Done(Done {
            transfer_id,
            total_bytes,
            total_blocks: block_no,
        });
        self.socket.send_to_group(done_message).await?;

        Ok(())
    }

}
