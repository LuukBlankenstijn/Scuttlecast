use bytes::Bytes;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;

use crate::error::ProtoError;

const WRITE_BEHIND: usize = 1 << 20;

pub(super) async fn pump(
    mut blocks: mpsc::Receiver<Bytes>,
    writer: impl AsyncWrite + Unpin,
) -> Result<(), ProtoError> {
    let mut writer = BufWriter::with_capacity(WRITE_BEHIND, writer);

    while let Some(block) = blocks.recv().await {
        writer.write_all(&block).await.map_err(ProtoError::File)?;
    }

    writer.flush().await.map_err(ProtoError::File)
}
