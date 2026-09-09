use bytes::Bytes;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::error::ProtoError;

/// Writes blocks in the order the net task hands them over. Slices only leave
/// the net task once every slice below them has, so a plain sequential write is
/// all the ordering this needs.
pub(super) async fn pump(
    mut blocks: mpsc::Receiver<Bytes>,
    mut writer: impl AsyncWrite + Unpin,
) -> Result<(), ProtoError> {
    while let Some(block) = blocks.recv().await {
        writer.write_all(&block).await.map_err(ProtoError::File)?;
    }

    writer.flush().await.map_err(ProtoError::File)
}
