use bytes::Bytes;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;

use crate::error::ProtoError;

/// Written in one go rather than a block at a time. Files and stdout are both
/// served by the blocking thread pool, so an unbuffered block-sized write
/// costs a task hand-off to another thread and back, which dwarfs the write.
const WRITE_BEHIND: usize = 1 << 20;

/// Writes blocks in the order the net task hands them over. Slices only leave
/// the net task once every slice below them has, so a plain sequential write is
/// all the ordering this needs.
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
