use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

/// Read in one go: files and stdin are served by the blocking thread pool
const READ_AHEAD: usize = 1 << 20;

/// Blocks share the allocation they were read into
pub(crate) fn split<R: AsyncRead + Unpin>(
    mut reader: R,
    block_size: usize,
) -> impl futures_core::Stream<Item = std::io::Result<Bytes>> {
    async_stream::stream! {
        let mut slab = BytesMut::with_capacity(READ_AHEAD);

        loop {
            while slab.len() < block_size {
                match reader.read_buf(&mut slab).await {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(e) => { yield Err(e); return; }
                }
            }

            if slab.is_empty() { break; }
            yield Ok(slab.split_to(block_size.min(slab.len())).freeze());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::split;
    use futures_util::StreamExt;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, ReadBuf};

    async fn collect(reader: impl AsyncRead + Unpin, block_size: usize) -> Vec<Vec<u8>> {
        Box::pin(split(reader, block_size))
            .map(|block| block.expect("block").to_vec())
            .collect()
            .await
    }

    struct Dripping {
        remaining: Vec<u8>,
    }

    impl AsyncRead for Dripping {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.remaining.is_empty() {
                return Poll::Ready(Ok(()));
            }
            let byte = self.remaining.remove(0);
            buf.put_slice(&[byte]);
            Poll::Ready(Ok(()))
        }
    }

    struct Failing;

    impl AsyncRead for Failing {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::other("boom")))
        }
    }

    #[tokio::test]
    async fn empty_input_yields_no_blocks() {
        assert!(collect(&[][..], 4).await.is_empty());
    }

    #[tokio::test]
    async fn short_input_yields_one_partial_block() {
        assert_eq!(collect(&[1, 2, 3][..], 4).await, vec![vec![1, 2, 3]]);
    }

    #[tokio::test]
    async fn exact_multiple_yields_no_trailing_empty_block() {
        assert_eq!(
            collect(&[1, 2, 3, 4, 5, 6][..], 3).await,
            vec![vec![1, 2, 3], vec![4, 5, 6]]
        );
    }

    #[tokio::test]
    async fn one_byte_past_a_block_yields_a_single_byte_block() {
        assert_eq!(
            collect(&[1, 2, 3, 4][..], 3).await,
            vec![vec![1, 2, 3], vec![4]]
        );
    }

    #[tokio::test]
    async fn fills_blocks_across_short_reads() {
        let reader = Dripping {
            remaining: vec![1, 2, 3, 4, 5],
        };
        assert_eq!(collect(reader, 3).await, vec![vec![1, 2, 3], vec![4, 5]]);
    }

    #[tokio::test]
    async fn read_error_ends_the_stream() {
        let blocks: Vec<_> = Box::pin(split(Failing, 4)).collect().await;
        assert_eq!(blocks.len(), 1);
        assert!(blocks[0].is_err());
    }
}
