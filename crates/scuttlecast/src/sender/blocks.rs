use tokio::io::{AsyncRead, AsyncReadExt};

pub(crate) fn split<R: AsyncRead + Unpin>(
    mut reader: R,
    block_size: usize,
) -> impl futures_core::Stream<Item = std::io::Result<Vec<u8>>> {
    async_stream::stream! {
        loop {
            let mut buf = vec![0u8; block_size];
            let mut filled = 0;

            while filled < block_size {
                match reader.read(&mut buf[filled..]).await {
                    Ok(0) => break,
                    Ok(n) => filled += n,
                    Err(e) => { yield Err(e); return; }
                }
            }

            if filled == 0 { break; }
            buf.truncate(filled);
            yield Ok(buf);
        }
    }
}
