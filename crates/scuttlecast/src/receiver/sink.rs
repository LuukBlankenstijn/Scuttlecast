use std::{io, os::unix::fs::FileExt};

use bytes::Bytes;
use derive_more::Constructor;

use crate::receiver::reorderer::Reorderer;

pub trait Sink {
    async fn write(&mut self, block_no: u64, offset: u64, payload: Bytes) -> io::Result<()>;
    async fn finish(&mut self) -> io::Result<()>;
}

#[derive(Debug, Constructor)]
pub struct FileSink {
    file: std::fs::File,
}
impl Sink for FileSink {
    async fn write(&mut self, _block_no: u64, offset: u64, payload: Bytes) -> io::Result<()> {
        self.file.write_all_at(&payload, offset)
    }

    async fn finish(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Constructor)]
pub struct StreamSink {
    reorderer: Reorderer,
}
impl Sink for StreamSink {
    async fn write(&mut self, block_no: u64, _offset: u64, payload: Bytes) -> io::Result<()> {
        self.reorderer.on_block(block_no, payload).await;
        Ok(())
    }

    async fn finish(&mut self) -> io::Result<()> {
        self.reorderer.flush().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{FileSink, Sink};
    use bytes::Bytes;
    use std::fs;

    fn sink(path: &std::path::Path) -> FileSink {
        FileSink::new(fs::File::create(path).expect("create"))
    }

    #[tokio::test]
    async fn writes_blocks_at_their_offsets_regardless_of_arrival_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("out.bin");
        let mut sink = sink(&path);

        sink.write(2, 8, Bytes::from_static(b"CCCC"))
            .await
            .expect("write");
        sink.write(0, 0, Bytes::from_static(b"AAAA"))
            .await
            .expect("write");
        sink.write(1, 4, Bytes::from_static(b"BBBB"))
            .await
            .expect("write");
        sink.finish().await.expect("finish");

        assert_eq!(fs::read(&path).expect("read"), b"AAAABBBBCCCC");
    }

    #[tokio::test]
    async fn short_final_block_sets_file_length() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("out.bin");
        let mut sink = sink(&path);

        sink.write(0, 0, Bytes::from_static(b"AAAA"))
            .await
            .expect("write");
        sink.write(1, 4, Bytes::from_static(b"BB"))
            .await
            .expect("write");
        sink.finish().await.expect("finish");

        assert_eq!(fs::read(&path).expect("read"), b"AAAABB");
    }

    #[tokio::test]
    async fn gap_between_blocks_reads_back_as_zeroes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("out.bin");
        let mut sink = sink(&path);

        sink.write(0, 0, Bytes::from_static(b"AAAA"))
            .await
            .expect("write");
        sink.write(2, 8, Bytes::from_static(b"CCCC"))
            .await
            .expect("write");
        sink.finish().await.expect("finish");

        assert_eq!(fs::read(&path).expect("read"), b"AAAA\0\0\0\0CCCC");
    }
}
