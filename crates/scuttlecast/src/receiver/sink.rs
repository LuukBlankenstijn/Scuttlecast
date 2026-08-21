use std::{io, os::unix::fs::FileExt};

use derive_more::Constructor;

use crate::receiver::reorderer::Reorderer;

pub trait Sink {
    async fn write(&mut self, block_no: u32, offset: u64, bytes: &[u8]) -> io::Result<()>;
    async fn finish(&mut self) -> io::Result<()>;
}

#[derive(Debug, Constructor)]
pub struct FileSink {
    file: std::fs::File,
}
impl Sink for FileSink {
    async fn write(&mut self, _block_no: u32, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all_at(bytes, offset)
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
    async fn write(&mut self, block_no: u32, _offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.reorderer.on_block(block_no, bytes.to_vec()).await;
        Ok(())
    }

    async fn finish(&mut self) -> io::Result<()> {
        self.reorderer.flush().await;
        Ok(())
    }
}
