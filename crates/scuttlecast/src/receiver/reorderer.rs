use std::collections::BTreeMap;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct Reorderer {
    next_block: u32,
    pending: BTreeMap<u32, Vec<u8>>,
    tx: mpsc::Sender<Vec<u8>>,
}

impl Reorderer {
    pub fn new() -> (Self, mpsc::Receiver<Vec<u8>>) {
        let (tx, rx) = mpsc::channel(32);

        (
            Self {
                next_block: Default::default(),
                pending: Default::default(),
                tx,
            },
            rx,
        )
    }
    pub async fn on_block(&mut self, block_no: u32, payload: Vec<u8>) {
        if block_no < self.next_block {
            return;
        }
        self.pending.insert(block_no, payload);

        self.flush().await;
    }

    pub async fn flush(&mut self) {
        while let Some(payload) = self.pending.remove(&self.next_block) {
            if self.tx.send(payload).await.is_err() {
                return;
            }
            self.next_block += 1;
        }
    }
}
