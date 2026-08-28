use bytes::Bytes;
use std::collections::BTreeMap;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct Reorderer {
    next_block: u64,
    pending: BTreeMap<u64, Bytes>,
    tx: mpsc::Sender<Bytes>,
}

impl Reorderer {
    pub fn new() -> (Self, mpsc::Receiver<Bytes>) {
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
    pub async fn on_block(&mut self, block_no: u64, payload: Bytes) {
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

#[cfg(test)]
mod tests {
    use super::Reorderer;
    use bytes::Bytes;
    use tokio::sync::mpsc::error::TryRecvError;

    #[tokio::test]
    async fn emits_blocks_arriving_in_order() {
        let (mut reorderer, mut rx) = Reorderer::new();

        reorderer.on_block(0, Bytes::from_static(&[1])).await;
        reorderer.on_block(1, Bytes::from_static(&[2])).await;

        assert_eq!(rx.try_recv(), Ok(Bytes::from_static(&[1])));
        assert_eq!(rx.try_recv(), Ok(Bytes::from_static(&[2])));
    }

    #[tokio::test]
    async fn holds_blocks_until_the_gap_is_filled() {
        let (mut reorderer, mut rx) = Reorderer::new();

        reorderer.on_block(2, Bytes::from_static(&[3])).await;
        reorderer.on_block(1, Bytes::from_static(&[2])).await;
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

        reorderer.on_block(0, Bytes::from_static(&[1])).await;

        assert_eq!(rx.try_recv(), Ok(Bytes::from_static(&[1])));
        assert_eq!(rx.try_recv(), Ok(Bytes::from_static(&[2])));
        assert_eq!(rx.try_recv(), Ok(Bytes::from_static(&[3])));
    }

    #[tokio::test]
    async fn ignores_blocks_already_emitted() {
        let (mut reorderer, mut rx) = Reorderer::new();

        reorderer.on_block(0, Bytes::from_static(&[1])).await;
        assert_eq!(rx.try_recv(), Ok(Bytes::from_static(&[1])));

        reorderer.on_block(0, Bytes::from_static(&[99])).await;
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn later_duplicate_overwrites_pending_block() {
        let (mut reorderer, mut rx) = Reorderer::new();

        reorderer.on_block(1, Bytes::from_static(&[2])).await;
        reorderer.on_block(1, Bytes::from_static(&[2])).await;
        reorderer.on_block(0, Bytes::from_static(&[1])).await;

        assert_eq!(rx.try_recv(), Ok(Bytes::from_static(&[1])));
        assert_eq!(rx.try_recv(), Ok(Bytes::from_static(&[2])));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn flush_drops_blocks_stranded_behind_a_missing_one() {
        let (mut reorderer, mut rx) = Reorderer::new();

        reorderer.on_block(1, Bytes::from_static(&[2])).await;
        reorderer.on_block(2, Bytes::from_static(&[3])).await;
        reorderer.flush().await;

        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }
}
