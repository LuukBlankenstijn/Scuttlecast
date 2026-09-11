use bytes::Bytes;

use crate::proto::error::Error;

pub const HEADER_SIZE: usize = 20;

const VERSION: u8 = 1;

pub(crate) const FRAME_TAG: u8 = VERSION << 4;
pub(crate) const CONTROL_TAG: u8 = VERSION << 4 | 0xF;

const MAX_IP_DATAGRAM: usize = u16::MAX as usize;
const UDP_MAX_SEGMENTS: usize = 128;

#[derive(Debug, PartialEq)]
pub struct Frame {
    pub slot: u16,
    pub slice_parity: u8,
    pub transfer_id: u32,
    pub seq: u32,
    pub slice_no: u32,
    pub emit_floor: u32,
    pub payload: Bytes,
}

impl Frame {
    pub fn header(&self) -> [u8; HEADER_SIZE] {
        let mut header = [0u8; HEADER_SIZE];

        header[0] = FRAME_TAG;
        header[1] = self.slice_parity;
        header[2..4].copy_from_slice(&self.slot.to_le_bytes());
        header[4..8].copy_from_slice(&self.transfer_id.to_le_bytes());
        header[8..12].copy_from_slice(&self.seq.to_le_bytes());
        header[12..16].copy_from_slice(&self.slice_no.to_le_bytes());
        header[16..20].copy_from_slice(&self.emit_floor.to_le_bytes());

        header
    }

    pub fn parse(datagram: Bytes) -> Result<Self, Error> {
        if datagram.len() <= HEADER_SIZE {
            return Err(Error::FrameTooShort(datagram.len()));
        }
        if datagram[0] != FRAME_TAG {
            return Err(Error::UnknownTag(datagram[0]));
        }

        let field =
            |at: usize| u32::from_le_bytes(datagram[at..at + 4].try_into().expect("four bytes"));

        Ok(Self {
            slot: u16::from_le_bytes(datagram[2..4].try_into().expect("two bytes")),
            slice_parity: datagram[1],
            transfer_id: field(4),
            seq: field(8),
            slice_no: field(12),
            emit_floor: field(16),
            payload: datagram.slice(HEADER_SIZE..),
        })
    }
}

pub(crate) struct FrameBatch {
    staging: Vec<u8>,
    segment_size: usize,
    segments: usize,
    limit: usize,
}

impl FrameBatch {
    pub(crate) fn new(block_size: usize, max_segments: usize) -> Self {
        let segment_size = HEADER_SIZE + block_size;
        let capacity = segments_per_datagram(segment_size).min(max_segments.max(1));

        Self {
            staging: vec![0; capacity * segment_size],
            segment_size,
            segments: 0,
            limit: capacity,
        }
    }

    pub(crate) fn segment_size(&self) -> usize {
        self.segment_size
    }

    pub(crate) fn capacity(&self) -> usize {
        self.staging.len() / self.segment_size
    }

    pub(crate) fn narrow_to(&mut self, segments: usize) {
        self.limit = segments.clamp(1, self.capacity());
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.segments == 0
    }

    pub(crate) fn is_full(&self) -> bool {
        self.segments >= self.limit
    }

    pub(crate) fn push(&mut self, frame: &Frame) {
        let at = self.segments * self.segment_size;
        let segment = &mut self.staging[at..at + self.segment_size];

        segment[..HEADER_SIZE].copy_from_slice(&frame.header());
        segment[HEADER_SIZE..HEADER_SIZE + frame.payload.len()].copy_from_slice(&frame.payload);
        self.segments += 1;
    }

    pub(crate) fn filled(&self) -> &[u8] {
        &self.staging[..self.segments * self.segment_size]
    }

    pub(crate) fn clear(&mut self) {
        self.segments = 0;
    }
}

fn segments_per_datagram(segment_size: usize) -> usize {
    (MAX_IP_DATAGRAM / segment_size).clamp(1, UDP_MAX_SEGMENTS)
}

#[cfg(test)]
mod tests {
    use super::{CONTROL_TAG, Frame, FrameBatch, HEADER_SIZE, segments_per_datagram};
    use crate::proto::error::Error;
    use bytes::Bytes;
    use proptest::prelude::*;

    const UNCAPPED: usize = usize::MAX;

    fn frame(slot: u16, payload: Bytes) -> Frame {
        Frame {
            slot,
            slice_parity: 8,
            transfer_id: 0xdead_beef,
            seq: 0x0102_0304,
            slice_no: 9,
            emit_floor: 8,
            payload,
        }
    }

    fn datagram_of(frame: &Frame) -> Bytes {
        let mut datagram = vec![0u8; HEADER_SIZE + frame.payload.len()];
        datagram[..HEADER_SIZE].copy_from_slice(&frame.header());
        datagram[HEADER_SIZE..].copy_from_slice(&frame.payload);
        Bytes::from(datagram)
    }

    proptest! {
        #[test]
        fn roundtrips_through_a_datagram(
            slot: u16,
            slice_parity: u8,
            transfer_id: u32,
            seq: u32,
            slice_no: u32,
            emit_floor: u32,
            payload in proptest::collection::vec(any::<u8>(), 1..64),
        ) {
            let original = Frame {
                slot,
                slice_parity,
                transfer_id,
                seq,
                slice_no,
                emit_floor,
                payload: Bytes::from(payload),
            };

            prop_assert_eq!(Frame::parse(datagram_of(&original)).unwrap(), original);
        }

        #[test]
        fn a_header_is_the_same_width_whatever_it_carries(
            slot: u16,
            slice_parity: u8,
            transfer_id: u32,
            seq: u32,
            slice_no: u32,
            emit_floor: u32,
        ) {
            let payload = Bytes::from_static(b"x");
            let written = datagram_of(&Frame {
                slot,
                slice_parity,
                transfer_id,
                seq,
                slice_no,
                emit_floor,
                payload: payload.clone(),
            });

            prop_assert_eq!(written.len(), HEADER_SIZE + payload.len());
        }
    }

    #[test]
    fn rejects_a_datagram_carrying_no_payload() {
        assert!(matches!(
            Frame::parse(datagram_of(&frame(0, Bytes::new()))),
            Err(Error::FrameTooShort(HEADER_SIZE))
        ));
    }

    #[test]
    fn rejects_a_control_datagram() {
        let mut datagram = vec![0u8; HEADER_SIZE + 4];
        datagram[0] = CONTROL_TAG;

        assert!(
            matches!(Frame::parse(Bytes::from(datagram)), Err(Error::UnknownTag(tag)) if tag == CONTROL_TAG)
        );
    }

    #[test]
    fn a_shard_names_the_parity_its_slice_carries() {
        let staged = datagram_of(&frame(3, Bytes::from_static(b"payload")));

        assert_eq!(Frame::parse(staged).expect("parses").slice_parity, 8);
    }

    #[test]
    fn a_batch_fills_the_largest_datagram_the_kernel_takes() {
        assert_eq!(segments_per_datagram(HEADER_SIZE + 1452), 44);
        assert_eq!(segments_per_datagram(HEADER_SIZE + 8952), 7);
        assert_eq!(segments_per_datagram(HEADER_SIZE + 100), 128);
        assert_eq!(segments_per_datagram(70_000), 1);
    }

    #[test]
    fn staged_segments_parse_back_at_a_fixed_stride() {
        let mut batch = FrameBatch::new(8, UNCAPPED);
        let stride = batch.segment_size();

        for slot in 0..3u16 {
            batch.push(&frame(slot, Bytes::from(vec![slot as u8; 8])));
        }

        assert_eq!(batch.filled().len(), 3 * stride);

        let filled = Bytes::copy_from_slice(batch.filled());
        for slot in 0..3u16 {
            let at = slot as usize * stride;
            let parsed = Frame::parse(filled.slice(at..at + stride)).expect("parses");

            assert_eq!(parsed.slot, slot);
            assert_eq!(&parsed.payload[..], &[slot as u8; 8]);
        }
    }

    #[test]
    fn a_batch_honours_a_lower_segment_cap() {
        assert_eq!(FrameBatch::new(1452, 8).capacity(), 8);
        assert_eq!(FrameBatch::new(1452, 0).capacity(), 1);
        assert_eq!(FrameBatch::new(1452, UNCAPPED).capacity(), 44);
    }

    #[test]
    fn clearing_reuses_the_staging_buffer() {
        let mut batch = FrameBatch::new(4, UNCAPPED);
        while !batch.is_full() {
            batch.push(&frame(0, Bytes::from_static(b"abcd")));
        }

        batch.clear();

        assert!(batch.is_empty());
        assert_eq!(batch.capacity(), FrameBatch::new(4, UNCAPPED).capacity());
    }
}
