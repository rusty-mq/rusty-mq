//! T26 (frame layer): adversarial input over the incremental FrameReader —
//! arbitrary bytes, arbitrary chunk splits, oversized length fields, bad
//! frame-end octets. Invariants: no panic, no hang (bounded cases), no
//! unbounded allocation (every accepted allocation ≤ frame_max), and
//! malformed input is rejected, never interpreted.
//!
//! This is the stable-toolchain corpus harness; cargo-fuzz targets (same
//! entry points) run in CI with coverage feedback.

use proptest::prelude::*;

use rusty_mq_protocol::{FrameReader, MessageAssembler, NegotiatedLimits, ProtocolLimits};

fn limits(frame_max: u32) -> NegotiatedLimits {
    ProtocolLimits {
        max_frame_max: frame_max,
        ..Default::default()
    }
    .negotiate(16, frame_max, 0)
    .unwrap()
}

proptest! {
    /// Arbitrary bytes fed as one chunk: every outcome is Ok(frame),
    /// Ok(None) (incomplete), or a clean Err. The reader must never panic
    /// and its buffered bytes never exceed the budget.
    #[test]
    fn arbitrary_bytes_single_chunk(bytes in proptest::collection::vec(any::<u8>(), 0..8192)) {
        let l = limits(4096);
        let mut r = FrameReader::new_post_header(&l);
        if r.feed(&bytes).is_err() {
            return Ok(()); // backlog exceeded: a bounded rejection
        }
        loop {
            match r.next_frame() {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(_) => break, // malformed: rejected cleanly
            }
        }
        prop_assert!(r.buffered() <= 2 * 4096 + 64);
    }

    /// The same bytes split at arbitrary boundaries: the outcome set must
    /// be identical to the single-chunk feed (chunking is invisible).
    #[test]
    fn arbitrary_split_feeds_match_whole(
        bytes in proptest::collection::vec(any::<u8>(), 0..4096),
        split in 0usize..4096,
    ) {
        let l = limits(4096);
        let mut whole = FrameReader::new_post_header(&l);
        let mut split_reader = FrameReader::new_post_header(&l);
        if whole.feed(&bytes).is_err() {
            return Ok(());
        }
        split_reader.feed(&bytes[..split.min(bytes.len())]).unwrap();
        split_reader.feed(&bytes[split.min(bytes.len())..]).unwrap();
        let mut w = vec![];
        while let Ok(Some(f)) = whole.next_frame() { w.push(format!("{f:?}")); }
        let mut s = vec![];
        while let Ok(Some(f)) = split_reader.next_frame() { s.push(format!("{f:?}")); }
        prop_assert_eq!(w, s);
    }

    /// Oversized announced frames must reject BEFORE allocating: the
    /// declared length is never trusted past the budget.
    #[test]
    fn oversized_length_rejected_before_allocation(
        frame_type in 0u8..9,
        channel in any::<u16>(),
        claimed in 4097u32..u32::MAX,
    ) {
        let l = limits(4096);
        let mut r = FrameReader::new_post_header(&l);
        let mut bytes = vec![frame_type];
        bytes.extend_from_slice(&channel.to_be_bytes());
        bytes.extend_from_slice(&claimed.to_be_bytes());
        r.feed(&bytes).unwrap();
        prop_assert!(r.next_frame().is_err());
    }

    /// The frame-end octet is mandatory: any value != 0xCE rejects a
    /// well-formed-otherwise frame.
    #[test]
    fn bad_frame_end_rejects(
        body in proptest::collection::vec(any::<u8>(), 0..64),
        bad_end in proptest::bits::u8::ANY.prop_filter("must not be 0xCE", |v| *v != 0xCE),
    ) {
        let l = limits(4096);
        let mut r = FrameReader::new_post_header(&l);
        let size = body.len() as u32;
        let mut frame = vec![1u8, 0, 1];
        frame.extend_from_slice(&size.to_be_bytes());
        frame.extend_from_slice(&body);
        frame.push(bad_end);
        r.feed(&frame).unwrap();
        // Method payloads may fail parsing first; either way it NEVER
        // yields a frame silently.
        if let Ok(Some(_)) = r.next_frame() {
            // A method frame whose payload happened to parse is still
            // impossible here: the end octet check runs first.
            return Err(proptest::test_runner::TestCaseError::fail("bad end accepted"));
        }
    }

    /// MessageAssembler: announced body sizes beyond the cap are rejected
    /// at header time with 311 — before any body byte is buffered.
    #[test]
    fn assembler_caps_announced_size(
        announced in (16 * 1024 * 1024 + 1)..u32::MAX as u64,
    ) {
        let mut a = MessageAssembler::new(16 * 1024 * 1024);
        prop_assert!(a.start(60, announced).is_err());
    }

    /// Truncated bodies never complete; extra bytes never pass unnoticed.
    #[test]
    fn assembler_rejects_mismatch(
        announced in 1u64..64,
        delivered in 0usize..64,
    ) {
        let mut a = MessageAssembler::new(1024);
        a.start(60, announced).unwrap();
        let chunk = vec![0u8; delivered];
        // Only accepted while within the announced size.
        let _ = a.push_body(&chunk);
        prop_assert_eq!(a.is_complete(), delivered as u64 == announced);
    }
}
