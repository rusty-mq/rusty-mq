//! Coverage-guided fuzz: incremental frame decoding over arbitrary bytes.
//! Contract (T26): no panic, no hang, no unbounded allocation — arbitrary
//! input either yields frames, errors, or waits for more bytes.
#![no_main]

use libfuzzer_sys::fuzz_target;
use rusty_mq_protocol::{FrameReader, ProtocolLimits};

fuzz_target!(|data: &[u8]| {
    let limits = ProtocolLimits::default();
    let negotiated = limits
        .negotiate(limits.max_channel_max, limits.max_frame_max, 0)
        .expect("server's own proposal always negotiates");
    let mut reader = FrameReader::new_post_header(&negotiated);
    // Feed in small chunks to exercise the incremental boundary logic.
    for chunk in data.chunks(7) {
        if reader.feed(chunk).is_err() {
            return; // budget exceeded: legal bounded rejection
        }
        while let Ok(Some(_)) = reader.next_frame() {
            // decoded a frame; buffer stays bounded by the reader
        }
    }
});
