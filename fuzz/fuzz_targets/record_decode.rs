//! Coverage-guided fuzz: journal record decoding over arbitrary bytes.
//! Contract (T26): no panic; arbitrary (kind, payload) either decodes to
//! a well-formed record or errors — never silent corruption acceptance.
#![no_main]

use libfuzzer_sys::fuzz_target;
use rusty_mq_storage::record::Record;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    // First byte chooses the record kind; the rest is the payload.
    let _ = Record::decode(data[0], &data[1..]);
});
