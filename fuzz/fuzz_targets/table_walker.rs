//! Coverage-guided fuzz: field-table budget validation over arbitrary
//! method payloads. Contract (T26/FR-P06): the walker bounds nesting and
//! counts BEFORE the recursive parser runs — arbitrary bytes must never
//! panic or recurse without bound, only pass or return a table error.
#![no_main]

use libfuzzer_sys::fuzz_target;
use rusty_mq_protocol::{tables, ProtocolLimits};

fuzz_target!(|data: &[u8]| {
    let limits = ProtocolLimits::default();
    let _ = tables::validate_method(data, &limits);
    let _ = tables::validate_content_header(data, &limits);
});
