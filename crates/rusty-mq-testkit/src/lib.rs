//! Test utilities for rusty-mq (grows with M3+).
//!
//! Planned (PRD §15): deterministic clocks, failpoints for crash injection,
//! topology fixtures, and the independent durability oracle process
//! (§17.2). Only minimal helpers live here until the suites that need them
//! land — no speculative API.

/// A failpoint registry: named injection points that tests arm to observe or
/// abort at specific boundaries (journal append, fsync, snapshot publish).
///
/// The real failpoint machinery lands with M4; the type exists now so the
/// storage design accounts for it.
#[derive(Clone, Debug, Default)]
pub struct Failpoints {
    armed: std::collections::HashSet<&'static str>,
}

impl Failpoints {
    pub fn arm(&mut self, name: &'static str) {
        self.armed.insert(name);
    }

    pub fn disarm(&mut self, name: &'static str) {
        self.armed.remove(name);
    }

    pub fn is_armed(&self, name: &str) -> bool {
        self.armed.contains(name)
    }
}
