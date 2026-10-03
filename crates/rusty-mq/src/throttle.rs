//! Auth-failure throttling (FR-S05): sliding-window limiters per peer and
//! broker-wide. Bounded state (peer-map capped, oldest evicted) — a
//! distributed attacker cannot grow memory.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Decision from [`AuthThrottle::check`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Throttled,
}

struct Window {
    failures: VecDeque<Instant>,
}

/// Sliding-window auth throttle.
pub struct AuthThrottle {
    per_peer: Mutex<Vec<(String, Window)>>,
    global: Mutex<Window>,
    window: Duration,
    per_peer_max: usize,
    global_max: usize,
}

impl AuthThrottle {
    pub fn new(window: Duration, per_peer_max: usize, global_max: usize) -> Self {
        Self {
            per_peer: Mutex::new(Vec::new()),
            global: Mutex::new(Window {
                failures: VecDeque::new(),
            }),
            window,
            per_peer_max,
            global_max,
        }
    }

    /// Would a new connection from `peer` be allowed right now? Does not
    /// mutate state (the connection handshake checks again on failure).
    pub fn check(&self, peer: &str) -> Decision {
        let now = Instant::now();
        if self.global_exceeds(now) {
            return Decision::Throttled;
        }
        if self.peer_exceeds(peer, now) {
            return Decision::Throttled;
        }
        Decision::Allow
    }

    /// Record an authentication failure for `peer`.
    pub fn record_failure(&self, peer: &str) {
        let now = Instant::now();
        self.global.lock().unwrap().failures.push_back(now);
        let mut peers = self.per_peer.lock().unwrap();
        // Bounded map: cap distinct tracked peers.
        const MAX_PEERS: usize = 4096;
        if let Some(idx) = peers.iter().position(|(p, _)| p == peer) {
            peers[idx].1.failures.push_back(now);
        } else {
            if peers.len() >= MAX_PEERS {
                // Evict the entry with the oldest last failure.
                if let Some(oldest) = peers
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, (_, w))| w.failures.back().copied())
                    .map(|(i, _)| i)
                {
                    peers.remove(oldest);
                }
            }
            peers.push((
                peer.to_string(),
                Window {
                    failures: VecDeque::from([now]),
                },
            ));
        }
    }

    fn global_exceeds(&self, now: Instant) -> bool {
        let mut g = self.global.lock().unwrap();
        prune(&mut g.failures, now, self.window);
        g.failures.len() >= self.global_max
    }

    fn peer_exceeds(&self, peer: &str, now: Instant) -> bool {
        let peers = self.per_peer.lock().unwrap();
        match peers.iter().find(|(p, _)| p == peer) {
            Some((_, w)) => {
                let mut failures = w.failures.clone();
                prune(&mut failures, now, self.window);
                failures.len() >= self.per_peer_max
            }
            None => false,
        }
    }
}

fn prune(failures: &mut VecDeque<Instant>, now: Instant, window: Duration) {
    while let Some(front) = failures.front() {
        if now.duration_since(*front) > window {
            failures.pop_front();
        } else {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn throttle() -> AuthThrottle {
        AuthThrottle::new(Duration::from_secs(1), 3, 5)
    }

    #[test]
    fn per_peer_limit_trips() {
        let t = throttle();
        for _ in 0..3 {
            assert_eq!(t.check("p"), Decision::Allow);
            t.record_failure("p");
        }
        assert_eq!(t.check("p"), Decision::Throttled);
        // Other peers unaffected until the global cap.
        assert_eq!(t.check("other"), Decision::Allow);
    }

    #[test]
    fn global_limit_trips() {
        let t = throttle();
        for i in 0..5 {
            t.record_failure(&format!("peer-{i}"));
        }
        assert_eq!(t.check("fresh-peer"), Decision::Throttled);
    }

    #[test]
    fn window_slides() {
        let t = AuthThrottle::new(Duration::from_millis(50), 2, 10);
        t.record_failure("p");
        t.record_failure("p");
        assert_eq!(t.check("p"), Decision::Throttled);
        std::thread::sleep(Duration::from_millis(80));
        assert_eq!(t.check("p"), Decision::Allow);
    }

    #[test]
    fn peer_map_is_bounded() {
        let t = throttle();
        for i in 0..5000 {
            t.record_failure(&format!("p{i}"));
        }
        assert!(t.per_peer.lock().unwrap().len() <= 4096 + 1);
    }
}
