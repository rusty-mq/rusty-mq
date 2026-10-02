//! Resource alarms (§10, FR-R03/R04): memory and disk states with
//! transitions that notify live connections (FR-R07) and quiesce durable
//! admissions (§6.4 — never a false confirm).
//!
//! Memory: the in-memory store's byte total against its budget, with
//! hysteresis (raise at the budget, clear at the low watermark) so a
//! single settle doesn't flap the state.
//!
//! Disk: free space on the data volume against
//! `max(disk_free_min_bytes, disk_free_min_ratio × volume)`. The probe is
//! statvfs(2) (the single narrowly-scoped unsafe besides the LOCK liveness
//! kill(2)) or an injected value in tests.

use std::path::Path;

/// Raised/cleared resource alarms plus the notification reasons.
#[derive(Clone, Debug, PartialEq)]
pub struct Alarms {
    memory: bool,
    disk: bool,
    /// Test hook: when set, replaces the statvfs probe.
    injected_free_bytes: Option<u64>,
    /// Test hook: pretend the volume is this large (ratio math).
    volume_bytes: u64,
    pub disk_free_min_bytes: u64,
    pub disk_free_min_ratio: f64,
}

pub const DEFAULT_DISK_FREE_MIN_BYTES: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_DISK_FREE_MIN_RATIO: f64 = 0.10;
/// Memory alarm clears at this fraction of the budget.
pub const MEMORY_CLEAR_FRACTION: f64 = 0.5;

impl Default for Alarms {
    fn default() -> Self {
        Self {
            memory: false,
            disk: false,
            injected_free_bytes: None,
            volume_bytes: 0,
            disk_free_min_bytes: DEFAULT_DISK_FREE_MIN_BYTES,
            disk_free_min_ratio: DEFAULT_DISK_FREE_MIN_RATIO,
        }
    }
}

/// Which transitions happened in an evaluation (callers notify on these).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Transitions {
    pub memory_raised: bool,
    pub memory_cleared: bool,
    pub disk_raised: bool,
    pub disk_cleared: bool,
}

impl Transitions {
    pub fn any(&self) -> bool {
        self.memory_raised || self.memory_cleared || self.disk_raised || self.disk_cleared
    }
}

#[cfg(unix)]
fn free_bytes_of(path: &Path) -> Option<u64> {
    let c = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).ok()?;
    // Safety: statvfs(2) writes into a valid, zeroed buffer; the path is a
    // NUL-terminated CString. Narrowly scoped per PRD §15.2 (documented
    // exception; the workspace lint set fails on any other unsafe).
    #[allow(unsafe_code)]
    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        let rc = libc::statvfs(c.as_ptr(), &mut stat);
        if rc != 0 {
            return None;
        }
        Some(stat.f_bavail as u64 * stat.f_bsize as u64)
    }
}

#[cfg(not(unix))]
fn free_bytes_of(_path: &Path) -> Option<u64> {
    None
}

impl Alarms {
    pub fn memory(&self) -> bool {
        self.memory
    }

    pub fn disk(&self) -> bool {
        self.disk
    }

    pub fn any_raised(&self) -> bool {
        self.memory || self.disk
    }

    /// Test hook: pin the free-space probe result.
    pub fn inject_free_bytes(&mut self, free: Option<u64>) {
        self.injected_free_bytes = free;
    }

    /// Test hook: pretend the volume has this many bytes (ratio floor).
    pub fn inject_volume_bytes(&mut self, total: u64) {
        self.volume_bytes = total;
    }

    /// Evaluate both alarms and return the transitions.
    /// `store_bytes`/`store_budget` drive memory; `data_dir` drives disk
    /// (persistent mode only — memory mode has no disk admission).
    pub fn evaluate(
        &mut self,
        store_bytes: usize,
        store_budget: usize,
        data_dir: Option<&Path>,
    ) -> Transitions {
        let mut t = Transitions::default();
        // Memory with hysteresis.
        if !self.memory && store_bytes as u64 >= store_budget as u64 {
            self.memory = true;
            t.memory_raised = true;
        } else if self.memory
            && (store_bytes as u64) < (store_budget as f64 * MEMORY_CLEAR_FRACTION) as u64
        {
            self.memory = false;
            t.memory_cleared = true;
        }
        // Disk (persistent mode only).
        if let Some(dir) = data_dir {
            let free = self.injected_free_bytes.or_else(|| free_bytes_of(dir));
            let threshold = self.threshold();
            match free {
                Some(f) if !self.disk && f < threshold => {
                    self.disk = true;
                    t.disk_raised = true;
                }
                Some(f) if self.disk && f >= threshold => {
                    self.disk = false;
                    t.disk_cleared = true;
                }
                _ => {}
            }
        } else if self.disk {
            self.disk = false;
            t.disk_cleared = true;
        }
        t
    }

    fn threshold(&self) -> u64 {
        let ratio_floor = (self.volume_bytes as f64 * self.disk_free_min_ratio) as u64;
        self.disk_free_min_bytes.max(ratio_floor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("rmq-alarms-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&d);
        d
    }

    #[test]
    fn memory_hysteresis() {
        let mut a = Alarms::default();
        let t = a.evaluate(1000, 1000, None);
        assert!(t.memory_raised && a.memory());
        // Still above clear-watermark: no transition.
        let t = a.evaluate(900, 1000, None);
        assert!(!t.any());
        // Below 50%: clears.
        let t = a.evaluate(400, 1000, None);
        assert!(t.memory_cleared && !a.memory());
    }

    #[test]
    fn disk_alarm_with_injected_free_bytes() {
        let dir = tmp();
        let mut a = Alarms::default();
        a.inject_volume_bytes(10_000_000_000);
        a.inject_free_bytes(Some(5_000_000_000));
        let t = a.evaluate(0, 1000, Some(&dir));
        assert!(!t.any(), "5GB free > 1GB floor");
        a.inject_free_bytes(Some(100 * 1024 * 1024));
        let t = a.evaluate(0, 1000, Some(&dir));
        assert!(t.disk_raised && a.disk());
        a.inject_free_bytes(Some(2_000_000_000));
        let t = a.evaluate(0, 1000, Some(&dir));
        assert!(t.disk_cleared && !a.disk());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disk_ratio_floor_wins_on_large_volumes() {
        let dir = tmp();
        let mut a = Alarms::default();
        a.inject_volume_bytes(10_000 * 1_000_000_000); // 10TB
        a.inject_free_bytes(Some(5_000_000_000)); // 5GB
        let t = a.evaluate(0, 1000, Some(&dir));
        assert!(
            t.disk_raised,
            "10% of 10TB = 1TB floor; 5GB is below it despite exceeding 1GB"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn memory_mode_never_raises_disk() {
        let mut a = Alarms::default();
        a.inject_free_bytes(Some(0));
        let t = a.evaluate(0, 1000, None);
        assert!(!t.disk_raised, "no data dir -> no disk admission");
    }
}
