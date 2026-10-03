//! Offline doctor (§13.1): read-only inspection of a stopped broker's
//! data directory. Never mutates, never repairs — findings only, so the
//! operator decides. Exit semantics live in the CLI; this module returns
//! structured findings.

use std::path::Path;

use crate::record::FormatError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Level {
    /// Healthy/expected observation.
    Ok,
    /// Advisory: broker may still run, but the operator should know.
    Warn,
    /// Corruption or inconsistency: startup would (or should) refuse.
    Error,
}

#[derive(Clone, Debug)]
pub struct Finding {
    pub level: Level,
    pub area: String,
    pub detail: String,
}

#[derive(Clone, Debug, Default)]
pub struct DoctorReport {
    pub findings: Vec<Finding>,
    /// Set when any Error-level finding exists.
    pub healthy: bool,
}

impl DoctorReport {
    fn push(&mut self, level: Level, area: &str, detail: String) {
        if level == Level::Error {
            self.healthy = false;
        }
        self.findings.push(Finding {
            level,
            area: area.to_string(),
            detail,
        });
    }
}

/// Inspect `dir` read-only.
pub fn doctor(dir: &Path) -> DoctorReport {
    let mut report = DoctorReport {
        findings: Vec::new(),
        healthy: true,
    };

    if !dir.exists() {
        report.push(
            Level::Error,
            "directory",
            format!("{} does not exist", dir.display()),
        );
        return report;
    }

    // LOCK: a live foreign writer means the directory may change under us.
    if crate::journal::writer_lock_alive(dir) {
        report.push(
            Level::Warn,
            "lock",
            "a live writer holds the directory; stop the broker for stable findings".into(),
        );
    } else if dir.join("LOCK").exists() {
        report.push(
            Level::Ok,
            "lock",
            "stale LOCK present (writer crashed or was aborted); the next open takes over".into(),
        );
    } else {
        report.push(Level::Ok, "lock", "no LOCK present".into());
    }

    // Journal: chain + committed-record dry run.
    match crate::journal::last_committed_lsn(dir) {
        Ok(0) => report.push(Level::Warn, "journal", "no committed records found".into()),
        Ok(lsn) => report.push(
            Level::Ok,
            "journal",
            format!("segment chain intact; last committed fence LSN {lsn}"),
        ),
        Err(e) => report.push(Level::Error, "journal", format!("recovery failed: {e}")),
    }

    // Segment inventory.
    match crate::journal::segment_inventory(dir) {
        Some(segs) if segs.is_empty() => {
            report.push(Level::Warn, "journal", "no segments; first run?".into())
        }
        Some(segs) => report.push(
            Level::Ok,
            "journal",
            format!(
                "{} segment(s): {:?}..{:?}",
                segs.len(),
                segs.first(),
                segs.last()
            ),
        ),
        None => report.push(
            Level::Error,
            "journal",
            "cannot read segment directory".into(),
        ),
    }

    // Manifest + snapshot consistency.
    match crate::snapshot::read_manifest(dir) {
        Ok(None) => report.push(
            Level::Ok,
            "manifest",
            "no manifest (journal-only recovery root; snapshot never taken)".into(),
        ),
        Ok(Some(m)) => {
            let snap_dir = dir.join("snapshots").join(&m.snapshot);
            if !snap_dir.exists() {
                report.push(
                    Level::Error,
                    "manifest",
                    format!("manifest references missing snapshot '{}'", m.snapshot),
                );
            } else {
                match crate::snapshot::read_snapshot(&snap_dir) {
                    Ok(s) if s.generation != m.generation => report.push(
                        Level::Error,
                        "manifest",
                        format!(
                            "generation mismatch: manifest {} vs snapshot {}",
                            m.generation, s.generation
                        ),
                    ),
                    Ok(s) => report.push(
                        Level::Ok,
                        "manifest",
                        format!(
                            "recovery root: snapshot generation {}, covered LSN {}, {} record(s)",
                            m.generation,
                            m.covered_lsn,
                            s.records.len()
                        ),
                    ),
                    Err(e) => report.push(
                        Level::Error,
                        "snapshot",
                        format!("snapshot unreadable: {e}"),
                    ),
                }
            }
        }
        Err(e) => report.push(
            Level::Error,
            "manifest",
            format!("manifest unreadable: {e}"),
        ),
    }

    // Full dry-run recovery: the strongest check (same fold the broker
    // runs at startup, executed against a copy-free read).
    match crate::rebuild::rebuild(dir, usize::MAX / 2) {
        Ok(b) => {
            let vhost = b.topology.find_vhost("/");
            let queues = vhost.map(|v| b.topology.iter_queues().filter(|(_, r)| r.vhost == v).count()).unwrap_or(0);
            let auth_rows = b.auth.list_counts_for_doctor();
            report.push(
                Level::Ok,
                "recovery",
                format!(
                    "dry-run recovered {} record(s): {} durable queue(s), {} principal(s), {} permission grant(s)",
                    b.replayed,
                    queues,
                    auth_rows.0,
                    auth_rows.1
                ),
            );
        }
        Err(FormatError::Checksum(lsn)) => report.push(
            Level::Error,
            "recovery",
            format!("checksum mismatch at LSN {lsn}; do not delete data — restore from the latest verified backup"),
        ),
        Err(e) => report.push(Level::Error, "recovery", format!("dry-run failed: {e}")),
    }

    // Projection: presence is informational; the journal rebuild above is
    // the authority (§9.7 — a stale/corrupt projection rebuilds).
    let index = dir.join("index").join("state.redb");
    if index.exists() {
        report.push(
            Level::Ok,
            "projection",
            "index present (derived; rebuilt automatically if stale or corrupt)".into(),
        );
    }

    report
}
