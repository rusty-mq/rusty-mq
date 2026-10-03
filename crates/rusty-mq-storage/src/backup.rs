//! Offline backup, verification, and restore (§9.10, T23).
//!
//! A backup is a complete copy of the recovery chain — MANIFEST (if
//! published), `snapshots/`, and `journal/` — taken while no writer holds
//! the data directory (enforced via the LOCK liveness probe). Every
//! backup carries a `CHECKSUMS` sidecar (SHA-256 per file) so transport
//! corruption is caught byte-level before the semantic check. Restore
//! targets must be empty; a nonempty target is refused rather than merged
//! or overwritten (INV: never silently initialize over existing state).
//!
//! Verification checks the sidecar, then replays the backup through the
//! real recovery fold: a backup that cannot be recovered is not a valid
//! backup.

use std::fs;
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::record::FormatError;

/// Name of the integrity sidecar written into every backup.
pub const CHECKSUMS_FILE: &str = "CHECKSUMS";

fn io_err(e: std::io::Error) -> FormatError {
    FormatError::Io(e.to_string())
}

/// Copy `from` into a fresh `to` (created), recursively, excluding the
/// LOCK file (session state, not part of the recovery chain).
fn copy_dir_excluding_lock(from: &Path, to: &Path) -> Result<(), FormatError> {
    fs::create_dir_all(to).map_err(io_err)?;
    for entry in fs::read_dir(from).map_err(io_err)? {
        let entry = entry.map_err(io_err)?;
        if entry.file_name() == "LOCK" || entry.file_name() == "MANIFEST.tmp" {
            continue;
        }
        let target = to.join(entry.file_name());
        if entry.file_type().map_err(io_err)?.is_dir() {
            copy_dir_excluding_lock(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), &target).map_err(io_err)?;
        }
    }
    Ok(())
}

/// Deterministic (sorted) relative paths of every file under `root`,
/// skipping `skip` names wherever they appear.
fn inventory(root: &Path, skip: &[&str]) -> Result<Vec<std::path::PathBuf>, FormatError> {
    fn walk(
        root: &Path,
        dir: &Path,
        skip: &[&str],
        out: &mut Vec<std::path::PathBuf>,
    ) -> Result<(), FormatError> {
        for entry in fs::read_dir(dir).map_err(io_err)? {
            let entry = entry.map_err(io_err)?;
            if skip.contains(&entry.file_name().to_string_lossy().as_ref()) {
                continue;
            }
            let path = entry.path();
            if entry.file_type().map_err(io_err)?.is_dir() {
                walk(root, &path, skip, out)?;
            } else {
                let rel = path
                    .strip_prefix(root)
                    .map_err(|_| FormatError::Io("inventory escape: path outside root".into()))?;
                out.push(rel.to_path_buf());
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    walk(root, root, skip, &mut out)?;
    out.sort();
    Ok(out)
}

/// Create a backup of `data_dir` under `output` (created fresh; must not
/// exist or be empty). Refuses while a live writer holds the directory.
pub fn create(data_dir: &Path, output: &Path) -> Result<(), FormatError> {
    if crate::journal::writer_lock_alive(data_dir) {
        return Err(FormatError::Io(
            "data directory is held by a live writer; stop the broker first (§9.10 offline backup)"
                .into(),
        ));
    }
    if !data_dir.exists() {
        return Err(FormatError::Io(format!(
            "data directory {} does not exist",
            data_dir.display()
        )));
    }
    if output.exists() {
        return Err(FormatError::Io(format!(
            "backup output {} already exists; refusing to overwrite",
            output.display()
        )));
    }
    copy_dir_excluding_lock(data_dir, output)?;
    write_checksums(output)
}

fn file_sha256(path: &Path) -> Result<String, FormatError> {
    let bytes = fs::read(path).map_err(io_err)?;
    Ok(format!("{:x}", Sha256::digest(&bytes)))
}

/// Write the CHECKSUMS sidecar: one `<hex>  <relative/path>` line per
/// copied file, sorted, covering everything in the backup.
fn write_checksums(backup: &Path) -> Result<(), FormatError> {
    let files = inventory(backup, &[CHECKSUMS_FILE])?;
    let mut body = String::new();
    for rel in files {
        let hex = file_sha256(&backup.join(&rel))?;
        body.push_str(&format!("{hex}  {}\n", rel.to_string_lossy()));
    }
    fs::write(backup.join(CHECKSUMS_FILE), body).map_err(io_err)
}

/// Byte-level integrity against the CHECKSUMS sidecar: every listed file
/// must hash to its recorded digest, and the file set must match exactly
/// (files added or removed after creation fail). This catches transport
/// corruption before the (slower) semantic replay below.
fn verify_checksums(backup: &Path) -> Result<(), FormatError> {
    let body = fs::read_to_string(backup.join(CHECKSUMS_FILE)).map_err(|_| {
        FormatError::Io(format!(
            "backup is missing {CHECKSUMS_FILE}; refusing to verify (recreate the backup)"
        ))
    })?;
    let mut listed: Vec<String> = Vec::new();
    for line in body.lines() {
        let (hex, rel) = line
            .split_once("  ")
            .ok_or_else(|| FormatError::Io(format!("malformed {CHECKSUMS_FILE} line: {line:?}")))?;
        let actual = file_sha256(&backup.join(rel))
            .map_err(|_| FormatError::Io(format!("{CHECKSUMS_FILE} lists missing file {rel}")))?;
        if actual != hex {
            return Err(FormatError::Io(format!(
                "checksum mismatch for {rel}: recorded {hex}, found {actual}"
            )));
        }
        listed.push(rel.to_string());
    }
    let present: Vec<String> = inventory(backup, &[CHECKSUMS_FILE])?
        .iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect();
    if listed != present {
        return Err(FormatError::Io(format!(
            "backup file set does not match {CHECKSUMS_FILE}: listed {listed:?}, present {present:?}"
        )));
    }
    Ok(())
}

/// Verify a backup: byte-level integrity against the CHECKSUMS sidecar,
/// then a full semantic replay through the real recovery fold. Returns
/// the rebuilt summary on success (records replayed + queue count as a
/// sanity signal).
pub fn verify(backup: &Path) -> Result<VerifySummary, FormatError> {
    verify_checksums(backup)?;
    // The data-directory layout is flat: numbered `*.log` segments at the
    // root, `snapshots/`, `MANIFEST`. A backup must be that recovery root.
    let has_segments = fs::read_dir(backup)
        .map_err(io_err)?
        .filter_map(|e| e.ok())
        .any(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.ends_with(".log") && n.len() == 24)
        });
    if !has_segments && !backup.join("MANIFEST").exists() {
        return Err(FormatError::Io(
            "not a rusty-mq backup: no journal segments and no MANIFEST".into(),
        ));
    }
    let rebuilt = crate::rebuild::rebuild(backup, usize::MAX / 2)?;
    let vhost = rebuilt
        .topology
        .find_vhost("/")
        .expect("recovery always constructs the default vhost");
    let queues = rebuilt.topology.iter_queues().count();
    let _ = vhost;
    Ok(VerifySummary {
        replayed: rebuilt.replayed,
        queues,
    })
}

/// Restore a verified backup into `target`: the target must not exist or
/// be empty — restore never merges into or overwrites existing state, and
/// never targets a directory a live writer holds.
pub fn restore(backup: &Path, target: &Path) -> Result<(), FormatError> {
    // Verify FIRST: never restore garbage (T23).
    verify(backup)?;
    if target.exists() {
        let nonempty = fs::read_dir(target).map_err(io_err)?.next().is_some();
        if nonempty {
            return Err(FormatError::Io(format!(
                "restore target {} is not empty; refusing to overwrite (restore into an empty directory)",
                target.display()
            )));
        }
    }
    if crate::journal::writer_lock_alive(target) {
        return Err(FormatError::Io(
            "restore target is held by a live writer".into(),
        ));
    }
    // CHECKSUMS is backup metadata, never part of a data directory.
    copy_dir_excluding_lock(backup, target)?;
    if target.join(CHECKSUMS_FILE).exists() {
        fs::remove_file(target.join(CHECKSUMS_FILE)).map_err(io_err)?;
    }
    Ok(())
}

/// Result of a successful verification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VerifySummary {
    pub replayed: usize,
    pub queues: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{JournalConfig, JournalWriter};
    use crate::record::{Enqueue, QueueRecord, Record};

    fn tmp(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("rmq-backup-{}-{tag}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    fn seed(dir: &Path) {
        let w = JournalWriter::open(dir, JournalConfig::default()).unwrap();
        w.commit(&[Record::QueueDeclare(QueueRecord {
            name: "jobs".into(),
            id: 5,
            durable: true,
            exclusive: false,
            auto_delete: false,
            owner: 0,
        })])
        .unwrap();
        w.commit(&[Record::Enqueue(Enqueue {
            message_id: 0,
            property_bytes: vec![],
            body: b"payload-one".to_vec(),
            exchange: "".into(),
            routing_key: "jobs".into(),
            persistent: true,
            destinations: vec![(5, 0)],
        })])
        .unwrap();
        // Release the lock and close (drop would too, but be explicit for
        // the offline contract).
        w.release_lock();
        drop(w);
    }

    #[test]
    fn backup_verify_restore_roundtrip() {
        let dir = tmp("src");
        seed(&dir);
        let backup = tmp("backup");
        create(&dir, &backup).unwrap();
        let summary = verify(&backup).unwrap();
        assert_eq!(summary.queues, 1);
        assert!(summary.replayed >= 2);

        // Restore into a fresh directory and recover the identical state.
        let restored = tmp("restored");
        restore(&backup, &restored).unwrap();
        let a = crate::rebuild::rebuild(&dir, usize::MAX / 2).unwrap();
        let mut b = crate::rebuild::rebuild(&restored, usize::MAX / 2).unwrap();
        assert_eq!(a.replayed, b.replayed);
        let vhost = b.topology.find_vhost("/").unwrap();
        let qid = b
            .topology
            .find_queue(vhost, "jobs")
            .expect("queue restored");
        let entry = b.store.pop_ready(qid).expect("message restored").message;
        assert_eq!(entry.body, b"payload-one".to_vec());

        // The restored directory is immediately usable by a writer.
        let w = JournalWriter::open(&restored, JournalConfig::default()).unwrap();
        w.commit(&[Record::QueueDelete { id: 5 }]).unwrap();
        drop(w);
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&backup);
        let _ = fs::remove_dir_all(&restored);
    }

    #[test]
    fn live_writer_refuses_backup() {
        let dir = tmp("live");
        let _w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
        let backup = tmp("live-backup");
        let err = create(&dir, &backup).unwrap_err();
        assert!(err.to_string().contains("live writer"));
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&backup);
    }

    #[test]
    fn stale_lock_is_tolerated() {
        let dir = tmp("stale");
        seed(&dir);
        // Simulate a crashed writer: stale LOCK pointing at a dead pid.
        fs::write(dir.join("LOCK"), "999999999").unwrap();
        let backup = tmp("stale-backup");
        create(&dir, &backup).unwrap(); // tolerated
        assert!(!backup.join("LOCK").exists(), "LOCK never enters backups");
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&backup);
    }

    #[test]
    fn nonempty_restore_target_refused() {
        let dir = tmp("src2");
        seed(&dir);
        let backup = tmp("backup2");
        create(&dir, &backup).unwrap();
        let target = tmp("target2");
        fs::create_dir_all(&target).unwrap();
        fs::write(target.join("existing.txt"), b"x").unwrap();
        let err = restore(&backup, &target).unwrap_err();
        assert!(err.to_string().contains("not empty"));
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&backup);
        let _ = fs::remove_dir_all(&target);
    }

    #[test]
    fn checksums_sidecar_written_and_listed() {
        let dir = tmp("cs-src");
        seed(&dir);
        let backup = tmp("cs-backup");
        create(&dir, &backup).unwrap();
        let body = fs::read_to_string(backup.join(CHECKSUMS_FILE)).unwrap();
        assert!(
            body.lines()
                .any(|l| l.ends_with("00000000000000000001.log")),
            "segment must be listed: {body}"
        );
        // Each line is 64 hex chars + two spaces + a relative path.
        for line in body.lines() {
            let (hex, rel) = line.split_once("  ").unwrap();
            assert_eq!(hex.len(), 64, "sha256 hex for {rel}");
        }
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&backup);
    }

    #[test]
    fn byte_flip_caught_by_checksums() {
        let dir = tmp("cs-src2");
        seed(&dir);
        let backup = tmp("cs-backup2");
        create(&dir, &backup).unwrap();
        // Corrupt a byte in a segment header (before any record payload so
        // the journal CRC may or may not fire first — the sidecar must).
        let seg = backup.join("00000000000000000001.log");
        let mut data = fs::read(&seg).unwrap();
        data[0] ^= 0x01;
        fs::write(&seg, data).unwrap();
        let err = verify(&backup).unwrap_err().to_string();
        assert!(err.contains("checksum mismatch"), "got: {err}");
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&backup);
    }

    #[test]
    fn file_set_changes_fail_verification() {
        let dir = tmp("cs-src3");
        seed(&dir);
        let backup = tmp("cs-backup3");
        create(&dir, &backup).unwrap();

        // File removed after creation.
        let seg = backup.join("00000000000000000001.log");
        fs::remove_file(&seg).unwrap();
        let err = verify(&backup).unwrap_err().to_string();
        assert!(err.contains("missing file"), "got: {err}");

        // File added after creation (recreate a fresh backup for it).
        let _ = fs::remove_dir_all(&backup);
        let backup = tmp("cs-backup3b");
        create(&dir, &backup).unwrap();
        fs::write(backup.join("sneaky.log"), b"x").unwrap();
        let err = verify(&backup).unwrap_err().to_string();
        assert!(err.contains("does not match"), "got: {err}");
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&backup);
    }

    #[test]
    fn missing_sidecar_refuses_and_restore_excludes_it() {
        let dir = tmp("cs-src4");
        seed(&dir);
        let backup = tmp("cs-backup4");
        create(&dir, &backup).unwrap();
        fs::remove_file(backup.join(CHECKSUMS_FILE)).unwrap();
        let err = verify(&backup).unwrap_err().to_string();
        assert!(err.contains("missing CHECKSUMS"), "got: {err}");

        // A valid backup restores cleanly and CHECKSUMS never lands in the
        // data directory.
        let backup2 = tmp("cs-backup4b");
        create(&dir, &backup2).unwrap();
        let target = tmp("cs-target4");
        restore(&backup2, &target).unwrap();
        assert!(
            !target.join(CHECKSUMS_FILE).exists(),
            "CHECKSUMS must not enter a data directory"
        );
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&backup);
        let _ = fs::remove_dir_all(&backup2);
        let _ = fs::remove_dir_all(&target);
    }

    #[test]
    fn tampered_backup_fails_verification() {
        let dir = tmp("src3");
        seed(&dir);
        let backup = tmp("backup3");
        create(&dir, &backup).unwrap();
        // Flip a byte inside the first journal segment payload (flat
        // layout: segments live at the recovery-root top level).
        let seg = backup.join("00000000000000000001.log");
        let mut data = fs::read(&seg).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        fs::write(&seg, data).unwrap();
        assert!(verify(&backup).is_err(), "corrupted backups do not verify");
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&backup);
    }
}
