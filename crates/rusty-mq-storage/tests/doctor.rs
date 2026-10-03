//! Doctor: read-only findings on a stopped data directory — healthy dirs
//! report OK, corrupted ones report Error findings, and the doctor never
//! mutates the directory.

use rusty_mq_storage::doctor::{doctor, Level};
use rusty_mq_storage::journal::{JournalConfig, JournalWriter};
use rusty_mq_storage::record::{QueueRecord, Record};

fn tmp(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!(
        "rmq-doctor-{}-{tag}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn seed(dir: &std::path::Path) {
    let mut w = JournalWriter::open(dir, JournalConfig::default()).unwrap();
    w.commit(&[Record::QueueDeclare(QueueRecord {
        name: "q".into(),
        id: 1,
        durable: true,
        exclusive: false,
        auto_delete: false,
        owner: 0,
    })])
    .unwrap();
    w.release_lock();
}

#[test]
fn healthy_directory_reports_ok() {
    let dir = tmp("ok");
    seed(&dir);
    let report = doctor(&dir);
    assert!(report.healthy, "findings: {:#?}", report.findings);
    assert!(report.findings.iter().any(|f| f.level == Level::Ok
        && f.area == "recovery"
        && f.detail.contains("1 durable queue")));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn corrupted_journal_is_an_error_finding() {
    let dir = tmp("corrupt");
    seed(&dir);
    let seg = dir.join("00000000000000000001.log");
    let mut data = std::fs::read(&seg).unwrap();
    let last = data.len() - 1;
    data[last] ^= 0xFF;
    std::fs::write(&seg, data).unwrap();

    let report = doctor(&dir);
    assert!(!report.healthy);
    assert!(report
        .findings
        .iter()
        .any(|f| f.level == Level::Error && f.area == "journal"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn missing_manifest_snapshot_is_an_error_finding() {
    let dir = tmp("missing-snap");
    std::fs::create_dir_all(dir.join("snapshots")).unwrap();
    rusty_mq_storage::snapshot::publish_manifest(
        &dir,
        &rusty_mq_storage::snapshot::Manifest {
            generation: 7,
            covered_lsn: 100,
            snapshot: "snapshot-00000000000000000007".into(),
        },
    )
    .unwrap();
    let report = doctor(&dir);
    assert!(!report.healthy);
    assert!(report.findings.iter().any(|f| f.level == Level::Error
        && f.area == "manifest"
        && f.detail.contains("missing snapshot")));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn doctor_never_mutates_the_directory() {
    let dir = tmp("readonly");
    seed(&dir);
    // Fingerprint every file before and after.
    let before: Vec<(std::path::PathBuf, Vec<u8>)> = {
        let mut files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .collect();
        files.sort();
        files
            .into_iter()
            .map(|p| (p.clone(), std::fs::read(&p).unwrap()))
            .collect()
    };
    let _ = doctor(&dir);
    let after: Vec<(std::path::PathBuf, Vec<u8>)> = {
        let mut files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .collect();
        files.sort();
        files
            .into_iter()
            .map(|p| (p.clone(), std::fs::read(&p).unwrap()))
            .collect()
    };
    assert_eq!(before, after, "doctor must be strictly read-only");
    let _ = std::fs::remove_dir_all(&dir);
}
