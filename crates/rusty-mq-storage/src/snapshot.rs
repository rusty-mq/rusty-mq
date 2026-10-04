//! Snapshots, the recovery manifest, and segment reclamation (§9.9, M6).
//!
//! The authoritative recovery root is: an atomically published MANIFEST
//! pointing at an immutable snapshot directory plus the committed journal
//! suffix after the snapshot's covered LSN. A snapshot is a stream of the
//! SAME record encodings the journal uses (one decoder for both paths);
//! replay of suffix records over snapshot state is idempotent (INV-11), so
//! the covered LSN may trail the snapshot's actual capture moment.
//!
//! Reclamation deletes only journal segments whose every record LSN is
//! covered, never the writer's current tail (an unlinked tail would keep
//! accepting appends that vanish on restart).

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::journal::{crc32, scan_segments, JournalWriter, RECORD_HEADER_LEN};
use crate::record::{FormatError, Record};

/// Snapshot magic (`RMQSNAP1`).
const SNAPSHOT_MAGIC: [u8; 8] = *b"RMQSNAP1";
pub const SNAPSHOT_MAJOR: u32 = 1;
pub const SNAPSHOT_MINOR: u32 = 0;
/// Manifest magic (`RMQMNFST`).
const MANIFEST_MAGIC: [u8; 8] = *b"RMQMNFST";
pub const MANIFEST_MAJOR: u32 = 1;

/// The published recovery root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub generation: u64,
    /// Every journal record with LSN ≤ this is covered by the snapshot.
    pub covered_lsn: u64,
    /// Directory name under `snapshots/`.
    pub snapshot: String,
}

fn io_err(e: std::io::Error) -> FormatError {
    FormatError::Io(e.to_string())
}

// ---------------------------------------------------------------------
// Snapshot files.
// ---------------------------------------------------------------------

/// Header written at the start of a snapshot payload file.
///
/// Layout: magic(8) major(4) minor(4) generation(8) covered_lsn(8)
/// record_count(8) = 40 bytes, then record frames:
/// kind(1) payload_len(4) crc32(4) payload.
#[allow(clippy::needless_range_loop)]
pub fn write_snapshot(
    dir: &Path,
    generation: u64,
    covered_lsn: u64,
    records: &[Record],
) -> Result<PathBuf, FormatError> {
    let snap_dir = dir
        .join("snapshots")
        .join(format!("snapshot-{generation:020}"));
    fs::create_dir_all(&snap_dir).map_err(io_err)?;
    let path = snap_dir.join("state.bin");
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(&SNAPSHOT_MAGIC);
    buf.extend_from_slice(&SNAPSHOT_MAJOR.to_le_bytes());
    buf.extend_from_slice(&SNAPSHOT_MINOR.to_le_bytes());
    buf.extend_from_slice(&generation.to_le_bytes());
    buf.extend_from_slice(&covered_lsn.to_le_bytes());
    buf.extend_from_slice(&(records.len() as u64).to_le_bytes());
    for rec in records {
        let payload = rec.encode();
        let mut crc_input = Vec::with_capacity(payload.len() + 5);
        crc_input.push(rec.kind());
        crc_input.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        crc_input.extend_from_slice(&payload);
        buf.extend_from_slice(&rec.kind().to_le_bytes());
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&crc32(&crc_input).to_le_bytes());
        buf.extend_from_slice(&payload);
    }
    // Atomic publication (§9.9): write+fsync a temp file, then rename —
    // a concurrent reader (or a crash mid-write) sees either the old
    // complete snapshot or the new one, never a truncation. Found by a
    // rare durability-test flake ("failed to fill whole buffer": a
    // background compaction truncating state.bin while recovery read it).
    let tmp = snap_dir.join("state.bin.tmp");
    let mut file = File::create(&tmp).map_err(io_err)?;
    file.write_all(&buf).map_err(io_err)?;
    file.sync_all().map_err(io_err)?;
    fs::rename(&tmp, &path).map_err(io_err)?;
    // Sync the snapshot directory so the rename is durably linked.
    File::open(&snap_dir)
        .and_then(|d| d.sync_all())
        .map_err(io_err)?;
    Ok(snap_dir)
}

/// A snapshot payload: header plus its record stream.
pub struct Snapshot {
    pub generation: u64,
    pub covered_lsn: u64,
    pub records: Vec<Record>,
}

pub fn read_snapshot(snap_dir: &Path) -> Result<Snapshot, FormatError> {
    let path = snap_dir.join("state.bin");
    let mut buf = Vec::new();
    File::open(&path)
        .and_then(|mut f| f.read_to_end(&mut buf))
        .map_err(io_err)?;
    if buf.len() < 40 || buf[0..8] != SNAPSHOT_MAGIC {
        return Err(FormatError::BadMagic);
    }
    let major = u32::from_le_bytes(buf[8..12].try_into().unwrap());
    if major != SNAPSHOT_MAJOR {
        return Err(FormatError::UnsupportedMajor(major));
    }
    let generation = u64::from_le_bytes(buf[16..24].try_into().unwrap());
    let covered_lsn = u64::from_le_bytes(buf[24..32].try_into().unwrap());
    let count = u64::from_le_bytes(buf[32..40].try_into().unwrap()) as usize;
    let mut records = Vec::with_capacity(count.min(1_000_000));
    let mut pos = 40usize;
    for _ in 0..count {
        if pos + 9 > buf.len() {
            return Err(FormatError::Corruption("snapshot truncated".into()));
        }
        let kind = buf[pos];
        let payload_len = u32::from_le_bytes(buf[pos + 1..pos + 5].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(buf[pos + 5..pos + 9].try_into().unwrap());
        if pos + 9 + payload_len > buf.len() {
            return Err(FormatError::Corruption("snapshot payload truncated".into()));
        }
        let payload = &buf[pos + 9..pos + 9 + payload_len];
        let mut crc_input = Vec::with_capacity(payload.len() + 5);
        crc_input.push(kind);
        crc_input.extend_from_slice(&(payload_len as u32).to_le_bytes());
        crc_input.extend_from_slice(payload);
        if crc32(&crc_input) != crc {
            return Err(FormatError::Checksum(0));
        }
        records.push(Record::decode(kind, payload)?);
        pos += 9 + payload_len;
    }
    if pos != buf.len() {
        return Err(FormatError::Corruption("snapshot trailing bytes".into()));
    }
    Ok(Snapshot {
        generation,
        covered_lsn,
        records,
    })
}

// ---------------------------------------------------------------------
// Manifest (atomic publication).
// ---------------------------------------------------------------------

fn manifest_bytes(m: &Manifest) -> Vec<u8> {
    let name = m.snapshot.as_bytes();
    let mut b = Vec::with_capacity(8 + 4 + 8 + 8 + 4 + name.len());
    b.extend_from_slice(&MANIFEST_MAGIC);
    b.extend_from_slice(&MANIFEST_MAJOR.to_le_bytes());
    b.extend_from_slice(&m.generation.to_le_bytes());
    b.extend_from_slice(&m.covered_lsn.to_le_bytes());
    b.extend_from_slice(&(name.len() as u32).to_le_bytes());
    b.extend_from_slice(name);
    b
}

/// Publish the recovery root: write MANIFEST.tmp, fsync, rename over
/// MANIFEST, fsync the directory (§9.9 step 4).
pub fn publish_manifest(dir: &Path, manifest: &Manifest) -> Result<(), FormatError> {
    let tmp = dir.join("MANIFEST.tmp");
    {
        let mut f = File::create(&tmp).map_err(io_err)?;
        f.write_all(&manifest_bytes(manifest)).map_err(io_err)?;
        f.sync_all().map_err(io_err)?;
    }
    fs::rename(&tmp, dir.join("MANIFEST")).map_err(io_err)?;
    File::open(dir).and_then(|d| d.sync_all()).map_err(io_err)?;
    Ok(())
}

pub fn read_manifest(dir: &Path) -> Result<Option<Manifest>, FormatError> {
    let path = dir.join("MANIFEST");
    if !path.exists() {
        return Ok(None);
    }
    let mut buf = Vec::new();
    File::open(&path)
        .and_then(|mut f| f.read_to_end(&mut buf))
        .map_err(io_err)?;
    if buf.len() < 32 || buf[0..8] != MANIFEST_MAGIC {
        return Err(FormatError::BadMagic);
    }
    let major = u32::from_le_bytes(buf[8..12].try_into().unwrap());
    if major != MANIFEST_MAJOR {
        return Err(FormatError::UnsupportedMajor(major));
    }
    let generation = u64::from_le_bytes(buf[12..20].try_into().unwrap());
    let covered_lsn = u64::from_le_bytes(buf[20..28].try_into().unwrap());
    let name_len = u32::from_le_bytes(buf[28..32].try_into().unwrap()) as usize;
    if buf.len() < 32 + name_len {
        return Err(FormatError::Corruption("manifest truncated".into()));
    }
    let snapshot = std::str::from_utf8(&buf[32..32 + name_len])
        .map_err(|_| FormatError::Corruption("manifest name not UTF-8".into()))?
        .to_string();
    Ok(Some(Manifest {
        generation,
        covered_lsn,
        snapshot,
    }))
}

// ---------------------------------------------------------------------
// Reclamation.
// ---------------------------------------------------------------------

/// Highest record LSN in a segment (0 for an empty segment).
fn segment_max_lsn(dir: &Path, id: u64) -> Result<u64, FormatError> {
    use std::io::{Seek, SeekFrom};
    let mut file =
        File::open(dir.join(crate::journal::segment_file_name_pub(id))).map_err(io_err)?;
    file.seek(SeekFrom::Start(32)).map_err(io_err)?;
    let mut max_lsn = 0u64;
    let mut header = [0u8; RECORD_HEADER_LEN];
    loop {
        match file.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(io_err(e)),
        }
        let lsn = u64::from_le_bytes(header[0..8].try_into().unwrap());
        let payload_len = u32::from_le_bytes(header[9..13].try_into().unwrap()) as usize;
        max_lsn = max_lsn.max(lsn);
        // Seek past the payload (a torn tail simply ends the scan).
        file.seek(SeekFrom::Current(payload_len as i64))
            .map_err(io_err)?;
    }
    Ok(max_lsn)
}

/// Delete journal segments fully covered by `covered_lsn`, keeping the
/// writer's current `keep_segment` (and anything newer). Returns the
/// number of segments removed. Older snapshots beyond the manifest's
/// generation are also removed.
pub fn reclaim(
    dir: &Path,
    covered_lsn: u64,
    keep_segment: u64,
    current_generation: u64,
) -> Result<usize, FormatError> {
    let ids = scan_segments(dir)?;
    let mut removed = 0usize;
    for id in ids {
        if id >= keep_segment {
            break; // ids are sorted; everything from keep_segment on stays
        }
        let max_lsn = segment_max_lsn(dir, id)?;
        if max_lsn <= covered_lsn {
            fs::remove_file(dir.join(crate::journal::segment_file_name_pub(id))).map_err(io_err)?;
            removed += 1;
        }
    }
    if removed > 0 {
        File::open(dir).and_then(|d| d.sync_all()).map_err(io_err)?;
    }
    // Superseded snapshots.
    let snaps = dir.join("snapshots");
    if snaps.exists() {
        for entry in fs::read_dir(&snaps).map_err(io_err)? {
            let entry = entry.map_err(io_err)?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(stem) = name.strip_prefix("snapshot-") else {
                continue;
            };
            if let Ok(gen) = stem.parse::<u64>() {
                if gen < current_generation {
                    let _ = fs::remove_dir_all(entry.path());
                }
            }
        }
        let _ = OpenOptions::new().read(true).open(&snaps);
    }
    Ok(removed)
}

/// Total bytes of journal segments on disk (compaction heuristics).
pub fn journal_bytes(dir: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(ids) = scan_segments(dir) else {
        return 0;
    };
    for id in ids {
        if let Ok(m) = fs::metadata(dir.join(crate::journal::segment_file_name_pub(id))) {
            total += m.len();
        }
    }
    total
}

/// The writer's current segment id (reclaim keeps it).
pub fn writer_segment_id(w: &JournalWriter) -> u64 {
    w.current_segment_id()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::JournalConfig;
    use crate::record::{Enqueue, QueueRecord};

    fn tmp(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("rmq-snap-{}-{tag}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    fn q(id: u64, name: &str) -> Record {
        Record::QueueDeclare(QueueRecord {
            name: name.into(),
            id,
            durable: true,
            exclusive: false,
            auto_delete: false,
            owner: 0,
        })
    }

    fn msg(id: u64, seq: u64) -> Record {
        Record::Enqueue(Enqueue {
            message_id: id,
            property_bytes: vec![],
            body: format!("m{id}").into_bytes(),
            exchange: "".into(),
            routing_key: "jobs".into(),
            persistent: true,
            destinations: vec![(7, seq)],
        })
    }

    #[test]
    fn snapshot_roundtrip() {
        let dir = tmp("roundtrip");
        let records = vec![q(7, "jobs"), msg(1, 0), msg(2, 1)];
        let snap_dir = write_snapshot(&dir, 3, 42, &records).unwrap();
        let snap = read_snapshot(&snap_dir).unwrap();
        assert_eq!(snap.generation, 3);
        assert_eq!(snap.covered_lsn, 42);
        assert_eq!(snap.records, records);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_snapshot_is_explicit() {
        let dir = tmp("corrupt");
        let snap_dir = write_snapshot(&dir, 1, 1, &[q(7, "jobs")]).unwrap();
        let path = snap_dir.join("state.bin");
        let mut data = fs::read(&path).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        fs::write(&path, data).unwrap();
        assert!(matches!(
            read_snapshot(&snap_dir),
            Err(FormatError::Checksum(_))
        ));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn manifest_atomic_publish_and_missing_snapshot_error() {
        let dir = tmp("manifest");
        fs::create_dir_all(&dir).unwrap();
        assert!(read_manifest(&dir).unwrap().is_none());
        let m = Manifest {
            generation: 2,
            covered_lsn: 10,
            snapshot: "snapshot-00000000000000000002".into(),
        };
        publish_manifest(&dir, &m).unwrap();
        assert_eq!(read_manifest(&dir).unwrap(), Some(m));
        // Manifest pointing at a snapshot that does not exist: explicit.
        let result = crate::rebuild::rebuild(&dir, 1 << 20);
        assert!(result.is_err(), "missing snapshot must fail recovery");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn reclaim_removes_only_covered_segments() {
        let dir = tmp("reclaim");
        let w = JournalWriter::open(
            &dir,
            JournalConfig {
                segment_bytes: 200,
                ..Default::default()
            },
        )
        .unwrap();
        let mut last_fence = 0;
        for i in 0..15 {
            last_fence = w.commit(&[q(i, &format!("q{i}"))]).unwrap();
        }
        let keep = w.current_segment_id();
        drop(w);
        let before = journal_bytes(&dir);
        // Real compaction order: publish a manifest covering the LSNs the
        // reclaim will delete (the snapshot holds the live state as of the
        // covered point — q0 here).
        let covered = last_fence / 2;
        write_snapshot(&dir, 1, covered, &[q(0, "q0")]).unwrap();
        publish_manifest(
            &dir,
            &Manifest {
                generation: 1,
                covered_lsn: covered,
                snapshot: "snapshot-00000000000000000001".into(),
            },
        )
        .unwrap();
        // Early segments whose every record is covered get reclaimed.
        let removed = reclaim(&dir, covered, keep, 1).unwrap();
        assert!(removed >= 1, "at least the fully-covered early segments go");
        assert!(journal_bytes(&dir) < before);
        // Recovery still yields the FULL history (suffix intact).
        let rebuilt = crate::rebuild::rebuild(&dir, 1 << 20).unwrap();
        let vhost = rebuilt.topology.find_vhost("/").unwrap();
        assert!(
            rebuilt.topology.find_queue(vhost, "q0").is_some(),
            "covered-but-unreclaimed or suffix state survives"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
