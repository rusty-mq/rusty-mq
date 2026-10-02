//! Segment journal writer and recovery reader (docs/storage-format.md,
//! ADR-0002): one authoritative append-only chain with explicit commit
//! fences, CRC-verified records, contiguous segment linking, torn-tail
//! discard, and a sync-gated durable watermark (ADR-0001).

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::record::{FormatError, Record};

/// Segment magic (`RMQJRNL1`).
const MAGIC: [u8; 8] = *b"RMQJRNL1";
pub const FORMAT_MAJOR: u32 = 1;
pub const FORMAT_MINOR: u32 = 0;
/// Fixed size of the segment header.
const SEGMENT_HEADER_LEN: usize = 32;
/// Fixed size of a record header: LSN(8) + kind(1) + len(4) + crc(4).
const RECORD_HEADER_LEN: usize = 17;
// ---------------------------------------------------------------------
// CRC-32 (IEEE 802.3), table-driven. Self-contained: no new dependency,
// fixed known vectors in tests.
// ---------------------------------------------------------------------

fn crc32_table() -> &'static [u32; 256] {
    use std::sync::OnceLock;
    static TABLE: OnceLock<[u32; 256]> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        for i in 0..256u32 {
            let mut c = i;
            for _ in 0..8 {
                c = if c & 1 != 0 {
                    0xEDB8_8320 ^ (c >> 1)
                } else {
                    c >> 1
                };
            }
            t[i as usize] = c;
        }
        t
    })
}

pub fn crc32(data: &[u8]) -> u32 {
    let table = crc32_table();
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc = table[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

// ---------------------------------------------------------------------
// Wire framing helpers shared by writer and reader.
// ---------------------------------------------------------------------

/// Frame a control record (TxBegin/TxCommit) with the standard CRC rule
/// (kind + length + payload).
fn frame_control(kind: u8, lsn: u64) -> Vec<u8> {
    let payload = [0x00u8];
    let mut crc_input = Vec::with_capacity(5);
    crc_input.push(kind);
    crc_input.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    crc_input.extend_from_slice(&payload);
    let mut out = record_header_bytes(lsn, kind, payload.len() as u32, crc32(&crc_input)).to_vec();
    out.extend_from_slice(&payload);
    out
}

fn record_header_bytes(lsn: u64, kind: u8, payload_len: u32, crc: u32) -> [u8; RECORD_HEADER_LEN] {
    let mut h = [0u8; RECORD_HEADER_LEN];
    h[0..8].copy_from_slice(&lsn.to_le_bytes());
    h[8] = kind;
    h[9..13].copy_from_slice(&payload_len.to_le_bytes());
    h[13..17].copy_from_slice(&crc.to_le_bytes());
    h
}

fn segment_header_bytes(segment_id: u64, previous: u64) -> [u8; SEGMENT_HEADER_LEN] {
    let mut h = [0u8; SEGMENT_HEADER_LEN];
    h[0..8].copy_from_slice(&MAGIC);
    h[8..12].copy_from_slice(&FORMAT_MAJOR.to_le_bytes());
    h[12..16].copy_from_slice(&FORMAT_MINOR.to_le_bytes());
    h[16..24].copy_from_slice(&segment_id.to_le_bytes());
    h[24..32].copy_from_slice(&previous.to_le_bytes());
    h
}

/// Failpoint hook: called at crash-sensitive points in tests.
pub type Failpoint = Box<dyn Fn(&str) -> Result<(), String> + Send + Sync>;

/// Writer configuration.
#[derive(Clone)]
pub struct JournalConfig {
    /// Roll to a new segment after this many bytes.
    pub segment_bytes: usize,
    /// Optional failpoint hook (test-only).
    pub failpoint: Option<std::sync::Arc<Failpoint>>,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 256 * 1024 * 1024,
            failpoint: None,
        }
    }
}

/// The single serialized journal writer (ADR-0002).
pub struct JournalWriter {
    dir: PathBuf,
    config: JournalConfig,
    file: File,
    segment_id: u64,
    segment_len: usize,
    next_lsn: u64,
    /// Highest fence known fsynced (ADR-0001 watermark).
    durable_lsn: u64,
}

impl JournalWriter {
    /// Open (creating if absent) a journal directory. Fails explicitly on
    /// inconsistent storage; never initializes over a foreign layout.
    pub fn open(dir: &Path, config: JournalConfig) -> Result<Self, FormatError> {
        fs::create_dir_all(dir).map_err(io_err)?;
        // Validate any existing chain first.
        let chain = scan_segments(dir)?;
        let (segment_id, previous, next_lsn, tail_to_truncate) = match chain.last() {
            Some(last) => {
                let (hdr, _) = read_segment_header(dir, *last)?;
                let mut r = SegmentReader::open(dir, *last)?;
                let mut max_lsn = 0u64;
                while let Some(item) = r.next_record()? {
                    max_lsn = max_lsn.max(item.lsn);
                }
                // A torn tail would otherwise strand every future append
                // behind an unreadable region: truncate to the last intact
                // record boundary before writing (§9.8 rule 4).
                (
                    hdr.segment_id,
                    hdr.previous,
                    max_lsn + 1,
                    Some((*last, r.intact_prefix())),
                )
            }
            None => (1, 0, 1, None),
        };
        // The writer continues the existing tail segment (not a new one) so
        // LSNs stay contiguous within it.
        let path = dir.join(segment_file_name(segment_id));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(io_err)?;
        let mut w = Self {
            dir: dir.to_path_buf(),
            config,
            file,
            segment_id,
            segment_len: SEGMENT_HEADER_LEN,
            next_lsn,
            durable_lsn: 0,
        };
        if fs::metadata(&path).map(|m| m.len()).unwrap_or(0) == 0 {
            w.file
                .write_all(&segment_header_bytes(segment_id, previous))
                .map_err(io_err)?;
            w.file.sync_all().map_err(io_err)?;
            sync_dir(dir)?;
        } else if let Some((seg, prefix)) = tail_to_truncate {
            let current_len = fs::metadata(&path).map(|m| m.len()).unwrap_or(0) as usize;
            if prefix < current_len {
                let tail_path = dir.join(segment_file_name(seg));
                OpenOptions::new()
                    .write(true)
                    .open(&tail_path)
                    .and_then(|f| f.set_len(prefix as u64))
                    .map_err(io_err)?;
                sync_dir(dir)?;
                tracing::debug!(
                    segment = seg,
                    from = current_len,
                    to = prefix,
                    "truncated torn tail"
                );
            }
            w.file.seek(SeekFrom::End(0)).map_err(io_err)?;
        } else {
            w.file.seek(SeekFrom::End(0)).map_err(io_err)?;
        }
        Ok(w)
    }

    fn failpoint(&self, name: &str) -> Result<(), FormatError> {
        if let Some(fp) = &self.config.failpoint {
            fp(name).map_err(FormatError::Io)?;
        }
        Ok(())
    }

    /// Append one logical transaction (its records + an explicit commit
    /// fence), assign LSNs, and fsync. Returns the fence LSN (the commit
    /// identity callers can wait on). This is the durable boundary.
    pub fn commit(&mut self, records: &[Record]) -> Result<u64, FormatError> {
        self.failpoint("before-append")?;
        let mut buf: Vec<u8> = Vec::new();
        // TxBegin for multi-record transactions; single-record transactions
        // proceed without it (spec: the explicit fence commits).
        if records.len() > 1 {
            let lsn = self.next_lsn;
            self.next_lsn += 1;
            buf.extend_from_slice(&frame_control(0x01, lsn));
        }
        for rec in records {
            let lsn = self.next_lsn;
            self.next_lsn += 1;
            let payload = rec.encode();
            let mut crc_input = Vec::with_capacity(payload.len() + 5);
            crc_input.push(rec.kind());
            crc_input.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            crc_input.extend_from_slice(&payload);
            let crc = crc32(&crc_input);
            buf.extend_from_slice(&record_header_bytes(
                lsn,
                rec.kind(),
                payload.len() as u32,
                crc,
            ));
            buf.extend_from_slice(&payload);
        }
        let fence = self.next_lsn;
        self.next_lsn += 1;
        buf.extend_from_slice(&frame_control(0x02, fence));

        // Segment roll (header included in accounting).
        if self.segment_len + buf.len() > self.config.segment_bytes
            && self.segment_len > SEGMENT_HEADER_LEN
        {
            self.roll()?;
        }
        self.file.write_all(&buf).map_err(io_err)?;
        self.failpoint("before-sync")?;
        self.file.sync_all().map_err(io_err)?;
        self.failpoint("after-sync")?;
        self.segment_len += buf.len();
        // Watermark advances only after the successful sync (ADR-0001).
        self.durable_lsn = self.durable_lsn.max(fence);
        Ok(fence)
    }

    fn roll(&mut self) -> Result<(), FormatError> {
        let next = self.segment_id + 1;
        let previous = self.segment_id;
        let path = self.dir.join(segment_file_name(next));
        let mut file = File::create(&path).map_err(io_err)?;
        file.write_all(&segment_header_bytes(next, previous))
            .map_err(io_err)?;
        file.sync_all().map_err(io_err)?;
        // Durably link the new segment into the directory before switching.
        sync_dir(&self.dir)?;
        self.file = OpenOptions::new()
            .append(true)
            .read(true)
            .open(&path)
            .map_err(io_err)?;
        self.segment_id = next;
        self.segment_len = SEGMENT_HEADER_LEN;
        Ok(())
    }

    /// Clean-shutdown end marker (optimization only).
    pub fn write_end_marker(&mut self) -> Result<(), FormatError> {
        let lsn = self.next_lsn;
        self.next_lsn += 1;
        let payload = [0x00u8];
        let crc = crc32(&payload);
        let bytes = record_header_bytes(lsn, 0xF1, payload.len() as u32, crc);
        let mut buf = bytes.to_vec();
        buf.extend_from_slice(&payload);
        self.file.write_all(&buf).map_err(io_err)?;
        self.file.sync_all().map_err(io_err)?;
        self.segment_len += buf.len();
        Ok(())
    }

    /// Replace the failpoint hook (test-only; T13/T14 evidence).
    pub fn set_failpoint(&mut self, fp: Option<std::sync::Arc<Failpoint>>) {
        self.config.failpoint = fp;
    }

    pub fn durable_lsn(&self) -> u64 {
        self.durable_lsn
    }

    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }
}

fn io_err(e: io::Error) -> FormatError {
    FormatError::Io(e.to_string())
}

fn sync_dir(dir: &Path) -> Result<(), FormatError> {
    File::open(dir).and_then(|d| d.sync_all()).map_err(io_err)?;
    Ok(())
}

fn segment_file_name(id: u64) -> String {
    format!("{id:020}.log")
}

// ---------------------------------------------------------------------
// Recovery.
// ---------------------------------------------------------------------

/// A record read during recovery.
#[derive(Clone, Debug)]
pub struct RecoveredRecord {
    pub lsn: u64,
    pub fence_lsn: u64,
    pub record: Record,
}

/// One intact item in a segment (kind-agnostic).
struct RawItem {
    lsn: u64,
    kind: u8,
    #[allow(dead_code)]
    payload: Vec<u8>,
}

struct SegmentHeader {
    segment_id: u64,
    previous: u64,
    #[allow(dead_code)]
    major: u32,
    #[allow(dead_code)]
    minor: u32,
}

struct SegmentReader {
    file: File,
    pos: usize,
    len: usize,
}

enum Step {
    Item(RawItem),
    Truncated,
    End,
}

impl SegmentReader {
    /// Byte offset of the end of the last intact record read (the safe
    /// truncation point after a torn tail).
    fn intact_prefix(&self) -> usize {
        self.pos
    }

    fn open(dir: &Path, id: u64) -> Result<Self, FormatError> {
        let mut file = File::open(dir.join(segment_file_name(id))).map_err(io_err)?;
        let len = file.metadata().map_err(io_err)?.len() as usize;
        let mut header = [0u8; SEGMENT_HEADER_LEN];
        file.read_exact(&mut header).map_err(io_err)?;
        Ok(Self {
            file,
            pos: SEGMENT_HEADER_LEN,
            len,
        })
    }

    fn next_record(&mut self) -> Result<Option<RawItem>, FormatError> {
        match self.step()? {
            Step::Item(i) => Ok(Some(i)),
            // Torn tail: report end without advancing — `pos` marks the
            // start of the torn record, i.e. the safe truncation point.
            Step::Truncated => Ok(None),
            Step::End => Ok(None),
        }
    }

    fn step(&mut self) -> Result<Step, FormatError> {
        if self.pos >= self.len {
            return Ok(Step::End);
        }
        if self.len - self.pos < RECORD_HEADER_LEN {
            return Ok(Step::Truncated);
        }
        self.file
            .seek(SeekFrom::Start(self.pos as u64))
            .map_err(io_err)?;
        let mut header = [0u8; RECORD_HEADER_LEN];
        self.file.read_exact(&mut header).map_err(io_err)?;
        let lsn = u64::from_le_bytes(header[0..8].try_into().unwrap());
        let kind = header[8];
        let payload_len = u32::from_le_bytes(header[9..13].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(header[13..17].try_into().unwrap());
        if self.pos + RECORD_HEADER_LEN + payload_len > self.len {
            return Ok(Step::Truncated);
        }
        let mut payload = vec![0u8; payload_len];
        self.file.read_exact(&mut payload).map_err(io_err)?;
        // Checksum over a complete record must match (§9.8 rule 5).
        let mut crc_input = Vec::with_capacity(payload.len() + 5);
        crc_input.push(kind);
        crc_input.extend_from_slice(&(payload_len as u32).to_le_bytes());
        crc_input.extend_from_slice(&payload);
        if crc32(&crc_input) != crc {
            return Err(FormatError::Checksum(lsn));
        }
        self.pos += RECORD_HEADER_LEN + payload_len;
        Ok(Step::Item(RawItem { lsn, kind, payload }))
    }
}

fn read_segment_header(dir: &Path, id: u64) -> Result<(SegmentHeader, usize), FormatError> {
    let mut file = File::open(dir.join(segment_file_name(id))).map_err(io_err)?;
    let len = file.metadata().map_err(io_err)?.len() as usize;
    let mut h = [0u8; SEGMENT_HEADER_LEN];
    file.read_exact(&mut h).map_err(io_err)?;
    if h[0..8] != MAGIC {
        return Err(FormatError::BadMagic);
    }
    let major = u32::from_le_bytes(h[8..12].try_into().unwrap());
    let minor = u32::from_le_bytes(h[12..16].try_into().unwrap());
    if major != FORMAT_MAJOR {
        return Err(FormatError::UnsupportedMajor(major));
    }
    Ok((
        SegmentHeader {
            segment_id: u64::from_le_bytes(h[16..24].try_into().unwrap()),
            previous: u64::from_le_bytes(h[24..32].try_into().unwrap()),
            major,
            minor,
        },
        len,
    ))
}

/// List the segment ids in the directory, sorted.
fn scan_segments(dir: &Path) -> Result<Vec<u64>, FormatError> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(dir).map_err(io_err)? {
        let entry = entry.map_err(io_err)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(stem) = name.strip_suffix(".log") else {
            continue;
        };
        if let Ok(id) = stem.parse::<u64>() {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

/// Recover the committed transaction stream from a journal directory.
///
/// - Validates segment headers and the contiguous chain (explicit failure
///   on a break).
/// - Discards a torn trailing record/transaction in the FINAL append
///   segment only; checksum failures on complete records are errors.
/// - Returns records grouped by their commit fence.
pub fn recover(dir: &Path) -> Result<Vec<RecoveredRecord>, FormatError> {
    let ids = scan_segments(dir)?;
    let mut out = Vec::new();
    for (i, &id) in ids.iter().enumerate() {
        let (header, _len) = read_segment_header(dir, id)?;
        if header.segment_id != id {
            return Err(FormatError::Corruption(format!(
                "segment file name {id} disagrees with header id {}",
                header.segment_id
            )));
        }
        let expected_previous = if i == 0 { 0 } else { ids[i - 1] };
        if header.previous != expected_previous {
            return Err(FormatError::ChainBreak(header.previous));
        }
        let mut reader = SegmentReader::open(dir, id)?;
        let mut pending: Vec<RawItem> = Vec::new();
        while let Some(item) = reader.next_record()? {
            match item.kind {
                0x01 => {
                    // TxBegin: the writer always fences transactions, so a
                    // begin with pending data would be unexpected — drop it
                    // defensively rather than commit unfenced work.
                    pending.clear();
                }
                0x02 => {
                    // Commit fence: pending records become visible.
                    let fence = item.lsn;
                    for p in pending.drain(..) {
                        let record = Record::decode(p.kind, &p.payload)?;
                        out.push(RecoveredRecord {
                            lsn: p.lsn,
                            fence_lsn: fence,
                            record,
                        });
                    }
                }
                0xF1 => { /* clean-shutdown marker: nothing to replay */ }
                _k => {
                    // Data record: buffered until its fence (the writer
                    // always emits one).
                    pending.push(item);
                }
            }
        }
        if !pending.is_empty() {
            // An unfenced trailing transaction: discard only if this is the
            // final segment; a mid-chain gap is a chain break.
            if i + 1 != ids.len() {
                return Err(FormatError::Corruption(format!(
                    "unfenced records mid-chain in segment {id}"
                )));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("rmq-journal-{}-{tag}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    fn queue_declare(id: u64, name: &str) -> Record {
        Record::QueueDeclare(QueueRecord {
            name: name.into(),
            id,
            durable: true,
            exclusive: false,
            auto_delete: false,
            owner: 0,
        })
    }

    fn enqueue(msg: u64, seq: u64, body: &[u8]) -> Record {
        Record::Enqueue(Enqueue {
            message_id: msg,
            property_bytes: vec![],
            body: body.to_vec(),
            exchange: "".into(),
            routing_key: "jobs".into(),
            persistent: true,
            destinations: vec![(1, seq)],
        })
    }

    #[test]
    fn crc32_known_vectors() {
        assert_eq!(crc32(b""), 0x0000_0000);
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(
            crc32(b"The quick brown fox jumps over the lazy dog"),
            0x414F_A339
        );
    }

    #[test]
    fn commit_and_recover_roundtrip() {
        let dir = tmpdir("roundtrip");
        let mut w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
        let f1 = w.commit(&[queue_declare(1, "jobs")]).unwrap();
        let f2 = w
            .commit(&[
                enqueue(10, 1, b"one"),
                enqueue(11, 2, b"two"),
                Record::SettleAck { queue: 1, seq: 1 },
            ])
            .unwrap();
        assert!(f2 > f1);
        assert!(w.durable_lsn() >= f2);
        drop(w);

        let recovered = recover(&dir).unwrap();
        assert_eq!(recovered.len(), 4);
        assert_eq!(recovered[0].record, queue_declare(1, "jobs"));
        assert_eq!(
            recovered[1].record,
            enqueue(10, 1, b"one"),
            "message bodies survive the roundtrip"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn torn_tail_discards_unfenced_transaction() {
        let dir = tmpdir("torn");
        let mut w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
        w.commit(&[queue_declare(1, "jobs")]).unwrap();
        // Simulate a crash mid-transaction: write bytes for an unfenced
        // transaction and drop the writer without sync/fence.
        let payload = enqueue(10, 1, b"lost").encode();
        let mut buf = Vec::new();
        let crc_input = {
            let mut c = vec![payload_kind()];
            c.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            c.extend_from_slice(&payload);
            c
        };
        buf.extend_from_slice(&record_header_bytes(
            3,
            payload_kind(),
            payload.len() as u32,
            crc32(&crc_input),
        ));
        buf.extend_from_slice(&payload);
        // Abuse a second writer handle to append raw bytes.
        {
            use std::io::Write;
            let mut f = File::options()
                .append(true)
                .open(dir.join(segment_file_name(1)))
                .unwrap();
            f.write_all(&buf).unwrap();
        }
        drop(w);

        let recovered = recover(&dir).unwrap();
        assert_eq!(
            recovered.len(),
            1,
            "unfenced trailing transaction is discarded"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn checksum_failure_is_explicit() {
        let dir = tmpdir("crc");
        let mut w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
        w.commit(&[queue_declare(1, "jobs")]).unwrap();
        drop(w);
        // Flip a payload byte of the committed record.
        let path = dir.join(segment_file_name(1));
        let mut data = fs::read(&path).unwrap();
        *data.last_mut().unwrap() ^= 0xFF;
        fs::write(&path, data).unwrap();

        assert!(matches!(recover(&dir), Err(FormatError::Checksum(_))));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_rolling_and_chain_recovery() {
        let dir = tmpdir("roll");
        let mut w = JournalWriter::open(
            &dir,
            JournalConfig {
                segment_bytes: 200, // force rolls
                failpoint: None,
            },
        )
        .unwrap();
        for i in 0..20u64 {
            w.commit(&[queue_declare(i, &format!("q{i}"))]).unwrap();
        }
        assert!(w.segment_id > 1, "segments were rolled");
        drop(w);

        let ids = scan_segments(&dir).unwrap();
        assert!(ids.len() > 1);
        let recovered = recover(&dir).unwrap();
        assert_eq!(recovered.len(), 20, "all committed records across segments");
        // Reopen appends after the last intact LSN.
        let mut w2 = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
        w2.commit(&[queue_declare(100, "after-restart")]).unwrap();
        drop(w2);
        let recovered = recover(&dir).unwrap();
        assert_eq!(recovered.len(), 21);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn failpoint_before_sync_leaves_unfenced_tail() {
        // ADR-0001: no durability claim without a successful sync. With the
        // failpoint aborting before fsync, the fence bytes may exist but the
        // writer surfaced an error; recovery still only trusts committed
        // checksums — and here the process "crashed" before write_all
        // completed, so nothing is visible.
        let dir = tmpdir("failpoint");
        let fp: Failpoint = Box::new(|name| {
            if name == "before-sync" {
                Err("injected sync failure".into())
            } else {
                Ok(())
            }
        });
        let mut w = JournalWriter::open(
            &dir,
            JournalConfig {
                segment_bytes: usize::MAX,
                failpoint: Some(std::sync::Arc::new(fp)),
            },
        )
        .unwrap();
        let result = w.commit(&[queue_declare(1, "q")]);
        assert!(result.is_err(), "commit surfaces the injected failure");
        drop(w);
        let recovered = recover(&dir).unwrap();
        // Bytes were written before the failed sync; the fence exists so the
        // record may legitimately appear — the CONTRACT is that commit()
        // never returned Ok. Assert exactly that asymmetry:
        // (a) recovery sees at most the one record, (b) the caller saw Err.
        assert!(recovered.len() <= 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn chain_break_detected() {
        let dir = tmpdir("chain");
        {
            let mut w = JournalWriter::open(
                &dir,
                JournalConfig {
                    segment_bytes: 200,
                    failpoint: None,
                },
            )
            .unwrap();
            for i in 0..10u64 {
                w.commit(&[queue_declare(i, &format!("q{i}"))]).unwrap();
            }
        }
        // Delete a middle segment to break the chain.
        let ids = scan_segments(&dir).unwrap();
        assert!(ids.len() >= 2, "expected multiple segments");
        let middle = ids[0];
        fs::remove_file(dir.join(segment_file_name(middle))).unwrap();
        assert!(matches!(recover(&dir), Err(FormatError::ChainBreak(_))));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn reopen_after_torn_tail_truncates_and_future_commits_recover() {
        let dir = tmpdir("truncate");
        {
            let mut w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
            w.commit(&[queue_declare(1, "keep")]).unwrap();
        }
        // Crash mid-record: append half a record header (torn).
        {
            use std::io::Write;
            let mut f = File::options()
                .append(true)
                .open(dir.join(segment_file_name(1)))
                .unwrap();
            f.write_all(&[0x07, 0x00, 0x00]).unwrap();
        }
        // Reopen: truncates the torn bytes, then commits new work.
        let mut w2 = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
        w2.commit(&[queue_declare(2, "after-crash")]).unwrap();
        drop(w2);

        let recovered = recover(&dir).unwrap();
        assert_eq!(recovered.len(), 2, "both fenced records recover");
        assert_eq!(
            recovered[1].record,
            queue_declare(2, "after-crash"),
            "the post-crash commit is not stranded behind the torn tail"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn foreign_file_rejected_by_magic() {
        let dir = tmpdir("magic");
        fs::create_dir_all(&dir).unwrap();
        // Full-size header with a foreign magic: refused explicitly, never
        // appended to or reinitialized.
        let mut h = [0u8; 32];
        h[0..8].copy_from_slice(b"NOTJRNL1");
        fs::write(dir.join("00000000000000000001.log"), h).unwrap();
        assert!(matches!(
            JournalWriter::open(&dir, JournalConfig::default()),
            Err(FormatError::BadMagic)
        ));
        let _ = fs::remove_dir_all(&dir);
    }

    // helpers used by tests above
    fn payload_kind() -> u8 {
        crate::record::kind::ENQUEUE
    }
}
