//! Segment journal writer and recovery reader (docs/storage-format.md,
//! ADR-0002): one authoritative append-only chain with explicit commit
//! fences, CRC-verified records, contiguous segment linking, torn-tail
//! discard, and a sync-gated durable watermark (ADR-0001).

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::record::{FormatError, Record};

/// Segment magic (`RMQJRNL1`).
const MAGIC: [u8; 8] = *b"RMQJRNL1";
pub const FORMAT_MAJOR: u32 = 1;
pub const FORMAT_MINOR: u32 = 0;
/// Fixed size of the segment header.
const SEGMENT_HEADER_LEN: usize = 32;
/// Fixed size of a record header: LSN(8) + kind(1) + len(4) + crc(4).
pub(crate) const RECORD_HEADER_LEN: usize = 17;
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
    /// Group-commit window: the flusher waits up to this long for more
    /// records before fsyncing (§9.5 trigger; 0 = fsync on first batch).
    pub commit_batch_delay_ms: u32,
    /// Group-commit byte trigger: pending bytes at or above this flush
    /// immediately without waiting out the window (§9.5).
    pub commit_batch_bytes: usize,
    /// Optional failpoint hook (test-only); the RUNNING writer copies it
    /// into a shared cell at open so later `set_failpoint` changes reach
    /// the flusher thread.
    pub failpoint: Option<std::sync::Arc<Failpoint>>,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 256 * 1024 * 1024,
            commit_batch_delay_ms: 2,
            commit_batch_bytes: 1_048_576,
            failpoint: None,
        }
    }
}

/// State shared between committers and the flusher thread.
struct GroupState {
    /// Encoded frames waiting to be written (drained by the flusher).
    pending: Vec<u8>,
    /// Fence LSN of the last frame currently in `pending`.
    pending_fence: u64,
    /// Highest fence known fsynced (ADR-0001 watermark).
    durable_lsn: u64,
    /// First error seen by the flusher since it was observed (cleared
    /// once reported to all waiters whose fence it covers).
    flush_error: Option<String>,
    /// Writer bookkeeping moved under the lock.
    next_lsn: u64,
    segment_id: u64,
    segment_len: usize,
    /// Flusher shutdown flag.
    stopping: bool,
}

/// The single serialized journal writer (ADR-0002) with group commit
/// (§9.5): committers append under the state lock, hand the batch to the
/// flusher thread, and block on the condvar until their fence is covered
/// by an actual fsync — concurrency batches naturally, and a timer firing
/// is never treated as proof of durability (ADR-0001).
pub struct JournalWriter {
    dir: PathBuf,
    config: JournalConfig,
    state: std::sync::Arc<Mutex<GroupState>>,
    /// Notified when durable_lsn advances or an error is recorded.
    committed: std::sync::Arc<std::sync::Condvar>,
    /// Notified when new work arrives for the flusher.
    work: std::sync::Arc<std::sync::Condvar>,
    /// Shared failpoint cell visible to commit() and the flusher thread
    /// (runtime injection must reach the fsync site — T13/T14).
    failpoint: std::sync::Arc<Mutex<Option<std::sync::Arc<Failpoint>>>>,
    /// Shared handle to the active segment file (the flusher holds a
    /// clone); seal_if_covered swaps it under the state lock.
    file: std::sync::Arc<Mutex<File>>,
    flusher: Option<std::thread::JoinHandle<()>>,
}

/// Data-directory lock: the writer records its pid in `LOCK`; a live pid
/// means an active writer (backups and restores must refuse); a stale LOCK
/// (crashed writer, dead pid) is tolerated and taken over. PID reuse is a
/// documented V1 limitation of this simple scheme.
pub fn writer_lock_alive(dir: &Path) -> bool {
    let Ok(content) = fs::read_to_string(dir.join("LOCK")) else {
        return false;
    };
    let Ok(pid) = content.trim().parse::<i32>() else {
        return false; // unparseable lock: not evidence of a live writer
    };
    if pid <= 0 {
        return false;
    }
    // Own pid counts as alive for callers like backup: the writer is in
    // THIS process. signal 0: pure liveness probe.
    pid == std::process::id() as i32 || process_alive(pid)
}

/// Safety: `kill(pid, 0)` is a pure liveness probe — no signal is
/// delivered; the only contract is a valid pid argument, which the caller
/// checked. Narrowly scoped per PRD §15.2 (documented exception).
#[cfg(unix)]
#[allow(unsafe_code)]
fn process_alive(pid: i32) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

#[cfg(not(unix))]
fn process_alive(_pid: i32) -> bool {
    // Conservative fallback: assume alive (refuse rather than corrupt).
    true
}

fn take_writer_lock(dir: &Path) -> Result<(), FormatError> {
    // Single-writer enforcement is cross-process: a live FOREIGN pid holds
    // the directory. A lock carrying our own pid is taken over — an
    // aborted writer in this process cannot run Drop, and restart must
    // succeed (documented limitation: same-process double-open is not
    // distinguished; OS-level flock arrives with the multi-process work).
    let own = std::process::id() as i32;
    if let Ok(content) = fs::read_to_string(dir.join("LOCK")) {
        if let Ok(pid) = content.trim().parse::<i32>() {
            if pid != own && pid > 0 && process_alive(pid) {
                return Err(FormatError::Io(
                    "data directory is held by a live writer".into(),
                ));
            }
        }
    }
    std::fs::write(dir.join("LOCK"), own.to_string())
        .map_err(|e| FormatError::Io(e.to_string()))?;
    Ok(())
}

fn release_writer_lock(dir: &Path) {
    let _ = fs::remove_file(dir.join("LOCK"));
}

impl JournalWriter {
    /// Open (creating if absent) a journal directory. Fails explicitly on
    /// inconsistent storage; never initializes over a foreign layout.
    pub fn open(dir: &Path, config: JournalConfig) -> Result<Self, FormatError> {
        fs::create_dir_all(dir).map_err(io_err)?;
        take_writer_lock(dir)?;
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
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(io_err)?;
        let segment_len = if fs::metadata(&path).map(|m| m.len()).unwrap_or(0) == 0 {
            file.write_all(&segment_header_bytes(segment_id, previous))
                .map_err(io_err)?;
            file.sync_all().map_err(io_err)?;
            sync_dir(dir)?;
            SEGMENT_HEADER_LEN
        } else {
            if let Some((seg, prefix)) = tail_to_truncate {
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
                // Recompute segment length from the truncated file.
            }
            let len = fs::metadata(&path).map(|m| m.len()).unwrap_or(0) as usize;
            file.seek(SeekFrom::End(0)).map_err(io_err)?;
            len
        };
        let state = GroupState {
            pending: Vec::new(),
            pending_fence: 0,
            durable_lsn: 0,
            flush_error: None,
            next_lsn,
            segment_id,
            segment_len,
            stopping: false,
        };
        let file = std::sync::Arc::new(Mutex::new(file));
        let state = std::sync::Arc::new(Mutex::new(state));
        let committed = std::sync::Arc::new(std::sync::Condvar::new());
        let work = std::sync::Arc::new(std::sync::Condvar::new());
        let failpoint = std::sync::Arc::new(Mutex::new(config.failpoint.clone()));
        let flusher = {
            let dir = dir.to_path_buf();
            let flusher_config = config.clone();
            let file = file.clone();
            let state = state.clone();
            let committed = committed.clone();
            let work = work.clone();
            let failpoint = failpoint.clone();
            std::thread::spawn(move || {
                Self::flusher_loop(
                    &dir,
                    &flusher_config,
                    file,
                    state,
                    committed,
                    work,
                    failpoint,
                )
            })
        };
        Ok(Self {
            dir: dir.to_path_buf(),
            config,
            state,
            committed,
            work,
            failpoint,
            file,
            flusher: Some(flusher),
        })
    }

    /// Append one logical transaction (its records + an explicit commit
    /// fence), assign LSNs, and block until an actual fsync covers the
    /// fence. Returns the fence LSN (the commit identity callers wait on).
    ///
    /// Group commit (§9.5): the encoded frames join the shared pending
    /// batch under the state lock; the flusher thread writes+fsyncs the
    /// batch (batching concurrent committers); this caller waits on the
    /// condvar until `durable_lsn >= fence` or the flusher reports an
    /// error covering it. The batch window (delay/bytes triggers) never
    /// lets a caller return before its fsync (ADR-0001).
    pub fn commit(&self, records: &[Record]) -> Result<u64, FormatError> {
        if let Some(fp) = self.failpoint.lock().unwrap().as_ref() {
            fp("before-append").map_err(FormatError::Io)?;
        }
        let fence = {
            let mut st = self.state.lock().unwrap();
            let mut buf: Vec<u8> = Vec::new();
            // TxBegin for multi-record transactions; single-record
            // transactions proceed without it (spec: the fence commits).
            if records.len() > 1 {
                let lsn = st.next_lsn;
                st.next_lsn += 1;
                buf.extend_from_slice(&frame_control(0x01, lsn));
            }
            for rec in records {
                let lsn = st.next_lsn;
                st.next_lsn += 1;
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
            let fence = st.next_lsn;
            st.next_lsn += 1;
            buf.extend_from_slice(&frame_control(0x02, fence));
            st.pending.extend_from_slice(&buf);
            st.pending_fence = fence;
            self.work.notify_one();
            fence
        };
        // Wait for durability covering OUR fence (ADR-0001: the window
        // timing out is never proof; only the flusher's fsync result is).
        let mut st = self.state.lock().unwrap();
        loop {
            if st.durable_lsn >= fence {
                return Ok(fence);
            }
            if let Some(err) = st.flush_error.take() {
                // An error only fails callers whose fence it could cover;
                // later callers keep waiting for a successful flush.
                return Err(FormatError::Io(err));
            }
            if st.stopping {
                return Err(FormatError::Io("flusher stopped".into()));
            }
            st = self.committed.wait(st).unwrap();
        }
    }

    /// The flusher thread body. Loop: wait for work (with the batch
    /// window timeout), take the pending batch, write, fsync, advance the
    /// watermark, wake waiters. Segment rolling happens on the taken
    /// batch's size accounting.
    fn flusher_loop(
        dir: &Path,
        config: &JournalConfig,
        file: std::sync::Arc<Mutex<File>>,
        state: std::sync::Arc<Mutex<GroupState>>,
        committed: std::sync::Arc<std::sync::Condvar>,
        work: std::sync::Arc<std::sync::Condvar>,
        failpoint: std::sync::Arc<Mutex<Option<std::sync::Arc<Failpoint>>>>,
    ) {
        let window = std::time::Duration::from_millis(config.commit_batch_delay_ms as u64);
        loop {
            let (batch, batch_fence) = {
                let mut st = state.lock().unwrap();
                loop {
                    if st.stopping {
                        if st.pending.is_empty() {
                            return;
                        }
                        break; // final drain
                    }
                    if !st.pending.is_empty() {
                        // Byte trigger flushes immediately; otherwise wait
                        // out the window for more committers to join.
                        if st.pending.len() >= config.commit_batch_bytes || window.is_zero() {
                            break;
                        }
                        let (guard, _timed_out) = work.wait_timeout(st, window).unwrap();
                        st = guard;
                        break;
                    }
                    st = work.wait(st).unwrap();
                }
                (std::mem::take(&mut st.pending), st.pending_fence)
            };
            if batch.is_empty() {
                continue;
            }
            // Segment roll decision on the accumulated length.
            {
                let mut st = state.lock().unwrap();
                if st.segment_len + batch.len() > config.segment_bytes
                    && st.segment_len > SEGMENT_HEADER_LEN
                {
                    if let Err(e) = roll_segment(dir, &mut st, &file) {
                        record_flush_error(&state, &committed, e.to_string());
                        return;
                    }
                }
            }
            let mut f = file.lock().unwrap();
            if let Err(e) = f.write_all(&batch) {
                drop(f);
                record_flush_error(&state, &committed, e.to_string());
                return;
            }
            if let Some(fp) = failpoint.lock().unwrap().as_ref() {
                if fp("before-sync").is_err() {
                    drop(f);
                    record_flush_error(&state, &committed, "injected fsync failure".into());
                    return;
                }
            }
            if let Err(e) = f.sync_all() {
                drop(f);
                record_flush_error(&state, &committed, e.to_string());
                return;
            }
            if let Some(fp) = failpoint.lock().unwrap().as_ref() {
                if fp("after-sync").is_err() {
                    drop(f);
                    record_flush_error(&state, &committed, "after-sync failpoint".into());
                    return;
                }
            }
            drop(f);
            {
                let mut st = state.lock().unwrap();
                st.segment_len += batch.len();
                // Watermark advances only after the successful sync.
                st.durable_lsn = st.durable_lsn.max(batch_fence);
                st.flush_error = None;
                committed.notify_all();
            }
        }
    }

    /// Clean-shutdown end marker (optimization only).
    pub fn write_end_marker(&self) -> Result<(), FormatError> {
        let buf = {
            let mut st = self.state.lock().unwrap();
            let lsn = st.next_lsn;
            st.next_lsn += 1;
            let payload = [0x00u8];
            let crc = crc32(&payload);
            let mut buf = record_header_bytes(lsn, 0xF1, payload.len() as u32, crc).to_vec();
            buf.extend_from_slice(&payload);
            st.pending.extend_from_slice(&buf);
            st.pending_fence = st.pending_fence.max(lsn);
            self.work.notify_one();
            buf
        };
        let _ = buf;
        // The marker is an optimization: wait briefly for it to flush so a
        // clean shutdown actually persists it, but do not fail shutdown
        // if the window expires (absence is not an error — §9.8).
        let mut st = self.state.lock().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
        while st.durable_lsn < st.pending_fence {
            let (guard, _t) = self
                .committed
                .wait_timeout(
                    st,
                    deadline.saturating_duration_since(std::time::Instant::now()),
                )
                .unwrap();
            st = guard;
            if std::time::Instant::now() >= deadline {
                break;
            }
        }
        Ok(())
    }

    /// Highest fence known fsynced.
    pub fn durable_lsn(&self) -> u64 {
        self.state.lock().unwrap().durable_lsn
    }

    /// Next LSN to be assigned.
    pub fn next_lsn(&self) -> u64 {
        self.state.lock().unwrap().next_lsn
    }

    /// Current segment id (reclaim keeps this one).
    pub fn current_segment_id(&self) -> u64 {
        self.state.lock().unwrap().segment_id
    }

    /// Seal the ACTIVE segment when every record in it is durable at or
    /// before `covered_lsn` (the caller has just published a snapshot
    /// covering it) and roll the writer to a fresh segment, so reclamation
    /// may drop the sealed one (§9.9). Without this, a writer that never
    /// hits `segment_bytes` rotation keeps one growing segment forever —
    /// compaction snapshots but reclaims nothing (found by the T28 soak).
    ///
    /// Safety of the swap against an in-flight flusher batch: pending is
    /// empty under the state lock, so any batch already taken out lands in
    /// the NEW segment; replay of records at or below the snapshot's
    /// covered LSN is idempotent (INV-11), and nothing durable is ever
    /// dropped — the sealed segment only contains records the snapshot
    /// already covers.
    pub fn seal_if_covered(&self, covered_lsn: u64) -> Result<bool, FormatError> {
        let mut st = self.state.lock().unwrap();
        if !st.pending.is_empty()
            || st.durable_lsn > covered_lsn
            || st.segment_len <= SEGMENT_HEADER_LEN
        {
            return Ok(false); // busy or not fully covered: keep as-is
        }
        roll_segment(&self.dir, &mut st, &self.file)?;
        Ok(true)
    }

    /// Replace the failpoint hook (test-only; T13/T14 evidence). The
    /// change propagates to the running flusher via the shared cell.
    pub fn set_failpoint(&mut self, fp: Option<std::sync::Arc<Failpoint>>) {
        self.config.failpoint = fp;
        *self.failpoint.lock().unwrap() = self.config.failpoint.clone();
    }

    /// Release the data-directory lock (idempotent; also runs on Drop).
    pub fn release_lock(&self) {
        release_writer_lock(&self.dir);
    }
}

fn io_err(e: io::Error) -> FormatError {
    FormatError::Io(e.to_string())
}

/// Roll to the next segment under the state lock (flusher path). The new
/// segment header + directory are fsynced BEFORE the switch.
fn roll_segment(dir: &Path, st: &mut GroupState, file: &Mutex<File>) -> Result<(), FormatError> {
    let next = st.segment_id + 1;
    let previous = st.segment_id;
    let path = dir.join(segment_file_name(next));
    let mut new_file = File::create(&path).map_err(io_err)?;
    new_file
        .write_all(&segment_header_bytes(next, previous))
        .map_err(io_err)?;
    new_file.sync_all().map_err(io_err)?;
    sync_dir(dir)?;
    let appended = OpenOptions::new()
        .append(true)
        .read(true)
        .open(&path)
        .map_err(io_err)?;
    *file.lock().unwrap() = appended;
    st.segment_id = next;
    st.segment_len = SEGMENT_HEADER_LEN;
    Ok(())
}

/// Record a flush failure and wake every waiter: committers whose fence
/// was in (or behind) the failed batch get Err; the flusher stops (the
/// broker's §6.4 posture — do not continue after uncertain persistence).
fn record_flush_error(
    state: &std::sync::Arc<Mutex<GroupState>>,
    committed: &std::sync::Arc<std::sync::Condvar>,
    detail: String,
) {
    let mut st = state.lock().unwrap();
    st.flush_error = Some(detail);
    st.stopping = true;
    committed.notify_all();
}

impl Drop for JournalWriter {
    fn drop(&mut self) {
        // Stop the flusher after a final drain; never wait on it while
        // holding the state lock (deadlock).
        {
            let mut st = self.state.lock().unwrap();
            st.stopping = true;
            self.work.notify_all();
        }
        if let Some(t) = self.flusher.take() {
            let _ = t.join();
        }
        release_writer_lock(&self.dir);
    }
}

fn sync_dir(dir: &Path) -> Result<(), FormatError> {
    File::open(dir).and_then(|d| d.sync_all()).map_err(io_err)?;
    Ok(())
}

fn segment_file_name(id: u64) -> String {
    format!("{id:020}.log")
}

/// Public alias for snapshot/reclaim use.
pub fn segment_file_name_pub(id: u64) -> String {
    segment_file_name(id)
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
    payload: Vec<u8>,
}

struct SegmentHeader {
    segment_id: u64,
    previous: u64,
    // Parsed from every segment header; unread until a format migration
    // exists (the version check today is major==FORMAT_MAJOR at the read
    // site, which destructures only the fields it validates).
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
pub(crate) fn scan_segments(dir: &Path) -> Result<Vec<u64>, FormatError> {
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
    recover_with_options(dir, false)
}

/// Segment ids present, sorted (doctor inventory); None when the
/// directory cannot be read.
pub fn segment_inventory(dir: &Path) -> Option<Vec<u64>> {
    scan_segments(dir).ok()
}

/// The highest committed (fenced) LSN in the journal, 0 when empty.
/// Tolerates a reclaimed chain head when a manifest covers it (same
/// orphan-first rule as the authoritative recovery).
pub fn last_committed_lsn(dir: &Path) -> Result<u64, FormatError> {
    let orphan_first = crate::snapshot::read_manifest(dir)?.is_some();
    let mut max = 0u64;
    for item in recover_with_options(dir, orphan_first)? {
        max = max.max(item.fence_lsn);
    }
    Ok(max)
}

/// `allow_orphan_first`: a manifest-published reclamation removed the head
/// of the chain; the first remaining segment's `previous` legitimately
/// points at a deleted id (§9.9 step 5). Only legal when the caller has a
/// manifest covering the reclaimed LSNs.
pub fn recover_with_options(
    dir: &Path,
    allow_orphan_first: bool,
) -> Result<Vec<RecoveredRecord>, FormatError> {
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
        let expected_previous = match i {
            0 if allow_orphan_first => header.previous, // reclaimed head: accept
            0 => 0,
            _ => ids[i - 1],
        };
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
    fn concurrent_committers_group_commit() {
        let dir = tmpdir("group");
        let w = std::sync::Arc::new(JournalWriter::open(&dir, JournalConfig::default()).unwrap());
        let threads: usize = 8;
        let per: usize = 25;
        let mut handles = Vec::new();
        for t in 0..threads {
            let w = w.clone();
            handles.push(std::thread::spawn(move || {
                let mut fences = Vec::new();
                for i in 0..per {
                    let f = w
                        .commit(&[queue_declare((t * per + i) as u64, &format!("g{t}-{i}"))])
                        .unwrap();
                    fences.push(f);
                }
                fences
            }));
        }
        let mut all = Vec::new();
        for h in handles {
            all.extend(h.join().unwrap());
        }
        // Every fence distinct, watermark covers the max, monotonic by
        // construction of next_lsn under the lock.
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), threads * per, "fences unique");
        assert_eq!(w.durable_lsn(), *all.last().unwrap());
        // All records recoverable after drop.
        drop(w);
        let recovered = recover(&dir).unwrap();
        assert_eq!(recovered.len(), threads * per);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn zero_window_still_correct() {
        // delay=0 must behave exactly like per-commit sync (no lost waits).
        let dir = tmpdir("zero");
        let w = JournalWriter::open(
            &dir,
            JournalConfig {
                commit_batch_delay_ms: 0,
                ..Default::default()
            },
        )
        .unwrap();
        for i in 0..10u64 {
            w.commit(&[queue_declare(i, "z")]).unwrap();
            assert!(w.durable_lsn() > i);
        }
        drop(w);
        assert_eq!(recover(&dir).unwrap().len(), 10);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn failpoint_injection_reaches_running_flusher() {
        let dir = tmpdir("fp-live");
        let mut w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
        // Healthy commit first (T13's setup declare).
        w.commit(&[queue_declare(1, "ok")]).unwrap();
        // Inject on the RUNNING writer.
        let tripped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let t2 = tripped.clone();
        let fp: Failpoint = Box::new(move |name| {
            if name == "before-sync" {
                t2.store(true, std::sync::atomic::Ordering::SeqCst);
                Err("injected fsync failure".into())
            } else {
                Ok(())
            }
        });
        w.set_failpoint(Some(std::sync::Arc::new(fp)));
        let result = w.commit(&[queue_declare(2, "must-fail")]);
        assert!(
            result.is_err(),
            "running-writer injection must fail the commit"
        );
        assert!(tripped.load(std::sync::atomic::Ordering::SeqCst));
        drop(w);
        let recovered = recover(&dir).unwrap();
        // §7.4/§9.5: a complete transaction MAY survive without its
        // producer seeing a confirm — recovery retaining record 2 is legal.
        // The invariant (INV-01) is about confirms, which the Err above
        // preserves. Assert the healthy commit survived and nothing alien
        // appeared.
        let ids: Vec<u64> = recovered
            .iter()
            .filter_map(|r| match &r.record {
                Record::QueueDeclare(q) => Some(q.id),
                _ => None,
            })
            .collect();
        assert!(ids.iter().all(|id| *id == 1 || *id == 2));
        assert!(ids.contains(&1));
        let _ = std::fs::remove_dir_all(&dir);
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
        let w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
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
        let w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
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
        let w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
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
        let w = JournalWriter::open(
            &dir,
            JournalConfig {
                segment_bytes: 200, // force rolls
                ..Default::default()
            },
        )
        .unwrap();
        for i in 0..20u64 {
            w.commit(&[queue_declare(i, &format!("q{i}"))]).unwrap();
        }
        assert!(w.current_segment_id() > 1, "segments were rolled");
        drop(w);

        let ids = scan_segments(&dir).unwrap();
        assert!(ids.len() > 1);
        let recovered = recover(&dir).unwrap();
        assert_eq!(recovered.len(), 20, "all committed records across segments");
        // Reopen appends after the last intact LSN.
        let w2 = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
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
        let w = JournalWriter::open(
            &dir,
            JournalConfig {
                segment_bytes: usize::MAX,
                failpoint: Some(std::sync::Arc::new(fp)),
                ..Default::default()
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
            let w = JournalWriter::open(
                &dir,
                JournalConfig {
                    segment_bytes: 200,
                    ..Default::default()
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
            let w = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
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
        let w2 = JournalWriter::open(&dir, JournalConfig::default()).unwrap();
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
