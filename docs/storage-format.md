# rusty-mq durable journal format (M4 baseline)

This document specifies the on-disk format of the authoritative append-only
journal (ADR-0002). The journal is the single source of durable truth; the
`redb` projection (M4 later slice) is derived and rebuildable. All integers
are little-endian. The format is independent of compiler memory layout and
Rust enum discriminants: every field is explicitly encoded.

## Segment file layout

```text
journal/
  00000000000000000001.log
  00000000000000000002.log   (rolled when segment_bytes reached)
```

### Segment header (fixed, 32 bytes)

| Offset | Size | Field | Value |
| --- | --- | --- | --- |
| 0 | 8 | magic | `RMQJRNL1` |
| 8 | 4 | format major | `1` (unknown majors refuse startup) |
| 12 | 4 | format minor | `0` |
| 16 | 8 | segment id | monotonically increasing from 1; also the file name |
| 24 | 8 | previous segment id | `0` for the first segment, else the id of the segment this one continues |

Recovery requires the segment chain to be contiguous: a missing link is an
explicit failure (§9.8 rule 5), never a silent skip.

### Records (variable)

| Offset | Size | Field | Notes |
| --- | --- | --- | --- |
| 0 | 8 | LSN | global, monotonic, assigned by the writer |
| 8 | 1 | record kind | see below |
| 9 | 4 | payload length | capped at `u32::MAX`; large payloads chunk across records by the caller |
| 13 | 4 | CRC-32 (IEEE) | over kind + payload-length + payload |
| 17 | N | payload | kind-specific, versioned |

A record with kind `0xF1` (single zero payload byte) is the **end marker**
written on clean shutdown (an optimization only; its absence is not an
error — §9.8).

## Transaction framing

A logical transaction is: one or more data records followed by exactly one
commit-fence record in the same segment:

| Kind byte | Name | Meaning |
| --- | --- | --- |
| 0x01 | `TxBegin` | starts a multi-record transaction (optional for single-record transactions) |
| 0x02 | `TxCommit` | commit fence: all preceding records of the transaction become visible atomically |
| 0x10 | `QueueDeclare` | payload: queue record |
| 0x11 | `QueueDelete` | payload: queue identity |
| 0x12 | `ExchangeDeclare` | payload: exchange record |
| 0x13 | `ExchangeDelete` | payload: exchange identity |
| 0x14 | `Bind` | payload: binding |
| 0x15 | `Unbind` | payload: binding identity |
| 0x20 | `Enqueue` | payload: message + destination set (§9.4: stable queue ids + sequences, never names) |
| 0x21 | `SettleAck` | terminal positive settlement of a queue entry |
| 0x22 | `SettleDiscard` | terminal discard (reject/nack without requeue; also the no-ack terminal dequeue, journaled before exposure §9.6) |
| 0x23 | `Delivered` | delivery-attempt marker: entry exposed to a manual-ack consumer; recovery restores it with the conservative redelivered hint |
| 0x30 | `Purge` | payload: queue id + explicit list of purged entry sequences |
| 0xF1 | `EndMarker` | clean-shutdown marker |

Recovery visibility rule: records are only replayed once their transaction's
`TxCommit` fence has been read intact (checksum included). A trailing
transaction without a commit fence — including one torn mid-record by a
crash — is discarded (§9.8 rule 4). Any *complete* record whose checksum
fails is an explicit startup failure (rule 5): no repair-by-skipping.

The writer always emits an explicit `TxCommit` after every transaction,
single-record or not; recovery therefore requires a fence for visibility and
never commits data implicitly.

## Payload encodings (v1)

Common primitives: `u8/u16/u32/u64` little-endian; `bool` as `u8` (0/1);
`bytes` as `u32` length + data; `string` as `bytes` (UTF-8, enforced by the
encoder only; recovery treats invalid UTF-8 as corruption).

- **Queue record**: string name, u64 internal id, u8 durable, u8 exclusive,
  u8 auto-delete, u64 owning connection id (0 = none).
- **Queue identity**: u64 id.
- **Exchange record**: string name, u64 id, u8 kind (0 direct, 1 fanout,
  2 topic), u8 durable, u8 auto-delete, u8 internal.
- **Exchange identity**: u64 id.
- **Binding**: u64 exchange id, u64 queue id, string routing key.
- **Binding identity**: same triple.
- **Enqueue**: u64 message id, bytes property-blob, bytes body, string
  exchange, string routing key, u8 persistent, u32 destination count,
  then per destination: u64 queue id + u64 queue sequence.
- **SettleAck / SettleDiscard**: u64 queue id, u64 queue sequence.
- **Purge**: u64 queue id, u32 count, then count × u64 sequence (the exact
  set selected at the ordering point — §9.4, never "all below N").

## Synchronization and durability

The writer assigns LSNs in write order. A `commit()` flushes buffered
records to the OS and `fsync`s the segment file; the **durable LSN
watermark** advances to the last fsynced fence only after a successful
sync. A timer expiring is never treated as proof of durability (ADR-0001).
Directory metadata is synced when a new segment file is created. Group
commit (2 ms / 1 MiB triggers) batches syncs without ever acknowledging a
transaction before its fence is fsynced.

## Snapshots, manifest, and reclamation (§9.9, M6)

`snapshots/snapshot-<generation>/state.bin` holds a full durable-state
capture: magic `RMQSNAP1` + format versions + generation + covered LSN +
record count, then a stream of the SAME record frames the journal uses
(kind + length + CRC + payload — no LSNs; identity is the (queue, seq)
pair). The recovery root is `MANIFEST` (magic `RMQMNFST`, generation,
covered LSN, snapshot directory name), published atomically: write
`MANIFEST.tmp`, fsync, rename, fsync the directory.

Recovery order: snapshot records fold first, then journal records with LSN
> covered re-apply — the same idempotent fold (INV-11). The covered LSN is
read after the state capture, so it is ≥ every captured event; suffix
records between the capture moment and the covered LSN simply re-apply.

Reclamation deletes journal segments whose every record LSN ≤ covered, never
the writer's current tail (an unlinked tail would silently lose appends),
plus snapshot directories older than the manifest's generation. With a
manifest present, recovery tolerates the first remaining segment pointing
at a reclaimed predecessor; without one, the chain must be intact (a
leading gap without a manifest is an explicit chain-break failure).

The broker triggers snapshot+reclaim inline after a commit when journal
bytes exceed a ceiling; the capture uses try_lock, so a contended round
defers to the next commit instead of deadlocking against callers that hold
state locks across `journal_commit` (the persistent publish path).

## Offline backup and the LOCK (§9.10, M6)

`LOCK` holds the writer's pid (session state; never part of backups).
Locking is cross-process: a live foreign pid refuses writer open and any
backup/restore; a lock carrying the current process's pid is taken over
(an aborted in-process writer cannot run Drop and restart must succeed —
the documented same-process limitation until OS-level flock arrives); a
stale lock (dead pid) is tolerated. PID reuse is a known limitation.

`backup create` copies the recovery chain (segments, `snapshots/`,
`MANIFEST`; `LOCK`/`MANIFEST.tmp` excluded) into a fresh output directory
and refuses while a live writer holds the source. Every backup carries a
`CHECKSUMS` sidecar: one `<sha256-hex>␠␠<relative/path>` line per copied
file, sorted by path (UTF-8, `\n`-terminated). `backup verify` first
checks every digest and that the file set matches exactly (added or
removed files fail), then replays the copy through the real recovery
fold — a backup that cannot be recovered is not valid. A backup without
`CHECKSUMS` is refused. `backup restore` verifies first, refuses a
nonempty target, and never merges or overwrites; `CHECKSUMS` itself is
backup metadata and never lands in a data directory.

## Implementation status

Writer, reader, broker wiring (durable declarations, persistent enqueue,
settlements, §9.6 delivery safety), restart replay, and the M6
snapshot/manifest/reclamation lifecycle described above are implemented and
tested; see docs/implementation-status.md for the evidence ledger.
