# ADR-0007: Compaction seals the fully-covered active segment

- Status: Accepted
- Date: 2026-10-03
- Deciders: rusty-mq T28 soak finding (M9-13), maintainer review pending

## Context

Reclamation can only drop journal segments that precede the writer's
active one: the active segment is being appended to, so it can never be
deleted while it is the write target. Segment rotation (default 256 MiB)
therefore *looked* like the mechanism that eventually retires a segment.

The 24-hour churn soak's fail-fast bound check disproved that: with
`segment_bytes` never reached, an entire day of churn (declare →
persistent publishes → settle → delete, ~9 KiB per cycle) lived in ONE
growing segment. Compaction faithfully snapshotted and published the
manifest on every commit — and reclaimed nothing, because the only
segment in existence was the active one. Extrapolated: ~500 MiB of dead
journal at 24 hours. The pre-soak 20-cycle validation passed only because
the young segment was still under the assertion bound.

## Decision

1. Compaction, after publishing the manifest at `covered_lsn`, attempts
   to SEAL the active segment when every record in it is durable at or
   before `covered_lsn` and nothing is pending: the writer rolls to a
   fresh segment (creating + fsyncing its header and the directory
   before the switch, exactly like size-based rotation) and the sealed
   segment becomes reclaimable in the same compaction pass.
2. Safety of the swap against an in-flight flusher batch: the decision is
   made under the group-state lock with `pending` empty. A batch already
   taken out of `pending` lands in the NEW segment; replay of records at
   or below `covered_lsn` is idempotent (INV-11), and the sealed segment
   only ever contained records the snapshot covers — so nothing durable
   is lost or double-applied.
3. Recovery tolerates the resulting severed head link ONLY under a
   manifest (orphan-first walk). The redb projection and
   `last_committed_lsn` apply the same manifest-gated rule — a derived
   index must accept every journal shape the authoritative fold accepts.
   A manifest-less journal with a missing chain head remains a hard
   `ChainBreak` error.
4. A busy journal at seal time defers (try_lock posture, consistent with
   the rest of compaction); the next compaction seals. Conservative, and
   bounded by the compaction cadence.

## Consequences

- The live-journal band is bounded by the compaction cadence rather than
  by segment rotation: reclamation works even when `segment_bytes` is
  effectively unreachable. `segment_bytes` remains a mid-band bound and
  file-management tool, not a correctness dependency.
- Steady state is now asserted by two gates: the nightly 500-cycle churn
  job and the 24-hour soak (`journal < 1 MiB` after forced compaction,
  checked every 2,000 cycles so a leak fails in minutes, not at the
  finish line).
- Regression coverage: `compact_seals_and_reclaims_the_active_segment`
  drives the exact no-rotation shape that failed.
- One more moving piece in compaction (seal + roll between manifest
  publish and reclaim); the ordering is fixed and documented in
  `docs/storage-format.md`.
