# ADR-0002: One authoritative append-only journal; redb as projection

- Status: Accepted
- Date: 2026-10-02
- Deciders: rusty-mq baseline (PRD §9.1, §9.7), maintainer review pending

## Context

The unsafe pattern to avoid: commit metadata in one store, append message
payloads in another, and hope both survive a crash together. Any design with
two independently-committed sources of durable truth can acknowledge state
that is not recoverable.

## Decision

1. The authoritative recovery state is: **an atomically published manifest
   pointing at an immutable snapshot, plus the ordered committed journal
   suffix after that snapshot.** Before the first snapshot, the journal alone
   is the authority.
2. All durable facts — topology mutations, principals/permissions, persistent
   enqueues with destination sets, delivery-attempt markers, settlements,
   purges/deletes, checkpoint metadata — are represented in this one chain.
3. `redb` (crates.io, MIT OR Apache-2.0) is used strictly as a **derived,
   rebuildable index/checkpoint accelerator**. Its `applied_lsn` advances
   atomically with the index updates it covers; a projection is never written
   from journal events beyond the runtime durable watermark.
4. On startup, projection generation/schema/LSN are verified against the
   recovery chain; a missing, stale, or corrupt projection is **rebuilt from
   authoritative data**, never trusted as truth.
5. Startup fails explicitly on: checksum mismatch in a complete record,
   missing required segment, broken chain, unsupported major format. Only a
   physically incomplete trailing record set in the final segment (intact
   committed prefix) may be discarded.
6. An inconsistent nonempty data directory is never silently replaced with an
   empty broker.

## Consequences

- redb can be dropped/deleted at any time with only rebuild cost, which also
  defines the offline backup rule: back up the manifest/snapshot/journal
  chain, never `state.redb` alone.
- Multi-record logical transactions need an explicit commit fence in the
  journal format (design lives in docs/storage-format.md at M4).
- Journal record payloads reference stable opaque IDs (never user queue
  names) so replay does not depend on name resolution.
