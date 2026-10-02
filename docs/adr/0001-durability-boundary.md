# ADR-0001: Durability boundary and confirm timing

- Status: Accepted
- Date: 2026-10-02
- Deciders: rusty-mq baseline (PRD §7, §9.5), maintainer review pending

## Context

Publisher confirms are the application-facing durability contract. The PRD's
INV-01/INV-02 require that no positive confirm is emitted before the
responsible persistence barrier succeeds, and that confirmed persistent
entries remain recoverable until a valid terminal action. Several plausible
implementation shortcuts (confirm on socket write, confirm on journal append
before fsync, confirm on in-memory apply) all violate this.

## Decision

1. The **durable boundary** for a persistent enqueue is: journal record set +
   commit fence written, `fsync` (file data + required directory metadata)
   completed, durable LSN watermark advanced — only then is live state updated
   and the confirm serialized.
2. A timer-based or group-commit batch completion is necessary but not
   sufficient: the batch callback must carry the actual synchronization
   outcome. A timer firing is never treated as proof of durability.
3. Confirm emission ordering per channel is FIFO with publication order; a
   mandatory-return must be serialized before the corresponding positive
   confirm.
4. Durable *declarations* (queue/exchange/binding declare, purge, delete,
   permission changes) wait for the same commit barrier before their
   success reply (`nowait` suppresses the reply, not the barrier).
5. Transient messages and temporary queues use bounded in-memory admission
   and are never positively claimed as persistent (INV-14: storage errors may
   never silently demote durable to memory).

## Consequences

- Durable path latency is at least one fsync per commit batch; group commit
  (proposed 2 ms / 1 MiB triggers) is the batching mechanism, never a
  substitute for the barrier.
- The confirm path requires an explicit completion token from the storage
  executor; the type system distinguishes `Accepted`, `Committed`,
  `Applied`, `Confirmed` states (PRD §15.2).
- Test T13 (injected sync failure) must observe that no confirm precedes a
  failed sync; this is the acceptance evidence for this ADR.
