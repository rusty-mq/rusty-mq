# ADR-0004: Bounded admission and resource budgets

- Status: Accepted
- Date: 2026-10-02
- Deciders: rusty-mq baseline (PRD §10, INV-09), maintainer review pending

## Context

Every internal queue, cache, and pending-work set must be bounded or paged
(INV-09). Unbounded fanout, prefetch=0 consumers, slow clients, and pending
confirms are the classic broker memory-exhaustion vectors. The PRD requires
byte budgets in addition to count budgets (FR-R02) and alarms that stop new
affected admissions before exhaustion (§10).

## Decision

1. **All** inter-task queues are bounded at construction; there is no
   unbounded channel in the broker. Each has an explicit overflow policy:
   backpressure (reader/writer, coordinator), reject-close (connection
   admission), or alarm-stall (durable admissions under disk alarm).
2. Admission control is layered:
   - per-message: body ≤ `max_message_bytes`, header/table ≤ 64 KiB, checked
     before body assembly begins (311 before admission);
   - per-publish: destination-set size ≤ `max_destinations_per_publish`;
   - per-channel: pending confirms ≤ limit (connection.blocked + publisher
     stall when exceeded);
   - per-queue: ready bytes and counts accounted against the managed buffer
     budget; unacked durable entries are paged, not resident (FR-R08);
   - per-vhost: queues/bindings cardinality caps;
   - process: aggregate managed-buffer watermark + memory alarm; disk-free
     reserve alarms before exhaustion.
3. Byte accounting is first-class: every count budget has a byte counterpart
   where payloads are involved.
4. Under alarm, **new affected admissions stop** but confirmed persistent
   messages are never evicted, and control traffic (heartbeats, acks,
   cancellations) continues with bounded parsing so a mixed
   publish+ack connection cannot deadlock (PRD §10).
5. Slow clients are contained by the bounded writer queue: a connection whose
   writer stays full is eventually closed rather than allowed to monopolize
   memory (FR-R06), leaving unconfirmed outcomes explicitly uncertain.

## Consequences

- Default budgets (256 MiB managed buffers, 512 MiB memory alarm, 1 GiB/10%
  disk reserve) are config ceilings, not capacity claims; T24/T28 are the
  evidence gates.
- The admission path needs a single reservation check point (§9.5 step 3)
  with unwind on failure before any external effect.
