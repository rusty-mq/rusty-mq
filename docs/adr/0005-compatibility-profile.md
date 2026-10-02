# ADR-0005: Compatibility profile and strict-argument policy

- Status: Accepted
- Date: 2026-10-02
- Deciders: rusty-mq baseline (PRD §4), maintainer review pending

## Context

"Compatible" hides subtle differences (risk table §21). rusty-mq competes on
an explicit, tested envelope: it is stricter than implementations that ignore
unknown arguments, because silently accepting a requested behavior it does
not implement is the worst outcome (INV-13: capability flags never lie).

## Decision

1. The frozen reference for differential behavior is pinned in
   `compatibility/baseline.yaml` (a specific RabbitMQ release + image digest,
   exact client versions). Until that pin is verified on a CI runner,
   differential tests are marked **blocked**, never passed by assumption.
2. Field tables follow the **RabbitMQ wire behavior** (type chars listed in
   docs/protocol-profile.md), because that is what the five target clients
   emit; deviations from raw spec text are recorded, not inherited silently.
3. **Strict argument policy:** unknown or behavior-bearing arguments to
   queue/exchange/binding/consume declares are rejected (406/540 as
   appropriate) rather than ignored. Application *message headers* remain
   opaque and are always preserved (they are data, not topology arguments).
4. Deferred features (TTL, DLX, quorum/stream queues, priorities, tx,
   immediate, direct reply-to, single-active-consumer, ...) are rejected with
   the documented error before any state change — the acceptance test T22
   requires rejection, and features.yaml is the machine-readable matrix.
5. Capability advertisement grows only with implemented+tested paths;
   `product=rusty-mq` with the real version; never a RabbitMQ version string.
6. Queue profiles are restricted in V1 (no durable+exclusive, no
   durable+auto-delete, shared transient queues off by default) — a scoped,
   documented deviation rather than an untested lifecycle claim.

## Consequences

- Migration preflight (M8) must map strict rejections to clear blockers.
- Every intentional deviation needs an entry in features.yaml with the exact
  rejection channel/error so documentation and behavior cannot drift apart.
