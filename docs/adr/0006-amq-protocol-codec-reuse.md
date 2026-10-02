# ADR-0006: Reuse `amq-protocol` for wire types; own the framing state machines

- Status: Accepted
- Date: 2026-10-02
- Deciders: rusty-mq baseline (PRD §8.2 dependency plan)

## Context

The PRD's dependency plan selects `amq-protocol` (BSD-2-Clause, verified as a
low-level codec workspace, R14) with the selection rule "reuse wire
types/codecs; implement server state machines separately". The alternative
was a fully hand-written codec.

## Decision

1. Reuse `amq-protocol` v7.2.x (pinned minor) for: field table/AMQP value
   types, generated method argument structs and their parse/serialize
   functions, content header codec, and SASL PLAIN payload handling.
2. rusty-mq owns: the **incremental frame reader** (chunk-boundary-safe,
   bounded by negotiated `frame_max` before any allocation is trusted),
   frame-end/type/size validation, body reassembly budgets, per-connection
   and per-channel state machines, error scoping, and limit enforcement.
3. The boundary is deliberate: parsers are never fed untrusted length fields
   without the framing layer's budget checks first (PRD §5.1 note: validate
   limits before allocating from an untrusted size field).
4. Version pin: workspace dependency `amq-protocol = "7.2"`; upgrades require
   the interop suite to pass. License notice preserved in
   THIRD_PARTY_NOTICES.md (BSD-2-Clause, allowed per §8.2 licensing policy).

## Consequences

- Wire-format risk concentrates in our framing/limits layer, which is small
  and unit-testable (T02), while argument-level correctness rides the
  battle-tested codec shared with `lapin` (one of the five clients).
- The `U`/`s`, `L`/`l` RabbitMQ table-type quirks are handled by the codec
  consistently with lapin.
- Fuzz targets (M9) will target our framing layer and the codec entry points
  through `cargo-fuzz` corpora.
