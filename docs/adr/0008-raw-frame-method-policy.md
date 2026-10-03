# ADR-0008: Raw-frame method policy for fields the codec drops

- Status: Accepted
- Date: 2026-10-03
- Deciders: rusty-mq differential-matrix finding (M9-19/20), maintainer
  review pending

## Context

ADR-0006 reuses amq-protocol for wire types and codecs. That crate does
not model every field of the AMQP 0-9-1 grammar: reserved/legacy fields
are omitted from the generated structs and their parsers *parse and
discard* them. `basic.qos.prefetch_size` (a reserved u32) is the
concrete case: the frozen compatibility profile rejects a nonzero value
with a channel-scoped 540, `features.yaml` claimed that rejection — but
the codec silently dropped the field, so the server answered `qos-ok`
to a request it never saw. The differential profile matrix made the gap
visible as data (a conformance mismatch) after it had been invisible to
every client-suite test, because no client library can be asked to send
what the codec will not encode.

## Decision

1. Frozen-profile rules about fields the codec does not model are
   enforced on the RAW method payload bytes, in `FrameReader`, at the
   frame-decode boundary — before the parser runs, alongside the
   field-table budget walk (same precedent, same layer).
2. The current policy set is exactly one rule: `(class 60, method 10)`
   with a nonzero u32 at payload offset 4 (prefetch_size) produces a
   CHANNEL-scoped `ProtocolError::not_implemented(540)`.
3. Violations are surfaced as a per-frame *pending* value drained by the
   connection drive loop (`take_policy_violation`): the connection sends
   `channel.close` through the existing channel-error path (state
   dropped, close-ok interlude armed) and skips dispatching the
   violating frame. The connection survives; the scope is the frozen
   profile's choice, deliberately different from the baseline (which
   connection-kills).
4. Adding a rule means: raw byte offsets for that method, frozen-profile
   citation, a unit test on the policy function, a matrix case, and a
   hand-crafted frame integration test (the codec cannot express the
   input, so tests build the method bytes directly).

## Consequences

- The frozen profile is enforceable even where the codec is lossy; the
  prefetch_size claim in `features.yaml` is true again (conformance
  16/16 with reply codes, `compatibility/differential/`).
- A small raw parser lives beside the codec — bounded by policy: it only
  reads fixed offsets for specific (class, method) pairs, never a
  general grammar. Drift risk is pinned by the round-trip tests and the
  differential matrix, both in CI.
- The pending-violation channel on `FrameReader` is stateful by design
  (decode-time detection, dispatch-time reporting) so `next_frame`'s
  signature stays stable for all other callers.
