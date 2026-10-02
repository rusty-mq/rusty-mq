# Implementation status and requirement ledger

This is the authoritative requirement-to-implementation-to-test ledger required
by the PRD (§16.2). A requirement is `in_progress` only while its code is
landing; it becomes `complete` only when its acceptance evidence exists and
passes. Compilation is not completion.

Legend: `not_started` / `in_progress` / `blocked` / `complete (partial evidence)` / `complete`.

Updated: 2026-10-02 (M0/M1 development start).

## Milestones

| Milestone | Status | Notes |
| --- | --- | --- |
| M0 — contracts | complete (partial evidence) | Workspace, ledger, baseline fixtures, ADRs exist; baseline digest pinning is a standing TODO for the first CI runner with Docker |
| M1 — connection path | in_progress | lapin (1 of 5 clients) handshakes, opens/closes channels, cleans up; wrong credentials/vhost refused with correct codes; heartbeats (type-8 frames) answered; pika/amqplib/Java/Go fixtures pending (M8 gate) |
| M2 — topology and routing | in_progress | In-memory topology registry and routing matchers landed with tests; not yet wired to protocol methods |
| M3 — delivery state | not_started | |
| M4 — durable authority | not_started | Storage crate scaffold only |
| M5 — confirms and failure safety | not_started | |
| M6 — storage lifecycle | not_started | |
| M7 — secure operations | not_started | |
| M8 — migration and interoperability | not_started | |
| M9 — release qualification | not_started | |

## Requirement ledger

### Protocol and sessions (§5.1)

| ID | Status | Implementation | Evidence | Notes |
| --- | --- | --- | --- | --- |
| FR-P01 | in_progress | `rusty-mq-protocol` framing; server header check | unit tests for header accept/reject | Full accept-path pending |
| FR-P02 | in_progress | connection state machine | integration test (lapin connect) | start-ok PLAIN decode works |
| FR-P03 | in_progress | channel registry + lifecycle | unit + integration tests | |
| FR-P04 | in_progress | tune negotiation (`frame_max`, `channel_max`, heartbeat) | integration test | |
| FR-P05 | in_progress | incremental `FrameReader` across chunk boundaries | unit tests: byte-at-a-time feeds | |
| FR-P06 | in_progress | frame-end validation, body-size budget | unit tests | table depth/count caps pending |
| FR-P07 | in_progress | single writer task per connection | design in ADR-0003 | ordering tests pending |
| FR-P08 | not_started | | | nowait semantics |
| FR-P09 | in_progress | connection/channel close + reclaim | integration tests | |

### Exchanges and routing (§5.2)

| ID | Status | Implementation | Evidence | Notes |
| --- | --- | --- | --- | --- |
| FR-E01 | in_progress | `rusty-mq-core` topology registry | unit tests | not yet behind protocol methods |
| FR-E02 | not_started | | | |
| FR-E03 | not_started | | | |
| FR-E04 | not_started | | | |
| FR-E05 | in_progress | direct/fanout/topic matchers | unit + property tests | |
| FR-E06 | in_progress | destination-set dedup in router | unit tests | |
| FR-E07 | not_started | | | |
| FR-E08 | not_started | | | needs M4 journal |

### Queues (§5.3)

| ID | Status | Implementation | Evidence | Notes |
| --- | --- | --- | --- | --- |
| FR-Q01..FR-Q09 | not_started | profile types defined in core | — | |

### Messages (§5.4), delivery/settlement (§6), resource limits (§10)

All `not_started` except routing groundwork noted above.

### Security (§11), management (§12), CLI/config (§13)

All `not_started`. Pre-M7 binaries bind loopback only (PRD early safety constraint).

## Acceptance test ledger (§17.1)

| ID | Status | Evidence |
| --- | --- | --- |
| T01 | in_progress | `crates/rusty-mq/tests/handshake_lapin.rs` — lapin client handshake + channel open/close (1 of 5 clients) |
| T02 | in_progress | `rusty-mq-protocol` framing unit tests: split frames, merged chunks, bad frame-end, oversize |
| T03 | not_started | |
| T04 | in_progress | `rusty-mq-core` routing unit + property tests (below protocol layer) |
| T05–T30 | not_started | |

## Client-library findings (evidence-backed)

- **lapin 4.12 connect-future hang on server close during open-wait**: when
  the server sends `connection.close` while lapin awaits `connection.open-ok`
  (e.g. unknown vhost), lapin's `InitConnectionShutdown` runs with a `None`
  connection resolver (the pending reply is `Reply::ConnectionOpenOk`, not a
  `ConnectionStep`), so the outer connect promise is never rejected and
  `Connection::connect` hangs. Confirmed by full client-side trace; matches
  the pattern of [lapin#237](https://github.com/sozu-proxy/lapin/issues/237).
  rusty-mq's server behavior is per-protocol (close 403 + bounded linger for
  close-ok). The unknown-vhost test therefore asserts the wire behavior at
  the frame level; the lapin-level assertion is deferred to the M8
  five-client matrix (blocked on a lapin fix/upgrade, not on broker
  behavior).
- **AMQP 0-9-1 heartbeat frames are type 8** (0-8 used 4): verified from the
  amq-protocol codec and encoded correctly by rusty-mq.
- **In-process test-socket async reads can miss readiness wakes** when an
  in-process client pairs with an in-process server on one runtime; the
  frame-level test uses bounded `try_read` polling (test-only concern).

## Known deviations (intentional, per PRD §4.4)

Recorded in [compatibility/features.yaml](../compatibility/features.yaml). None
implemented as *behavior* yet; the V1 rejection profile for deferred features
(TTL, DLX, quorum queues, transactions, `immediate`, per-message `expiration`,
durable+exclusive and durable+auto-delete queue profiles) is enforced from M2
onward as each method path lands.

## Standing TODOs (honest open items)

1. Pin the RabbitMQ reference release container digest in
   `compatibility/baseline.yaml` (requires first CI runner with container
   access; currently marked `pending_verification`).
2. License inventory covers direct dependencies; full transitive `cargo deny`
   integration lands with CI hardening (M9 gate).
3. Five-client matrix: only `lapin` wired so far; pika/amqplib/Java/Go
   fixtures land with `tests/interop/` in M8 (and progressively).
