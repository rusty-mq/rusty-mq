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
| M2 — topology and routing | in_progress | Publish→route→get round trips verified with lapin: direct/topic/fanout/default-exchange routing, INV-05 dedup, typed property roundtrip, mandatory NO_ROUTE returns, manual-ack settlement + requeue-on-channel-close, real message counts. Remaining for the M2 exit gate: publish-time permission surface review + differential fixtures |
| M3 — delivery state | in_progress | Settlement surface complete: ack/reject/nack (single+multiple, discard vs requeue with original position + redelivered hint), basic.recover(requeue=true) with requeue=false 540, unknown-tag 406; consumers with round-robin + prefetch credit; auto-delete; cancel-notify; requeue on channel/connection loss. Remaining M3: T09 concurrency stress, channel.flow decision |
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
| FR-E01 | in_progress | built-ins predeclared per vhost | core unit tests; lapin `builtin_amq_direct_is_declared_passively` | |
| FR-E02 | complete (partial evidence) | declare + passive + equivalence | `topology_lapin.rs` roundtrip + 406/404 tests | delete paths tested; auto-delete lifecycle unit-tested in core |
| FR-E03 | in_progress | durability flags stored; auto-delete exchange lifecycle in core | core unit tests | internal=true publish gate lands with publish slice |
| FR-E04 | complete (partial evidence) | bind/unbind idempotent | lapin roundtrip (`duplicate bind idempotent`) | |
| FR-E05 | complete (partial evidence) | routing end-to-end on publish | `topic_fanout_routing_and_inv05`, `publish_route_get_roundtrip_with_properties`, `default_exchange_routes_by_queue_name` | |
| FR-E06 | complete (partial evidence) | destination-set dedup | `topic_fanout_routing_and_inv05` (two matching bindings → one copy) | |
| FR-E07 | complete (partial evidence) | default exchange routes by queue name | `default_exchange_routes_by_queue_name` | |
| FR-E08 | not_started | | | needs M4 journal |

### Queues (§5.3)

| ID | Status | Implementation | Evidence | Notes |
| --- | --- | --- | --- | --- |
| FR-Q01 | complete (partial evidence) | declare + passive + counts | `topology_lapin.rs` roundtrip; counts are 0 until M3 | |
| FR-Q02 | complete (partial evidence) | server-generated names + per-channel last-queue shorthand | `server_generated_queue_name` lapin test; core unit tests | |
| FR-Q03 | complete (partial evidence) | equivalence 406 | `conflicting_redeclare_is_406` | |
| FR-Q04 | complete (partial evidence) | exclusivity incl. passive-inspect lockout (405) | `exclusive_queue_dies_with_connection` | publish-through-exchange gate lands with publish slice |
| FR-Q05 | not_started | | | auto-delete after last consumer needs M3 consumers |
| FR-Q06 | complete (partial evidence) | delete with if_unused/if_empty (real ready counts), purge ready-only with counts | store unit tests (`purge_counts_and_frees_budget`) | if_unused consumer check arrives with M3 consumers |
| FR-Q07 | complete (partial evidence) | x-queue-type absent or classic accepted | `unsupported_arguments_are_540` (rejects non-classic) | |
| FR-Q08 | complete (partial evidence) | quorum/stream + all other args 540 | same test (x-message-ttl 540) | |
| FR-Q09 | not_started | | | consumer-cancel negotiation lands with M3 |

### Messages (§5.4), delivery/settlement (§6), resource limits (§10)

| ID | Status | Implementation | Evidence | Notes |
| --- | --- | --- | --- | --- |
| FR-M01 | complete (partial evidence) | arbitrary bodies incl. zero bytes and 0xCE octets | `publish_route_get_roundtrip_with_properties` | zero-length via assembler unit tests |
| FR-M02 | complete (partial evidence) | properties preserved as encoded blobs through store | same test (typed headers/props roundtrip) | |
| FR-M03 | complete (partial evidence) | absent=transient, 1/2 valid, else 503 | `invalid_delivery_mode_is_rejected` | |
| FR-M04 | complete (partial evidence) | user_id mismatch → 403 | unit-level gate in `admit_properties` | dedicated lapin test pending (M7 auth suite) |
| FR-M05 | complete (partial evidence) | expiration property → 540 | `expiration_property_is_rejected` | |
| FR-M06 | in_progress | priority preserved as property; FIFO scheduling | property roundtrip test | priority-queue args still 540 ✓ |
| FR-PUB01 | complete (partial evidence) | publish+envelope on delivery | roundtrip tests | |
| FR-PUB02 | complete (partial evidence) | mandatory NO_ROUTE return + content | `mandatory_return_frame_level` | lapin surfaces returns only in confirm mode |
| FR-PUB03 | complete (partial evidence) | nonexistent exchange → 404 at publish | exchange gate in publish handler | |
| FR-PUB04 | complete (partial evidence) | immediate=true → 540 | gate in publish handler | |
| FR-C02 | complete (partial evidence) | basic.get with get-ok/get-empty | all get-based tests | |
| FR-C01 | complete (partial evidence) | consume/cancel/consume-ok/cancel-ok, server tags, exclusive consumers | `consume_lapin.rs`: push flow, `exclusive_consumer_conflict_is_403`, cancel in push test | |
| FR-C02 | complete (partial evidence) | basic.get with get-ok/get-empty | all get-based tests | |
| FR-C03 | complete (partial evidence) | manual-ack + no_ack modes (get and consume) | push + no_ack tests | |
| FR-C04 | complete (partial evidence) | channel-scoped monotonic tags across get+deliver | push test (distinct tags) | confirm numbering separate (M5) |
| FR-C05 | complete (partial evidence) | ack/reject/nack incl. multiple settlement | `settlement_lapin.rs`: discard/requeue reject, nack multiple | |
| FR-C08 | complete (partial evidence) | recover(requeue=true) → recover-ok + redelivery; requeue=false 540; recover-async 540 | `recover_requeues_channel_deliveries_to_consumer`, `recover_without_requeue_is_540` | |
| FR-C06 | complete (partial evidence) | requeue on channel close AND connection loss, redelivered hint | `unacked_redelivers_to_new_consumer_after_connection_loss` | |
| FR-C07 | complete (partial evidence) | per-consumer prefetch (global=false) + shared channel limit (global=true) | `push_delivery_with_prefetch_and_ack_flow`; registry unit tests incl. shared-limit gating | prefetch_size unrepresentable by codec 7.x (finding) |
| FR-C09 | complete (partial evidence) | round-robin fair scheduling | `round_robin_across_two_consumers`; registry unit test | |
| FR-Q05 | complete (partial evidence) | auto-delete after last consumer only if it had one | `auto_delete_queue_dies_after_last_consumer` | |
| FR-Q09 | complete (partial evidence) | cancel-notify on queue delete, capability-gated | `queue_delete_cancels_consumers` (lapin declares the capability) | not yet advertised in server capabilities |

### Security (§11), management (§12), CLI/config (§13)

All `not_started`. Pre-M7 binaries bind loopback only (PRD early safety constraint).

## Acceptance test ledger (§17.1)

| ID | Status | Evidence |
| --- | --- | --- |
| T01 | in_progress | `crates/rusty-mq/tests/handshake_lapin.rs` — lapin client handshake + channel open/close (1 of 5 clients) |
| T02 | in_progress | `rusty-mq-protocol` framing unit tests: split frames, merged chunks, bad frame-end, oversize |
| T03 | not_started | |
| T04 | in_progress | `topology_lapin.rs` roundtrip + `publish_lapin.rs` routing matrix (direct/topic/fanout/default, INV-05) |
| T05 | in_progress | `topology_lapin.rs`: equivalence, passive, generated names, exclusivity, reclaim |
| T06 | in_progress | `publish_route_get_roundtrip_with_properties`: bit-identical bodies, typed headers/props |
| T12 | in_progress | `mandatory_return_frame_level`: return-before-any-success, 312 NO_ROUTE |
| T07 | in_progress | `consume_lapin.rs`: consume/cancel/no_ack/exclusive-consumer flows |
| T08 | in_progress | `settlement_lapin.rs`: reject/nack/multiple/unknown-tag/recover flows |
| T09 | in_progress | prefetch gating (`push_delivery_with_prefetch_and_ack_flow`); concurrency stress pending |
| T11 | in_progress | `unacked_redelivers_to_new_consumer_after_connection_loss` (connection-loss requeue) |
| T10, T13–T30 | not_started | |

## Client-library findings (evidence-backed)

- **amq-protocol 7.x does not model `basic.qos`'s `prefetch_size` field**:
  the generated struct only carries `prefetch_count`/`global`, so a nonzero
  prefetch_size cannot be detected and rejected (FR §6.2 rule 5) at the
  method layer. Wire bytes still parse consistently; revisit during the
  codec-version convergence task.
- **lapin `basic_publish` resolves on write, not on broker outcome**: without
  confirm mode the returned future completes when frames are written, so
  server-side publish rejections (404 exchange, 503 delivery-mode, 540
  expiration) surface on the *next* channel operation or as a local
  "channel not open" error, depending on timing. Tests observe the closure
  plus the substantive outcome (message absent). Confirms rejections
  deterministically once confirm.select lands (M5).
- **Close-handshake interlude is required**: a server channel.close while
  client content frames are in flight must NOT be treated as a fatal
  unexpected-frame 505/504 — RabbitMQ drops in-flight frames for channels
  awaiting close-ok. Implemented as an `awaiting_close_ok` set; found via
  lapin treating the 504 as connection-fatal.
- **Dependency version split**: lapin 4.12 resolves `amq-protocol-types`
  10.6.3 while the broker uses `amq-protocol` 7.2.3 — two codec versions coexist
  in the test tree. They interoperate on the wire (integration tests pass),
  but the workspace should converge on one family (upgrade the server codec
  to 10.x or pin lapin accordingly) during the M2 publish slice. Tracked as
  the next run's housekeeping item.
- **lapin method signatures take `ShortString`/`FieldTable` by value** and
  `queue_unbind` has no options struct; error text renders codes as
  `NOT-FOUND`/`not_found`, not "not found" (assertion convention).

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
