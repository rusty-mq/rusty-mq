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
| M3 — delivery state | complete (partial evidence) | Full §6 surface lapin-verified: consume/get/cancel, ack/reject/nack (single+multiple, discard vs requeue), recover(requeue=true), prefetch per-consumer + shared, round-robin, auto-delete, cancel-notify, requeue on channel/connection loss; T09 stress proves no-loss/no-duplicate delivery under 4×50 concurrent publishes with 3 competing consumers and prefetch credit held. channel.flow is a documented flow-ok no-op (RabbitMQ-compatible) |
| M4 — durable authority | in_progress | Journal engine + broker wiring complete: durable declares/deletes/binds/unbinds/purges commit before their replies; persistent publishes journal pre-assigned destination sequences under the store lock; terminal settlements journaled (INV-02); restart replays the journal into live topology+store with id stability and mint-bumping; `serve --data-dir` enables persistence (memory mode makes no persistence claim). lapin kill/restart round trip proves: topology + bindings survive, surviving set exactly {unacked-at-kill, ready, post-restart}, settled entries never resurrect. Remaining M4: delivery-attempt markers (§9.6, with M5 failpoints), redb projection |
| M5 — confirms and failure safety | in_progress | Confirms live (see FR-PUB05/06). §9.6 delivery-safety wired and lapin-verified: Delivered markers journaled BEFORE manual-ack exposure (redelivered hint survives kill/restart), no-ack terminal dequeues journaled BEFORE exposure (never redelivered), both on the consumer-job and basic.get paths; journal failpoints injectable at runtime (T13: injected fsync failure → no positive confirm, channel closes 506). Pending: disk-failure quiesce/alarms (FR-R04), redb projection, extended T14 kill matrix |
| M6 — storage lifecycle | in_progress | §9.9 snapshot/manifest/reclaim lifecycle live (see storage rows). §9.10 offline backup/verify/restore + LOCK landed: pid-based cross-process single-writer lock (stale LOCK tolerated; same-pid takeover is the documented abort-restart path), backup copies the recovery chain (LOCK/tmp excluded), verify replays the copy through the real recovery fold, restore refuses nonempty targets and verifies first. CLI: backup create/verify/restore. Remaining M6: redb projection, backup checksums file (T23 tail) |
| M7 — secure operations | in_progress | Auth (M7-1) live as below. M7-2: native HTTP API live — health/live+ready, /v1/status, /v1/capabilities (never advertising unsupported), queue rows with real ready counts + purge + delete (journaled), users CRUD + credential rotation + permissions CRUD over the durable store, all behind Basic-auth with role floors (Monitor reads / Admin mutations, FR-S04); Prometheus /metrics from a bounded-cardinality registry (counters wired at connect/publish/deliver/ack/nack/return/403 + ready/queues/journal-bytes gauges); serve --management-listen. M7-3: resource alarms live (FR-R02/R03/R04/R07) — memory alarm (store budget with hysteresis) stops message admissions (506, confirmed persistent never evicted), disk alarm (statvfs vs max(1 GiB, 10% volume), test-injectable) quiesces journal commits (§6.4: never a false confirm) and flips readiness while liveness stays healthy; blocked/unblocked fan out to capable clients via a bounded control channel (connection registry); evaluate+notify at publish-admission, settlement, qos; connection.blocked capability advertised. M7-4: TLS listener live (FR-S02) — connection layer generalized over any split AsyncRead/AsyncWrite stream; rustls acceptor from a PEM cert/key loaded once at startup (failures refuse startup); serve --tls-listen/--tls-cert/--tls-key (clap requires_all keeps the triple consistent); handshake proven over verified TLS (self-signed cert installed as the only root — no disabled verification) with channel.open-ok; plaintext AMQP to the TLS port gets a TLS fatal alert and close, never AMQP bytes. M7-5: connection registry surface + admin CLI live — GET /v1/connections (real usernames post-auth), POST /v1/connections/{id}/close (server-initiated connection.close 320 with close-handshake linger), GET /v1/permissions (full listing), principal deletion closes that user's live connections (FR-S08 complete), and the `rusty-mq admin` CLI (status/users/permissions/queues/connections; minimal HTTP/1.1 client with Basic auth, --json, RUSTY_MQ_ADMIN_PASSWORD; passwords never logged). Pending M7: auth throttling (anti-enumeration dummy-verify already in) |
| M8 — migration and interoperability | in_progress | T29 migration preflight live: offline RabbitMQ definitions inspection with findings grouped compatible/blocking/warning/unknown per §19.1 — queue-profile rules (durable+exclusive / durable+auto-delete blocking; shared-transient warning), frozen argument dispositions (TTL/DLX/priority/max-length/SAC/quorum blocking, x-queue-type=classic the one accepted form), exchange types, e2e bindings, policies + operator policies (ha-*/ttl/dlx/max-length/overflow/federation blocking), runtime/global parameters, default_queue_type, plugin inventory, permission-regex validation against the Rust subset (backreferences/lookaheads flagged), credential-reset warnings; unknown behavior-bearing settings block the ready gate by design. CLI `migration inspect --definitions --output` (exit 2 when not ready). M8-2: native definitions export/import live — `rusty-mq-definitions` format v1 (NOT RabbitMQ schema, never claimed to be): export covers durable queues, durable non-builtin exchanges, both-durable bindings (transient/exclusive/builtin excluded, verified); import validates the ENTIRE payload before any mutation (invalid payloads apply nothing), `?dry_run=true` reports would-be outcomes with zero mutation, real import is idempotent (equivalent → exists), conflicts are reported and never overwritten, durable declarations journaled with rollback on commit failure; GET/POST /v1/definitions (Monitor/Admin) + CLI `admin definitions export/import --dry-run`. M8-3: five-client matrix underway — REAL pika 1.4.4 and amqplib fixtures pass against the live broker (tests/interop/, driven by interop_matrix.rs): handshake+auth, durable topology, confirmed persistent publish, manual-ack get with typed properties (bit-identical binary bodies incl. 0xCE/UTF-8), nack-requeue redelivered hint, topic non-match discard, server-named exclusive queues, prefetch-2 exactly-once bursts; the harness then proves broker health with a lapin connection. Java/Go fixtures are CI jobs (go toolchain absent locally); missing runtimes SKIP (never counted as pass). Pending M8: Java/Go fixtures in CI, request/reply examples (T25) |
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
| FR-M04 | complete (partial evidence) | user_id mismatch → 403 | unit-level gate in `admit_properties` | dedicated lapin test pending |
| FR-S01 | complete (partial evidence) | Argon2id PHC verify; dev-mode flagged plaintext | `argon2_roundtrip` unit; `bootstrap_admin_works_and_wrong_password_fails` (incl. restart) | auth throttling pending (dummy-verify anti-enumeration in place) |
| FR-S03 | complete (partial evidence) | §11.2 checks on all listed operations + vhost gate | `permissions_enforce_the_11_2_table`; core auth unit tests | |
| FR-S08 | complete (partial evidence) | version token on every mutation; revocation immediate on the AMQP plane AND closes the user's live connections | `vhost_isolation_and_revocation`, `connections_list_operator_close_and_revocation_close` | |
| FR-M05 | complete (partial evidence) | expiration property → 540 | `expiration_property_is_rejected` | |
| FR-M06 | in_progress | priority preserved as property; FIFO scheduling | property roundtrip test | priority-queue args still 540 ✓ |
| FR-PUB01 | complete (partial evidence) | publish+envelope on delivery | roundtrip tests | |
| FR-PUB02 | complete (partial evidence) | mandatory NO_ROUTE return + content | `mandatory_return_frame_level` | lapin surfaces returns only in confirm mode |
| FR-PUB03 | complete (partial evidence) | nonexistent exchange → 404 at publish | exchange gate in publish handler | |
| FR-PUB04 | complete (partial evidence) | immediate=true → 540 | gate in publish handler | |
| FR-PUB05 | complete (partial evidence) | confirm.select/select-ok; per-channel sequences | `confirm_select_yields_positive_confirms` | |
| FR-PUB06 | complete (partial evidence) | positive confirm after admission; durable boundary ordered before it (INV-01) | `confirms_arrive_in_publish_order` + M4 restart test (journaled messages recovered after confirm path) | failpoint proof of no-confirm-before-sync is the T13 pending item |
| FR-PUB07 | in_progress | pending-confirm ceiling 10k/channel closes 506; outbound frames bounded by writer queue | unit-level ceiling only | slow-publisher interaction (blocked connections) pending |
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

### Storage (§9, M4)

| Item | Status | Implementation | Evidence |
| --- | --- | --- | --- |
| Format spec | complete | `docs/storage-format.md` (segments, records, fences, CRC, sync rules) | spec matches implementation |
| Record model/encoding | complete (partial evidence) | `record.rs` (topology/enqueue/settlement/purge, explicit LE encoding) | roundtrip + truncation + unknown-kind unit tests |
| Writer | complete (partial evidence) | `journal.rs` (LSN assignment, segment rolling + dir sync, commit() = fsync boundary, durable watermark) | roundtrip, rolling, reopen-after-restart tests |
| Recovery | complete (partial evidence) | `recover()` (chain validation, checksum verify, committed-only visibility, torn-tail discard) | torn-tail, checksum-corruption, chain-break, foreign-magic tests |
| Failpoints | complete (partial evidence) | injectable hooks (before-append/before-sync/after-sync) | `failpoint_before_sync_leaves_unfenced_tail` (commit() never returns Ok without sync) |
| Broker wiring | complete (partial evidence) | journal_commit gate in every durable path (506 on failure, live state intact or rolled back); `Broker::open_persistent` (recover-then-open); stable raw ids + mint bump after replay | `durability_lapin.rs`: restart round trip, fresh-dir isolation |
| Publish path | complete (partial evidence) | persistent+durable destinations: peek seqs under store lock → journal → live enqueue (§9.5 order) | same test (three messages survive with exact seqs) |
| Settlements | complete (partial evidence) | terminal ack/discard of persistent entries in durable queues journaled | INV-02 evidence: acked entry absent after restart |
| Purge | complete (partial evidence) | exact ready-set list journaled before live purge (§9.4) | store unit tests + seq identity via restart test |
| Delivery-attempt markers | complete (partial evidence) | Delivered record (0x23); journaled before manual-ack exposure on both consumer and get paths; replay sets the conservative hint | `manual_ack_delivery_comes_back_redelivered_after_kill`, `no_ack_delivery_settles_before_exposure` |
| redb projection | complete (partial evidence) | `projection.rs` (redb 4.3): queues/exchanges/bindings/entries/delivered tables, applied_lsn advanced atomically with index updates (Immediate durability), schema eager-init; startup load-or-rebuild with journal-suffix replay; broker advances it after every commit and drops a failed handle (never blocks the committed transaction) | 5 projection tests: fresh build, load-on-second-startup, trailing-suffix replay, truncated-index rebuild-not-trust, settlement reflected in loaded state |
| Offline backup/restore | complete (partial evidence) | `backup.rs`: create (live-writer refusal via LOCK liveness), verify (real-fold recovery of the copy), restore (empty-target enforced, verify-first); CLI wired | storage tests: roundtrip equivalence + append-after-restore, live-writer refusal, stale-lock tolerance, nonempty-target refusal, tampered-backup verification failure |
| Data-directory LOCK | in_progress | pid-based cross-process lock at writer open; foreign live pid refuses, own pid takes over (abort-restart path), stale LOCK tolerated | restart tests + live-writer backup refusal test | OS-level flock (kernel-released) deferred to multi-process hardening; PID-reuse limitation documented |
| Snapshots & compaction | complete (partial evidence) | `snapshot.rs` + `Broker::compact`; covered-LSN read after capture (≥ every event); reclaim only fully-covered non-tail segments + superseded snapshots | storage unit tests (roundtrip, corruption, reclaim, snapshot+suffix) + `compaction_reclaims_disk_and_state_survives_restart` |

### Security (§11), management (§12), CLI/config (§13)

| ID | Status | Implementation | Evidence | Notes |
| --- | --- | --- | --- | --- |
| FR-S04 | complete (partial evidence) | role floors on the HTTP surface (Basic auth → principal role; Monitor reads, Admin mutations) | `management_http.rs`: reads_require_monitor_role, lifecycle role checks | |
| §12.1 health | complete (partial evidence) | /health/live, /health/ready (honest single bit: alarms refine) | health_and_capabilities_are_public | |
| §12.1 status/capabilities | complete (partial evidence) | version/persistence/auth-version/LSN summary; feature matrix from implemented set | same + capabilities test (quorum_queues=false asserted) | |
| §12.1 queues | complete (partial evidence) | list with ready/consumer counts, purge (real counts), delete (journaled for durable) | queues_listing_and_purge_with_real_counts | opaque-id registry deferred; names as ids documented |
| §12.1 users/permissions | complete (partial evidence) | create/list/delete, credential rotation (Argon2id), permissions get/set/delete with regex validation at set time; hashes never in responses | user_lifecycle_credentials_and_permissions | |
| §12.2 metrics | in_progress | /metrics Prometheus text; 7 counters + 3 gauges, bounded labels by construction | metrics_text_endpoint; metrics unit test | queue-label metrics remain opt-out |
| CLI admin | in_progress | status/users/permissions/queues/connections/definitions via minimal HTTP/1.1 client; --json; env password | `cli_admin_client_round_trip`; definitions_import.rs | doctor pending |

Pre-M7 binaries bind loopback only (PRD early safety constraint).

## Acceptance test ledger (§17.1)

| ID | Status | Evidence |
| --- | --- | --- |
| T01 | in_progress | lapin + pika + amqplib (real fixtures, interop_matrix.rs): handshake, confirms, typed properties, prefetch bursts; Java/Go pending CI |
| T02 | in_progress | `rusty-mq-protocol` framing unit tests: split frames, merged chunks, bad frame-end, oversize |
| T03 | not_started | |
| T04 | in_progress | `topology_lapin.rs` roundtrip + `publish_lapin.rs` routing matrix (direct/topic/fanout/default, INV-05) |
| T05 | in_progress | `topology_lapin.rs`: equivalence, passive, generated names, exclusivity, reclaim |
| T06 | in_progress | `publish_route_get_roundtrip_with_properties`: bit-identical bodies, typed headers/props |
| T12 | complete (partial evidence) | `mandatory_return_frame_level` + `mandatory_unroutable_returns_then_confirms` (return serialized before the confirm, lapin-verified) |
| T07 | in_progress | `consume_lapin.rs`: consume/cancel/no_ack/exclusive-consumer flows |
| T08 | in_progress | `settlement_lapin.rs`: reject/nack/multiple/unknown-tag/recover flows |
| T09 | complete (partial evidence) | `stress_lapin.rs::concurrent_publish_consume_no_loss_no_duplicates` (200 msgs, 4 publishers, 3 consumers, prefetch 7: exact-once totals, zero duplicates, credit bound held) + `shared_prefetch_limits_channel_not_consumers` |
| T11 | in_progress | `unacked_redelivers_to_new_consumer_after_connection_loss` (connection-loss requeue) |
| T10, T15–T20, T22, T25–T28, T30 | not_started | |
| T29 | in_progress | migration_inspect.rs: clean export ready; every finding class detected; unknown-alone blocks readiness | definitions export/import live (M8-2) |
| T24 | in_progress | alarms_lapin.rs: memory-alarm admission stop + blocked notification, disk-alarm journal quiesce + readiness flip + recovery | slow-consumer mixed-connection posture pending |
| T21 | in_progress | `auth_lapin.rs`: two users, §11.2 refusals, vhost isolation, revocation-on-live-connection |
| T23 | complete (partial evidence) | storage backup tests: offline create + verify + restore into empty dir with state equivalence and post-restore appends; nonempty target refused; tampered backup fails verify |
| T13 | complete (partial evidence) | `no_positive_confirm_when_journal_fsync_fails`: before-sync failpoint → confirm future errors (channel close 506), failpoint provably fired; storage-level `failpoint_before_sync_leaves_unfenced_tail` proves commit() never returns Ok without sync

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
- **redb v4 iterator guards**: rows yield `(AccessGuard, AccessGuard)` —
  `.value()` each side; `range` takes tuple bounds over the full key type;
  tables don't exist until a write creates them (the projection
  eager-creates its schema at open so reads never hit "does not exist").
- **redb page checksums are lazy**: overwriting middle bytes of state.redb
  can go undetected on read; the corruption test uses truncation (detected
  at open). Full read-time integrity checking is redb's `check_integrity`
  — a candidate for the M9 doctor pass.
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

1. Journal group-commit batching (2 ms/1 MiB triggers) is not yet in the
   writer API — `commit()` fsyncs immediately per transaction (correct,
   unsophisticated; the persistent publish path holds the store lock
   across the commit, serializing publishers); batching + pipelining land
   with confirms (M5).
2. ~~Torn-tail physical truncation~~ — fixed in this slice: writer open
   truncates the tail segment to its last intact record boundary (test:
   `reopen_after_torn_tail_truncates_and_future_commits_recover`).

1. Pin the RabbitMQ reference release container digest in
   `compatibility/baseline.yaml` (requires first CI runner with container
   access; currently marked `pending_verification`).
2. License inventory covers direct dependencies; full transitive `cargo deny`
   integration lands with CI hardening (M9 gate).
3. Five-client matrix: only `lapin` wired so far; pika/amqplib/Java/Go
   fixtures land with `tests/interop/` in M8 (and progressively).
