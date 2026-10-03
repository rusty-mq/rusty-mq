# rusty-mq v0.1.0-alpha — release qualification report

Per §17.4 this report assesses the release gates against actual evidence.
**Conclusion: this is a PRERELEASE.** Gate 9 (maintainer review of storage
correctness and this report) has not occurred, and several gates carry
explicit gaps below. Until the gates close, distribute only clearly
labeled prereleases.

Evidence lives in the repository: the requirement ledger
(docs/implementation-status.md) maps every claim to a test path; run
`cargo test --workspace` to reproduce (181 tests, clippy `-D warnings`
clean at commit `58020a2`).

## Gate assessment

| # | Gate (§17.4) | Status | Evidence / gaps |
| --- | --- | --- | --- |
| 1 | Every P0 requirement has executable passing evidence; no hidden TODO/skips | **partial** | Ledger maps FR rows to tests; env-gated bench/soak and interop skips are explicit, never counted as passes. Remaining local gap: channel-flow beyond the documented no-op. Closed this cycle: doctor (M9-5), auth throttling (M9-6), definitions users/permissions export+import (M9-6) |
| 2 | All invariants covered + five-client matrix | **pass (tested scope)** | INV-01..INV-05 have kill/restart, failpoint, and property evidence; INV-06..INV-12 covered by the recovery/compaction suites. Client matrix is five-five: **lapin, pika 1.4.4, amqplib, Java amqp-client 5.21.0, Go amqp091-go v1.10.0** all pass against the live broker (interop_matrix.rs; roundtrip, confirms, typed properties, T25 RPC). Local evidence on this dev machine (staged JDK 21 + Go 1.23.4); the interop CI job (fails on any skip) has not run yet — first run awaits push access |
| 3 | Fault tests: no confirmed-message loss in the single-node model | **pass (tested scope)** | T13 injected-fsync, T14 kill/restart suites, §9.6 delivery-safety boundaries; kill -9 model, power loss explicitly out of scope |
| 4 | Compaction and offline restore proven | **pass (tested scope)** | segment reclamation, manifest publication order, crash-at-boundary via torn-tail suite, backup verify/restore roundtrip with post-restore writes; crash-at-every-checkpoint-step matrix is nightly CI |
| 5 | Security/isolation/malformed-input/resource-limit tests | **pass (tested scope)** | Argon2id + dummy-verify, §11.2 table, vhost isolation, revocation closes live connections, TLS verified-handshake + plaintext-refused, T26 adversarial suites, alarms quiesce admissions |
| 6 | 24-hour soak + reproducible benchmark report | **NOT MET** | Benchmark harness + churn soak exist and run (nightly CI); the 24-hour soak has not executed. One dev-machine benchmark recorded honestly (macOS/APFS, 39 confirmed/s, p99 77 s) — explicitly not a §14 target comparison |
| 7 | License/dependency checks, SBOM, notices, clean release builds | **partial** | THIRD_PARTY_NOTICES.md, gen-sbom.sh (290-row SBOM), release workflow with checksums + SBOM attach; cargo-deny gate live and green in CI (licenses+advisories+bans+sources; the sole advisory RUSTSEC-2025-0134 fixed by removing rustls-pemfile in favor of rustls-pki-types); deploy/smoke.sh release smoke passes against the release binary. A clean tagged build has not executed (CI run pending) |
| 8 | Documentation lists unsupported features + measured performance accurately | **pass (current docs)** | features.yaml is machine-checked against implementation; docs/operations.md, migration.md, storage-format.md, protocol-profile.md describe implemented behavior only; benchmarks/README labels local numbers as dev-shape |
| 9 | Maintainer review of storage correctness + this report | **NOT MET** | Requires the owner's review; this report is the input |

## Notable engineering findings fixed during qualification

1. **Load-fatal outbound drop** (M1 posture): try-send-and-drop reset all
   publisher connections under sustained load; replaced with
   bounded-await backpressure (found by the §14 benchmark).
2. **Journaled identity drift**: a consuming sequence peek made acked
   messages resurrect after restart (found by the M4 round-trip).
3. **Torn-tail stranding**: writer appends after a torn tail would strand
   future commits; open now truncates to the intact boundary.
4. **ABBA deadlock** in compaction capture vs publish-path lock order;
   resolved with try_lock deferral.
5. Client quirks pinned as fixtures: lapin's open-wait connect hang,
   amqplib's durable-default and async 404 events, pika's 3-tuple get.

## Known deviations (intentional, recorded)

- V1 queue-profile restrictions; strict unknown-argument rejection;
  channel.flow as a documented no-op; shared transient queues off by
  default — all in compatibility/features.yaml with exact rejections.

## Recommendation

Treat v0.1.0-alpha as a **developer/evaluation prerelease**: memory-backed
operation is fully usable; persistent operation is implemented and
fault-tested but awaits the maintainer storage review (gate 9), the
24-hour soak (gate 6), and a clean tagged CI build (gate 7) before any
production claim. The five-client interop matrix (gate 2) passes with
local evidence; its CI job's first run rides along with gate 7.
