# rusty-mq benchmarks

Method and result schemas for the §14 performance harness. **Numbers in
`results/` from development machines are not release evidence** — §14
targets are defined against the reference runner (4 dedicated cores, 8 GiB
RAM, local NVMe, recorded filesystem), and full runs use 60 s warm-up, ≥5
minute measurement, three repetitions.

## Reference profile (`reference-14.1`)

One direct exchange, one durable queue, 4 publishers / 4 consumers,
1 KiB payloads, `delivery_mode=2`, publisher confirms, manual consumer
acks, prefetch 100, bounded confirm window 1,000 per publisher.

Run (short, dev-shaped):

```sh
RMQ_BENCH_SECS=30 cargo test -p rusty-mq --test bench_reference reference_profile -- --nocapture
```

Result JSON (`results/reference-<unix-seconds>.json`):

| Field | Meaning |
| --- | --- |
| `throughput_confirmed_per_sec` | Aggregate positively-confirmed persistent publishes per second over the measured window |
| `confirm_latency_us.p50/p99/p999/max` | Publisher confirm latency micros, measured from publish-write to confirm resolution |
| `consumed_total` | Consumer deliveries + acks completed |
| `mode` | Always `persistent+confirms` for this profile |

Integrity rules (§14): a run is invalid if any confirm resolves non-ack,
if `consumed_total` stalls, or if the harness disables fsync/group-commit
semantics to improve the headline rate.

## Churn soak (`T28` slice)

Queue churn (declare → 20 persistent publishes → drain with get+ack →
delete) under a minimal compaction threshold; asserts the live journal
stays under 1 MiB — compaction must reclaim through the churn.

```sh
RMQ_SOAK_CYCLES=100 cargo test -p rusty-mq --test bench_reference churn_soak -- --nocapture
```

The full 24-hour soak (publish/consume churn with RSS/FD/task/disk-growth
assertions) runs in nightly CI; see `.github/workflows/nightly.yaml`.

### 24-hour soak — executed (T28, gate 6 evidence)

One full run executed locally on this dev machine (release build,
RMQ_SOAK_CYCLES=80000 cap / RMQ_SOAK_MIN_SECS=86400 floor):

| Attempt | Cycles | Wall clock | Journal at checkpoints | Result |
| --- | --- | --- | --- | --- |
| 1 | 2,000 (fail-fast) | 56 min | 4.5→18.2 MB, monotonic growth | FAILED — caught the unbounded-journal bug (M9-13); fix: seal covered active segments |
| 2 | 35,187 | 24h 00m 02s (86,402 s; floor met) | 32 bytes at ALL 70 checkpoints | PASS — journal bounded end-to-end; RSS MB-flat (8.6→16.7 MB), FDs 18–19 |

Evidence: `results/soak-attempt2-FINAL.json` (+ the mid-flight 20 h
snapshot and attempt 1's failure record). Single-machine dev-shape
evidence on macOS/APFS — not a §14 target comparison; §14.1-grade runs
on dedicated hardware remain future work.

## Known local-machine results

| Run | Machine | Throughput | p99 confirm | Note |
| --- | --- | --- | --- | --- |
| reference (5 s smoke, pre-group-commit) | dev macOS/APFS, temp-dir journal | 39 confirmed/s | 77 s | fsync-per-commit dominates on APFS; dev-shape evidence only, not a target comparison |
| reference (8 s smoke, group commit + lock-free publish path) | dev macOS/APFS, temp-dir journal | 59 confirmed/s | 52 s | +63% throughput, -39% p99 with §9.5 batching engaged; still dev-shape evidence, not a target comparison |

### §14.1 protocol runs (60 s warmup + 300 s measure ×3) — executed

First full-protocol-shape runs on a quiet dev machine (macOS/APFS,
temp-dir journal; the 24 h soak had finished, so fsync bandwidth was
exclusive). NOT target comparisons — §14 targets assume dedicated
hardware; these numbers are honest dev-shape evidence:

| Run | Throughput | p50 confirm | p99 confirm | p999 confirm |
| --- | --- | --- | --- | --- |
| 1 | 55.3 conf/s | 34.6 s | 62.5 s | 65.9 s |
| 2 | 55.9 conf/s | 34.4 s | 63.5 s | 66.8 s |
| 3 | 57.3 conf/s | 37.5 s | 63.6 s | 66.8 s |

Tight clustering across runs (±2%); fsync-per-commit on APFS remains
the limiter (see operations.md). Raw JSONs in `results/`.

Raw results land in `results/` — publish them unmodified; §14 forbids
presenting adjusted numbers as measurements.
