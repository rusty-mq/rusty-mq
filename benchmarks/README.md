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

## Known local-machine results

| Run | Machine | Throughput | p99 confirm | Note |
| --- | --- | --- | --- | --- |
| reference (5 s smoke) | dev macOS/APFS, temp-dir journal | 39 confirmed/s | 77 s | fsync-per-commit dominates on APFS; dev-shape evidence only, not a target comparison |

Raw results land in `results/` — publish them unmodified; §14 forbids
presenting adjusted numbers as measurements.
