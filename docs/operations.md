# rusty-mq operations guide

Operational reference for the current alpha: lifecycle, observability,
alarms, backup/restore, and incident handling. Everything here describes
implemented behavior; where a control is planned but absent, this document
says so.

## Lifecycle

### Starting

```sh
rusty-mq serve --config /etc/rusty-mq/config.toml
```

- Without `--data-dir`/`server.data_dir`: memory-backed development mode —
  accepted durable declarations make **no persistence claim**.
- With a data directory: the journal (and snapshot/manifest when present)
  is recovered *before* the listener accepts; recovery failure refuses
  startup explicitly (corrupt checksum, broken segment chain, missing
  snapshot — never a silent reinitialization).
- First run with a data directory bootstraps the flagged credentials
  (`--user`/`--password` or their defaults) as the durable admin — logged
  as a warning; rotate immediately via `admin users` + `credentials`.
- Optional listeners: `--management-listen` (HTTP API), `--tls-listen`
  with `--tls-cert/--tls-key`.

### Stopping

SIGTERM/SIGINT trigger shutdown. `server.shutdown_grace_seconds` bounds the
grace period. Confirmed-persistent messages already fsynced survive an
unclean kill (kill -9 is the tested failure model; power-loss is not —
see docs/storage-format.md).

One process per data directory; the directory LOCK refuses a second live
writer (cross-process pid probe; same-process takeover supports restart
after abort).

## Observability

### Metrics (`GET /metrics`, Prometheus text)

| Metric | Kind | Meaning |
| --- | --- | --- |
| `rusty_mq_connections_opened_total` | counter | Connections accepted since start |
| `rusty_mq_messages_published_total` | counter | Publish admissions completed |
| `rusty_mq_messages_delivered_total` | counter | Deliveries handed to writers |
| `rusty_mq_messages_acked_total` | counter | Terminal positive acks |
| `rusty_mq_messages_nacked_total` | counter | Negative settlements |
| `rusty_mq_messages_returned_total` | counter | Mandatory unroutable returns |
| `rusty_mq_auth_refusals_total` | counter | AMQP-plane 403 closes |
| `rusty_mq_ready_messages` | gauge | Ready entries across queues |
| `rusty_mq_queues` | gauge | Queue count |
| `rusty_mq_journal_bytes` | gauge | Live journal bytes on disk |

Labels are bounded by construction (no message ids, routing keys, or
consumer tags; per-queue labels remain off). A `rusty_mq_confirm_latency`
histogram is planned, not present.

### Health

- `GET /health/live` — process liveness, no auth.
- `GET /health/ready` — false under a disk alarm (durable admissions
  quiesced) while liveness stays healthy; that split is intentional.
- `GET /v1/status` (Monitor+) — version, persistence mode, auth-version,
  last committed fence LSN.

### Logging

`logging.format` = `json|text`, `logging.level` = standard levels; env
override `RUSTY_MQ__LOGGING__LEVEL`. Bodies, credentials, and tokens are
never logged.

## Resource alarms (§10)

| Alarm | Raises when | Effect |
| --- | --- | --- |
| memory | store bytes reach the budget | New message admissions refuse with 506; confirmed persistent messages are never evicted; clears at 50% of budget |
| disk | free space < max(1 GiB, 10% of volume) | Journal commits refuse (§6.4 — never a false confirm); readiness flips false; recovers when free space restores |

Clients that declared `connection.blocked` receive
`connection.blocked`/`unblocked` on transitions.

## Storage growth and compaction

The journal is append-only; reclamation keeps it bounded:

- **Automatic compaction** runs after commits once live journal bytes
  exceed `server.compact_threshold_bytes` (see `config validate` for the
  effective value). It snapshots durable state, atomically publishes the
  manifest, then deletes fully-covered segments.
- **Sealing**: the active segment is normally only reclaimable after a
  size-based rotation (default 256 MiB). Compaction therefore SEALS a
  fully-covered active segment and rolls the writer to a fresh one, so
  reclamation is not rotation-dependent (this closed an unbounded-growth
  bug found by the 24-hour churn soak — a no-rotation workload's journal
  grew ~9 KiB per cycle forever).
- **Expected steady state**: journal bytes oscillate in a band whose top
  is roughly one compaction threshold plus one segment of in-flight
  writes. Sustained monotonic growth ACROSS compactions is a bug, not a
  tuning problem — the nightly 500-cycle churn job and the 24-hour soak
  gate assert exactly this (`rusty_mq_journal_bytes` after a forced
  compaction stays in the KiB range on an idle system).
- **Disk alarms quiesce durable admissions** before growth can hit the
  floor (§6.4); free-space recovery re-enables commits automatically.
- `rusty-mq doctor --data-dir` reports the segment inventory and
  manifest/snapshot consistency read-only, for growth forensics.

## Backup & restore (offline, §9.10)

```sh
# broker MUST be stopped (live writers are refused)
rusty-mq backup-create  --data-dir /var/lib/rusty-mq --output /mnt/backup/rmq-$(date +%F)
rusty-mq backup-verify --input /mnt/backup/rmq-2026-10-03
rusty-mq backup-restore --input /mnt/backup/rmq-2026-10-03 --data-dir /var/lib/rusty-mq-new
```

- Verification replays the backup through the real recovery fold — a
  backup that cannot be recovered is not valid.
- Restore requires an empty target and never merges or overwrites.
- Backups contain credential material: protect at rest (file permissions,
  encryption at the storage layer).

## Incident quick reference

| Symptom | First checks |
| --- | --- |
| `health/ready` false | disk alarm (metrics: journal bytes vs disk free); free space or raise the floor after review |
| Publishers get 506 on publish | memory alarm (ready_messages gauge); drain consumers or raise the budget after review |
| Durable declares fail with 506 | disk alarm quiescing the journal |
| Startup refuses with checksum error | do not delete data; restore from the latest verified backup |
| Client hangs at connection | wrong vhost or credentials refused at open (see auth_refusals_total); TLS port probed with plaintext gets an alert+close |

## Known operational gaps (honest)

- No live config reload (restart applies changes).
- Connection-close endpoint exists; connection *listing* shows id+user
  only (no per-connection channel detail).
- Group commit (§9.5) batches concurrent durable commits (2 ms / 1 MiB
  triggers, fsync-inclusive boundary); single-publisher latency is still
  one fsync per transaction, so fsync-slow filesystems remain the
  persistent-throughput limiter (see benchmarks/README).
- The 24-hour soak evidence is from a single dev-class machine (APFS);
  §14.1-grade runs (dedicated runner, RSS/FD sampling) are pending.
