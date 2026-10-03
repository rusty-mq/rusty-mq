# Migrating from RabbitMQ to rusty-mq

Scope and procedure for evaluating and performing a migration, per §19.
rusty-mq is **not** a drop-in RabbitMQ replacement: migration is a
preflight-gated, explicitly rehearsed cutover.

## 1. Preflight (offline, no network)

```sh
rusty-mq migration-inspect \
  --definitions rabbitmq-export.json \
  --output report.json        # exit 2 when not ready
```

The export comes from RabbitMQ's definitions export (management UI or
`rabbitmqctl export_definitions`). The report groups findings:

- **compatible** — maps directly (durable work queues, direct/fanout/topic
  exchanges, both-durable bindings, permission regexes valid in the Rust
  subset);
- **blocking** — unsupported behavior (TTL/DLX/priority/max-length/
  overflow/SAC/quorum queues, headers exchanges, e2e bindings, ha-* or
  federation policies, runtime parameters, non-classic default queue type,
  federation/shovel/STOMP/MQTT plugins, durable+exclusive and
  durable+auto-delete queue profiles);
- **warning** — needs a decision or translation (shared transient queues
  need the compat switch, invalid-regex permissions need translation,
  users need credential RESETS — password hashes are never imported);
- **unknown** — behavior-bearing settings with no mapping
  (unrecognized arguments/policy keys/plugins/global parameters).

**A single unknown finding blocks "ready"** — that is deliberate
(§19.1): unknown never silently becomes supported.

Definitions alone are never sufficient: an application inventory and
integration tests against rusty-mq are required before eligibility.

## 2. Preparing the target

1. Create topology via the native API/CLI or import the (preflight-clean)
   native definitions export:
   ```sh
   rusty-mq admin definitions import --file native-defs.json --dry-run
   rusty-mq admin definitions import --file native-defs.json
   ```
   Import is idempotent (equivalent resources report `exists`); conflicts
   are reported, never overwritten.
2. Create fresh credentials; grants with `admin permissions set`.
3. Verify TLS material and listener posture (docs/operations.md).

## 3. Cutover (§19.2 — summarized)

1. Resolve every preflight blocker; back up the source topology.
2. Rehearse the real application's supported profile against rusty-mq in
   a non-production environment.
3. Maintenance window: stop producers, drain source consumers/acks where
   practical.
4. If backlog transfer is necessary, use a separately audited bridge
   (consume manual-ack → publish persistent with confirm → ack source).
   A bridge crash can duplicate and reorder: consumers must be
   idempotent. **rusty-mq ships no built-in bridge in V1.**
5. Repoint applications, run canaries, ramp traffic.
6. Keep the old broker available through the rollback window; record the
   exact cutover point.

## 4. Rollback (§19.3)

- Before target traffic: endpoint/config restoration only.
- After target traffic: pause, reconcile target-only work and
  outstanding acks, transfer, then switch back. A DNS flip alone is not a
  data rollback.

## Limitations to state plainly

- No message-body migration tool; only topology/definitions move.
- RabbitMQ password hashes are never imported — every user resets
  credentials.
- At-least-once means duplicates are possible after any cutover/rollback
  step; applications must be idempotent (§7.1).
- Anything the preflight labels blocking/unknown needs a design decision
  before it can be reconsidered — the feature matrix
  (compatibility/features.yaml) is the reference, and it never lists
  `planned` as `supported`.
