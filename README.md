# rusty-mq

A compact, self-hosted Rust message broker speaking AMQP 0-9-1 (with a tested
subset of RabbitMQ extensions) for durable work-queue, routing, and
request/reply workloads.

> **Status: early development (M0/M1).** rusty-mq is not production-ready and
> does not yet implement durable storage. See
> [docs/implementation-status.md](docs/implementation-status.md) for the
> requirement ledger and current milestone.

## What it is

- An independent Rust implementation of an AMQP 0-9-1 broker — not a fork or
  translation of RabbitMQ, and not a drop-in replacement for it.
- Single-node, durable-first design: one append-only segmented journal is the
  authoritative recovery root; the embedded `redb` index is a rebuildable
  projection.
- Native HTTP management API and CLI (V1); RabbitMQ HTTP API compatibility is
  a later, separately-tested surface.

## Compatibility posture

Compatibility is a demonstrated, published envelope — not a marketing claim.
We test against pinned versions of Python `pika`, Node.js `amqplib`, the Java
RabbitMQ client, Go `amqp091-go`, and Rust `lapin`. Unsupported features are
rejected explicitly, never silently accepted. See:

- [compatibility/baseline.yaml](compatibility/baseline.yaml) — frozen reference baseline
- [compatibility/features.yaml](compatibility/features.yaml) — feature/error matrix
- [docs/protocol-profile.md](docs/protocol-profile.md) — methods, limits, deviations

## Building

```sh
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
```

Toolchain is pinned in [rust-toolchain.toml](rust-toolchain.toml).

## Layout

| Path | Responsibility |
| --- | --- |
| `crates/rusty-mq` | Executable, service composition, CLI |
| `crates/rusty-mq-protocol` | AMQP framing, limits, connection/channel state machines |
| `crates/rusty-mq-core` | Typed commands, routing, queue behavior, admission, scheduling |
| `crates/rusty-mq-storage` | Journal, index, snapshots, recovery, backup |
| `crates/rusty-mq-management` | HTTP API, auth middleware, schemas |
| `crates/rusty-mq-testkit` | Deterministic clocks, failpoints, fixtures, process harness |
| `compatibility/` | Baseline, feature/error matrix, deviations |
| `tests/` | Interop (five-client) and fault scenarios |
| `docs/` | Requirements, protocol profile, storage format, ADRs, ops |

## License

Original rusty-mq code is dual-licensed under your choice of
[MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE). Third-party notices are
preserved in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md); the AMQP codec
(`amq-protocol`) is BSD-2-Clause and is reused with its notice intact.
