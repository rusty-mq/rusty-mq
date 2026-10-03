# Five-client interop fixtures

`crates/rusty-mq/tests/interop_matrix.rs` runs these fixtures against a live
in-process broker. lapin is covered by the Rust suites; pika (Python),
amqplib (Node), amqp-client (Java), and amqp091-go (Go) run from here when
their runtimes are available. Missing runtimes **skip** — a skip is never
counted as a pass.

Python and Node need no staging beyond `python3` / `node` on `PATH`
(amqplib resolves via `NODE_PATH`, default `/tmp/rmq-interop/node_modules`,
override `RMQ_NODE_MODULES`).

## Java (amqp-client 5.21.0)

```sh
mkdir -p /tmp/rmq-interop && cd /tmp/rmq-interop
curl -sfLO https://repo1.maven.org/maven2/com/rabbitmq/amqp-client/5.21.0/amqp-client-5.21.0.jar
export RMQ_JAVA_HOME=/path/to/jdk          # contains bin/javac, bin/java
export RMQ_AMQP_CLIENT_JAR=/tmp/rmq-interop/amqp-client-5.21.0.jar  # default
```

Without `RMQ_JAVA_HOME` the harness falls back to `javac`/`java` on `PATH`.

## Go (amqp091-go v1.10.0)

```sh
mkdir -p /tmp/rmq-interop/go && cd /tmp/rmq-interop/go
go mod init fixture
go get github.com/rabbitmq/amqp091-go@v1.10.0
export RMQ_GO_BIN=$(command -v go)         # default: "go"
export RMQ_GO_MOD=/tmp/rmq-interop/go      # default
```

The module dir must already contain the dependency — the harness never
fetches modules itself (no network in tests).

## Run

```sh
cargo test -p rusty-mq --test interop_matrix -- --nocapture
```

Each fixture prints `PASS <assertion>` lines; the harness asserts on the
expected lines and then opens a lapin connection to prove the broker
survived all five client families.

The `.github/workflows/interop.yaml` job stages all four runtimes and runs
the full matrix on every push.
