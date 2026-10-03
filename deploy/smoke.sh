#!/usr/bin/env sh
# T30 release smoke: the versioned executable starts, persists data,
# restarts, and shuts down. Run from the repo root; writes to a temp dir.
set -eu
BIN="${BIN:-./target/release/rusty-mq}"
DIR="$(mktemp -d /tmp/rusty-mq-smoke.XXXX)"
PORT="${PORT:-15699}"
trap 'rm -rf "$DIR"' EXIT

echo "== version =="
"$BIN" version

echo "== start + seed (durable queue + persistent message) =="
"$BIN" serve --listen "127.0.0.1:$PORT" --data-dir "$DIR" &
SRV=$!
sleep 1
python3 - "$PORT" <<'PY'
import sys, pika
port = sys.argv[1]
conn = pika.BlockingConnection(pika.URLParameters(f"amqp://guest:guest@127.0.0.1:{port}/%2F"))
ch = conn.channel()
ch.confirm_delivery()
ch.queue_declare(queue="smoke.q", durable=True)
ch.basic_publish("", "smoke.q", b"smoke-body",
                 pika.BasicProperties(delivery_mode=2))
print("seeded")
conn.close()
PY

echo "== kill -9 (crash model) =="
kill -9 $SRV 2>/dev/null || true
sleep 0.5

echo "== restart + verify recovery =="
"$BIN" serve --listen "127.0.0.1:$PORT" --data-dir "$DIR" &
SRV2=$!
sleep 1
python3 - "$PORT" <<'PY'
import sys, pika
port = sys.argv[1]
conn = pika.BlockingConnection(pika.URLParameters(f"amqp://guest:guest@127.0.0.1:{port}/%2F"))
ch = conn.channel()
m = ch.basic_get("smoke.q", auto_ack=True)
assert m[2] == b"smoke-body", f"body mismatch: {m[2]!r}"
print("recovered after kill -9")
conn.close()
PY

echo "== doctor on the stopped dir =="
kill $SRV2 2>/dev/null || true
wait $SRV2 2>/dev/null || true
"$BIN" doctor --data-dir "$DIR"

echo "== backup / verify / restore =="
"$BIN" backup-create --data-dir "$DIR" --output "$DIR.bak"
"$BIN" backup-verify --input "$DIR.bak"
"$BIN" backup-restore --input "$DIR.bak" --data-dir "$DIR.restored"

echo "SMOKE OK"
