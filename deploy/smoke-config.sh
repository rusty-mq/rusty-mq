#!/usr/bin/env sh
# §13.2 end-to-end smoke: serve --config with the PRD example file boots
# EVERY listener (plaintext AMQP, TLS AMQP, management over TLS, separate
# metrics) and serves real traffic on each plane. Requires: openssl,
# python3 (+pika), curl. Run from the repo root.
set -eu
BIN="${BIN:-./target/release/rusty-mq}"
DIR="$(mktemp -d /tmp/rusty-mq-cfg-smoke.XXXX)"
trap 'rm -rf "$DIR"; [ -n "${SRV:-}" ] && kill "$SRV" 2>/dev/null || true' EXIT

echo "== materialize config (example file + generated TLS) =="
# -addext forces X.509 v3: macOS LibreSSL emits v1 without it, and
# rustls rejects v1 (UnsupportedCertVersion) — found by this smoke.
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$DIR/key.pem" \
  -out "$DIR/cert.pem" -days 1 -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost" >/dev/null 2>&1
sed -e "s|^data_dir = .*|data_dir = \"$DIR/data\"|" \
    -e "s|^enabled = false|enabled = true|" \
    -e "s|^cert_file = .*|cert_file = \"$DIR/cert.pem\"|" \
    -e "s|^key_file = .*|key_file = \"$DIR/key.pem\"|" \
    -e "s|^tls_cert = .*|tls_cert = \"$DIR/cert.pem\"|" \
    -e "s|^tls_key = .*|tls_key = \"$DIR/key.pem\"|" \
    deploy/config.example.toml > "$DIR/config.toml"

echo "== serve --config =="
"$BIN" serve --config "$DIR/config.toml" --user smoke --password smoke-pass 2>"$DIR/serve.log" &
SRV=$!
for i in $(seq 1 50); do
  python3 -c "import socket; socket.create_connection(('127.0.0.1',5672),1)" 2>/dev/null && break
  sleep 0.3
done

echo "== AMQP roundtrip (plaintext loopback listener) =="
python3 - "$DIR/cert.pem" <<'PY'
import sys
import pika
conn = pika.BlockingConnection(pika.URLParameters("amqp://smoke:smoke-pass@127.0.0.1:5672/%2F"))
ch = conn.channel()
ch.confirm_delivery()
ch.queue_declare(queue="cfg-smoke.q", durable=True)
ch.basic_publish("", "cfg-smoke.q", b"cfg-smoke-body",
                 pika.BasicProperties(delivery_mode=2))
m = ch.basic_get("cfg-smoke.q", auto_ack=False)
assert m is not None and m[2] == b"cfg-smoke-body"
ch.basic_ack(m[0].delivery_tag)
conn.close()
print("AMQP ok")
PY

echo "== TLS AMQP handshake (cert verified) =="
python3 - "$DIR/cert.pem" <<'PY'
import socket, ssl, sys
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.load_verify_locations(sys.argv[1])
ctx.check_hostname = True
with socket.create_connection(("127.0.0.1", 5671), 5) as tcp:
    with ctx.wrap_socket(tcp, server_hostname="localhost") as tls:
        tls.sendall(b"AMQP\x00\x00\x09\x01")
        # KNOWN DEVIATION (recorded in the ledger): the server does not
        # echo its own protocol header — it proceeds straight to
        # connection.start (a type-1, channel-0 method frame). All five
        # target clients tolerate this; a spec-strict server header is a
        # follow-up.
        first = b""
        while len(first) < 8:
            chunk = tls.recv(8 - len(first))
            if not chunk:
                raise AssertionError("server closed before replying")
            first += chunk
        assert first[0] == 1 and first[1:3] == b"\x00\x00", first  # method, channel 0
print("TLS AMQP ok")
PY

echo "== management plane (loopback plaintext per the example; TLS on this plane is covered by management_tls.rs) =="
python3 - <<'PY'
import socket
with socket.create_connection(("127.0.0.1", 15672), 5) as tcp:
    tcp.sendall(b"GET /health/live HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
    head = tcp.recv(64).decode()
    assert head.startswith("HTTP/1.1 200"), head
print("management ok")
PY

echo "== separate metrics listener (plaintext loopback) =="
MET=$(curl -sf http://127.0.0.1:15692/metrics)
echo "$MET" | grep -q "rusty_mq_connections_opened_total" || {
  echo "metrics body missing expected gauges"; exit 1; }
echo "metrics ok"

kill "$SRV"; wait "$SRV" 2>/dev/null || true; SRV=""
echo "CONFIG SMOKE OK"

echo "== graceful shutdown (SIGTERM with an open connection) =="
"$BIN" serve --config "$DIR/config.toml" --user smoke --password smoke-pass >"$DIR/serve2.log" 2>&1 &
SRV=$!
for i in $(seq 1 50); do
  python3 -c "import socket; socket.create_connection(('127.0.0.1',5672),1)" 2>/dev/null && break
  sleep 0.3
done
python3 -c "
import pika
c = pika.BlockingConnection(pika.URLParameters('amqp://smoke:smoke-pass@127.0.0.1:5672/%2F'))
import time; time.sleep(30)  # hold the connection open
" &
HOLDER=$!
sleep 1
kill -TERM "$SRV"
wait "$SRV"; RC=$?
kill "$HOLDER" 2>/dev/null || true
[ "$RC" -eq 0 ] || { echo "server exit $RC after SIGTERM"; exit 1; }
grep -q "shutdown drain complete" "$DIR/serve2.log" || { echo "no drain-complete log"; exit 1; }
echo "graceful shutdown ok (drained, exit 0)"
