#!/usr/bin/env python3
"""rusty-mq interop fixture: Python pika (one of the five target clients).

Scenarios (§17.3 slice): handshake + auth, durable topology declare,
persistent publish with confirms, manual-ack consume, typed property
roundtrip, topic routing, server-named exclusive queue lifecycle.

Exit 0 = all assertions passed; prints PASS lines for the harness.
"""
import os
import sys

import pika

URL = os.environ["AMQP_URL"]
BODY = bytes([0, 1, 0xCE, 0x00, 255, 0xCE, 0xE2, 0x9C, 0x93])  # binary + UTF-8


def main() -> int:
    params = pika.URLParameters(URL)
    conn = pika.BlockingConnection(params)
    print("PASS connect handshake+auth")

    ch = conn.channel()
    ch.confirm_delivery()  # confirm.select

    ch.exchange_declare(
        exchange="pika.ex", exchange_type="topic", durable=True)
    ch.queue_declare(queue="pika.q", durable=True)
    ch.queue_bind(queue="pika.q", exchange="pika.ex", routing_key="a.*.c")
    print("PASS declare durable topology + bind")

    # Persistent publish with confirms: publisher must see the ack.
    props = pika.BasicProperties(
        content_type="application/json",
        delivery_mode=2,  # persistent
        priority=5,
        correlation_id="corr-42",
        headers={"x-num": 42, "x-str": "héllo", "x-flag": True},
    )
    ch.basic_publish(
        exchange="pika.ex",
        routing_key="a.b.c",
        body=BODY,
        properties=props,
        mandatory=False,
    )
    print("PASS confirmed persistent publish")

    # Manual-ack consume: payload bit-identical, properties preserved.
    method, header, body = ch.basic_get(queue="pika.q", auto_ack=False)
    assert body == BODY, f"body mismatch: {body!r}"
    assert header.content_type == "application/json"
    assert header.priority == 5
    assert header.correlation_id == "corr-42"
    assert header.headers["x-num"] == 42
    assert header.headers["x-str"] == "héllo"
    assert header.headers["x-flag"] is True
    assert method.exchange == "pika.ex"
    assert method.routing_key == "a.b.c"
    print("PASS manual-ack get with typed properties")

    # Requeue preserves relative position + redelivered hint.
    ch.basic_nack(method.delivery_tag, requeue=True)
    m2, _h, b2 = ch.basic_get(queue="pika.q", auto_ack=False)
    assert b2 == BODY
    assert m2.redelivered, "redelivered hint expected after requeue"
    ch.basic_ack(m2.delivery_tag)
    print("PASS nack-requeue redelivered + terminal ack")

    # Topic discrimination: non-matching key never arrives.
    ch.basic_publish(
        exchange="pika.ex", routing_key="a.b.d", body=b"no",
        properties=pika.BasicProperties(delivery_mode=2))
    got = ch.basic_get(queue="pika.q", auto_ack=True)
    method_got = got[0] if isinstance(got, tuple) else got
    assert method_got is None, f"a.b.d must not route through a.*.c, got {got!r}"
    print("PASS topic non-match discarded")

    # Server-named exclusive queue lifecycle.
    q = ch.queue_declare(queue="", exclusive=True, auto_delete=True)
    assert q.method.queue, "server must return the generated name"
    print("PASS server-named exclusive queue")

    conn.close()
    print("PASS connection close")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001 - fixture reports any failure
        print(f"FAIL {type(exc).__name__}: {exc}")
        sys.exit(1)
