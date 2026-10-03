#!/usr/bin/env python3
"""rusty-mq interop fixture: pika request/reply (T25, §2.3 workload 4).

Real temporary exclusive reply queue (server-named), reply_to +
correlation_id, replies through the default exchange; the exclusive queue
dies with its connection.
"""
import os
import sys
import threading
import time

import pika

URL = os.environ["AMQP_URL"]


def main() -> int:
    server = pika.BlockingConnection(pika.URLParameters(URL))
    sch = server.channel()
    sch.confirm_delivery()
    sch.queue_declare(queue="pika.rpc", durable=True)
    print("PASS server topology")

    def handler(ch, method, props, body):
        ch.basic_publish(
            exchange="",  # default exchange: reply_to is the queue name
            routing_key=props.reply_to,
            body=b"reply:" + body,
            properties=pika.BasicProperties(correlation_id=props.correlation_id),
        )
        ch.basic_ack(method.delivery_tag)

    sch.basic_consume(queue="pika.rpc", on_message_callback=handler)
    t = threading.Thread(target=sch.start_consuming, daemon=True)
    t.start()

    client = pika.BlockingConnection(pika.URLParameters(URL))
    cch = client.channel()
    cch.confirm_delivery()
    result = cch.queue_declare(queue="", exclusive=True)
    reply_q = result.method.queue
    assert reply_q, "server must name the reply queue"
    print("PASS server-named exclusive reply queue")

    for i in range(3):
        corr = f"corr-{i}"
        cch.basic_publish(
            exchange="",
            routing_key="pika.rpc",
            body=f"ping-{i}".encode(),
            properties=pika.BasicProperties(
                reply_to=reply_q,
                correlation_id=corr,
                delivery_mode=2,
            ),
        )
        method = None
        for _ in range(200):  # basic_get pumps connection I/O; retry briefly
            method, props, body = cch.basic_get(queue=reply_q, auto_ack=False)
            if method is not None:
                break
            time.sleep(0.01)
        assert method is not None, f"reply {i} missing"
        assert body == f"reply:ping-{i}".encode(), f"payload {body!r}"
        assert props.correlation_id == corr, f"correlation {props.correlation_id!r}"
        cch.basic_ack(method.delivery_tag)
    print("PASS 3 correlated request/reply roundtrips")

    client.close()  # exclusive reply queue dies here
    time.sleep(0.2)

    # The reply queue is gone (passive declare 404s); the rpc queue lives.
    probe = pika.BlockingConnection(pika.URLParameters(URL))
    pch = probe.channel()
    try:
        pch.queue_declare(queue=reply_q, passive=True)
        raise AssertionError("exclusive reply queue must die with its connection")
    except pika.exceptions.ChannelClosedByBroker as exc:
        assert exc.reply_code == 404, f"expected 404, got {exc.reply_code}"
    pch2 = probe.channel()
    pch2.queue_declare(queue="pika.rpc", passive=True)
    print("PASS exclusive reply queue lifecycle")

    server.add_callback_threadsafe(sch.stop_consuming)
    t.join(timeout=5)
    server.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL {type(exc).__name__}: {exc}")
        sys.exit(1)
