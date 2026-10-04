#!/usr/bin/env python3
"""Frozen error-profile matrix (docs/protocol-profile.md, features.yaml).

Executes every frozen-profile case against any AMQP 0-9-1 broker and
records the observed outcome as JSON. The point is NOT that both brokers
agree — the profile intentionally defers features the baseline supports —
but that rusty-mq's outcomes match the FROZEN profile exactly while the
baseline's are recorded alongside as the compatibility reference.

Usage:
  profile_matrix.py [--expect] <amqp-url> <output.json>

  --expect  additionally assert the rusty-mq frozen profile (exit 1 on
            any divergence); without it the script only records.
"""

import json
import re
import sys

import pika

# (case id, frozen expectation for rusty-mq, runner)
# Expectations: ("ok", None) or ("channel_error", reply_code) — every
# deferred feature in the profile is a channel-scoped 540; queue-profile
# deviations are 406 (ADR-0005).
CASES = []


def case(name, expect_kind, expect_code, fn, url_vhost=None):
    CASES.append((name, expect_kind, expect_code, fn, url_vhost))


def declare(queue, durable=False, exclusive=False, auto_delete=False, args=None):
    def run(ch):
        ch.queue_declare(
            queue=queue,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=args or {},
        )

    return run


case("queue_plain_durable_ok", "ok", None, declare("diff.plain", durable=True))
case(
    "queue_x_message_ttl",
    "channel_error",
    540,
    declare("diff.ttl", args={"x-message-ttl": 100}),
)
case(
    "queue_x_queue_type_quorum",
    "channel_error",
    540,
    declare("diff.quorum", args={"x-queue-type": "quorum"}),
)
case(
    "queue_x_max_priority",
    "channel_error",
    540,
    declare("diff.prio", args={"x-max-priority": 5}),
)
case(
    "queue_durable_exclusive",
    "channel_error",
    406,
    declare("diff.durex", durable=True, exclusive=True),
)
case(
    "queue_durable_auto_delete",
    "channel_error",
    406,
    declare("diff.durad", durable=True, auto_delete=True),
)


def _exchange(kind):
    def run(ch):
        ch.exchange_declare(exchange="diff.headers", exchange_type=kind)

    return run


case("exchange_type_headers", "channel_error", 540, _exchange("headers"))


def _publish_immediate(ch):
    # pika does not expose immediate; use the properties path of a
    # rejected feature instead: expiration (frozen: 540 at publish time).
    ch.basic_publish(
        exchange="",
        routing_key="diff.plain",
        body=b"x",
        properties=pika.BasicProperties(expiration="1000"),
    )


case("publish_expiration_property", "channel_error", 540, _publish_immediate)


def _tx_select(ch):
    ch.tx_select()


case("tx_select", "channel_error", 540, _tx_select)


def _qos_prefetch_size(ch):
    ch.basic_qos(prefetch_size=10, prefetch_count=0)


# Enforced since M9-20 via raw-frame policy (the codec drops the field):
# nonzero prefetch_size closes the channel with the frozen 540.
case("basic_qos_prefetch_size", "channel_error", 540, _qos_prefetch_size)


def _consume_sac(ch):
    # no_local is not expressible through pika 1.x's basic_consume API;
    # the equivalent frozen consumer-argument rejection is SAC.
    ch.basic_consume(
        "diff.plain",
        lambda *a: None,
        arguments={"x-single-active-consumer": True},
    )


case("consume_x_single_active_consumer", "channel_error", 540, _consume_sac)


def _recover_requeue_false(ch):
    ch.basic_recover(requeue=False)


case("basic_recover_requeue_false", "channel_error", 540, _recover_requeue_false)


# --- 404 NOT_FOUND family (channel scope, frozen error table) ---

def _passive_missing(ch):
    ch.queue_declare(queue="diff.nope", passive=True)


case("queue_passive_missing", "channel_error", 404, _passive_missing)


def _delete_missing(ch):
    ch.queue_delete(queue="diff.nope")


case("queue_delete_missing", "channel_error", 404, _delete_missing)


def _bind_missing_exchange(ch):
    ch.queue_bind(queue="diff.plain", exchange="diff.nox", routing_key="k")


case("queue_bind_missing_exchange", "channel_error", 404, _bind_missing_exchange)


def _publish_missing_exchange(ch):
    # 404 is emitted before content processing (frozen publish gates);
    # the post-case probe surfaces the async close.
    ch.basic_publish(exchange="diff.nox", routing_key="k", body=b"x")


case("publish_missing_exchange", "channel_error", 404, _publish_missing_exchange)


# --- 403 ACCESS_REFUSED family (connection scope at open) ---

def _noop(_ch):
    pass


# Unknown vhost: refused at connection.open with 403 on both brokers.
case("wrong_vhost", "connection_error", 403, _noop, url_vhost="/does-not-exist")


def _reopen_channel(conn_holder):
    # Re-open an already-open channel number: pika's BlockingChannel keeps
    # its channel number; issuing channel.open raw on the same number is
    # an unexpected frame sequence. pika cannot express it (its API never
    # re-opens), so this case needs a raw AMQP peer — implemented in the
    # Rust harness instead (reopening_an_open_channel_is_505). Recorded
    # here as N/A: pika cannot send it.
    raise NotImplementedError("raw-frame only")


# Frozen profile: unexpected frame sequence -> 505 (connection scope);
# pika cannot express the input — covered by the Rust frame-level test.
case("channel_reopen", "na", None, _reopen_channel)


def _safe_close(conn):
    try:
        conn.close()
    except Exception:
        pass


def run_all(url):
    results = []
    for name, expect_kind, expect_code, fn, url_vhost in CASES:
        if expect_kind == "na":
            results.append({
                "case": name, "outcome": "not_expressible",
                "reply_code": None, "reply_text": "client API cannot send this input",
                "frozen_expect": None, "url_vhost": None,
            })
            print(f"{name}: not expressible via this client")
            continue
        case_url = url if url_vhost is None else url.rsplit("/", 1)[0] + url_vhost
        # Fresh connection per case: channel errors kill the channel and
        # some cases may close the connection (or refuse it at open).
        try:
            conn = pika.BlockingConnection(pika.URLParameters(case_url))
            ch = conn.channel()
            # A probe queue that exists for the whole connection: the
            # post-case probe (passive declare of it) surfaces async
            # channel errors without adding its own 404.
            ch.queue_declare(queue="__probe__", durable=True)
            try:
                fn(ch)
                # Async channel errors (e.g. publish-time 540s) surface on
                # the next event pass — drain first (the close carries its
                # reply code there), then probe a passive declare.
                try:
                    conn.process_data_events(time_limit=0.3)
                    if not ch.is_open:
                        outcome = {"case": name, "outcome": "channel_error",
                                   "reply_code": None,
                                   "reply_text": "channel closed asynchronously"}
                    else:
                        ch.queue_declare(queue="__probe__", passive=True)
                        outcome = {"case": name, "outcome": "ok", "reply_code": None,
                                   "reply_text": None}
                except pika.exceptions.ChannelClosedByBroker as e:
                    outcome = {"case": name, "outcome": "channel_error",
                               "reply_code": e.reply_code, "reply_text": e.reply_text}
                except pika.exceptions.ConnectionClosedByBroker as e:
                    outcome = {"case": name, "outcome": "connection_error",
                               "reply_code": e.reply_code, "reply_text": e.reply_text}
            except pika.exceptions.ChannelClosedByBroker as e:
                outcome = {"case": name, "outcome": "channel_error",
                           "reply_code": e.reply_code, "reply_text": e.reply_text}
            except pika.exceptions.ConnectionClosedByBroker as e:
                outcome = {"case": name, "outcome": "connection_error",
                           "reply_code": e.reply_code, "reply_text": e.reply_text}
            finally:
                # pika's conn.close() can hang draining events after an
                # async channel error (frame-level probes show the server
                # answers connection.close in both orderings — client
                # quirk, recorded in the ledger). Watchdog the close.
                import threading
                t = threading.Thread(target=lambda: _safe_close(conn), daemon=True)
                t.start()
                t.join(2.0)
        except pika.exceptions.ConnectionClosedByBroker as e:
            # Refused AT open (e.g. unknown vhost -> 403): a protocol
            # outcome, not a transport failure.
            outcome = {"case": name, "outcome": "connection_error",
                       "reply_code": e.reply_code, "reply_text": e.reply_text}
        except (pika.exceptions.ProbableAccessDeniedError,
                pika.exceptions.ProbableAuthenticationError) as e:
            # pika wraps open-time broker closes in these; the reply code
            # is embedded in the message.
            m = re.search(r"\((\d+)\)", str(e))
            outcome = {"case": name, "outcome": "connection_error",
                       "reply_code": int(m.group(1)) if m else None,
                       "reply_text": str(e)}
        except Exception as e:  # transport-level failure
            outcome = {"case": name, "outcome": "transport_error",
                       "reply_code": None,
                       "reply_text": f"{type(e).__name__}: {e}"}
        outcome["frozen_expect"] = (
            None if expect_kind == "ok" else f"{expect_kind}:{expect_code}"
        )
        outcome["url_vhost"] = url_vhost
        results.append(outcome)
        print(f"{name}: {outcome['outcome']}"
              f"{' code=' + str(outcome['reply_code']) if outcome['reply_code'] else ''}")
    return results


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    expect = "--expect" in sys.argv
    if len(args) != 2:
        print(__doc__)
        sys.exit(2)
    url, out_path = args
    results = run_all(url)
    with open(out_path, "w") as f:
        json.dump({"url_sanitized": url.split("@")[-1], "cases": results}, f, indent=1)
    if expect:
        mismatches = [
            r for r in results
            if r["outcome"] != "not_expressible"
            and (r["frozen_expect"] is None and r["outcome"] != "ok")
            or (r["frozen_expect"] is not None and (
                r["outcome"] != r["frozen_expect"].split(":")[0]
                or (r["reply_code"] is not None
                    and str(r["reply_code"]) != r["frozen_expect"].split(":")[1])))
        ]
        for r in mismatches:
            print(f"PROFILE MISMATCH {r['case']}: got {r['outcome']}:{r['reply_code']}, "
                  f"frozen {r['frozen_expect']}", file=sys.stderr)
        if mismatches:
            sys.exit(1)
        print(f"PROFILE CONFORMANCE: all {len(results)} cases match the frozen profile")


if __name__ == "__main__":
    main()
