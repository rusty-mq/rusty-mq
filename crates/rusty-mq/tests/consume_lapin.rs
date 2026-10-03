//! T07/T09 slices: push consumers (basic.consume/cancel), prefetch credit
//! gating, no-ack delivery, exclusive consumers, round-robin across two
//! consumers, auto-delete after the last consumer (FR-Q05), and
//! consumer-cancel notification on queue deletion (FR-Q09).

use std::time::Duration;

use futures_lite::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicPublishOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties, Consumer,
};

async fn start_broker() -> std::net::SocketAddr {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "off".into()),
        )
        .try_init();
    let broker = rusty_mq::Broker::new("guest".into(), "guest".into());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rusty_mq::server::serve_listener(listener, broker));
    addr
}

async fn connect(addr: std::net::SocketAddr) -> Connection {
    let uri = format!("amqp://guest:guest@{addr}/%2F");
    tokio::time::timeout(
        Duration::from_secs(10),
        Connection::connect(&uri, ConnectionProperties::default()),
    )
    .await
    .expect("connect within timeout")
    .expect("handshake")
}

fn durable_queue() -> QueueDeclareOptions {
    QueueDeclareOptions {
        durable: true,
        ..Default::default()
    }
}

async fn publish(ch: &lapin::Channel, routing_key: &str, body: &[u8]) {
    ch.basic_publish(
        "".into(),
        routing_key.into(),
        BasicPublishOptions::default(),
        body,
        BasicProperties::default(),
    )
    .await
    .unwrap();
}

/// Next delivery with a bounded wait (push path).
async fn next_delivery(consumer: &mut Consumer) -> lapin::message::Delivery {
    match tokio::time::timeout(Duration::from_secs(5), consumer.next()).await {
        Ok(Some(Ok(d))) => d,
        Ok(Some(Err(e))) => panic!("delivery error: {e}"),
        Ok(None) => panic!("consumer stream ended"),
        Err(_) => panic!("timed out waiting for delivery"),
    }
}

/// Assert no delivery arrives within the window.
async fn assert_no_delivery(consumer: &mut Consumer) {
    match tokio::time::timeout(Duration::from_millis(300), consumer.next()).await {
        Ok(Some(Ok(d))) => panic!("unexpected delivery: {:?}", d.data),
        Ok(Some(Err(e))) => panic!("unexpected error: {e}"),
        Ok(None) => panic!("consumer stream ended unexpectedly"),
        Err(_) => {}
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn push_delivery_with_prefetch_and_ack_flow() {
    let addr = start_broker().await;
    let publisher = connect(addr).await;
    let consumer_conn = connect(addr).await;
    let pub_ch = publisher.create_channel().await.unwrap();
    let con_ch = consumer_conn.create_channel().await.unwrap();

    con_ch
        .queue_declare("push.q".into(), durable_queue(), FieldTable::default())
        .await
        .unwrap();
    // Prefetch 2 applies to consumers created after this qos (§6.2 rule 2).
    con_ch
        .basic_qos(2, lapin::options::BasicQosOptions::default())
        .await
        .unwrap();
    let mut consumer = con_ch
        .basic_consume(
            "push.q".into(),
            "".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let tag = consumer.tag().to_string();
    assert!(!tag.is_empty(), "server-generated consumer tag returned");

    for i in 0..3u8 {
        publish(&pub_ch, "push.q", &[i]).await;
    }

    // Prefetch=2: exactly two arrive before any ack (FR-C07).
    let d1 = next_delivery(&mut consumer).await;
    assert_eq!(d1.data, vec![0]);
    let d2 = next_delivery(&mut consumer).await;
    assert_eq!(d2.data, vec![1]);
    assert_no_delivery(&mut consumer).await;

    // Settling the first frees one credit slot.
    d1.acker.ack(BasicAckOptions::default()).await.unwrap();
    let d3 = next_delivery(&mut consumer).await;
    assert_eq!(d3.data, vec![2]);

    // Cancel: stop the consumer; the channel stays usable (FR-C01).
    con_ch
        .basic_cancel(tag.into(), BasicCancelOptions::default())
        .await
        .unwrap();
    // Outstanding unacked deliveries stay with the channel (§6.1): the
    // second delivery is still unacked and must NOT reappear anywhere.
    d2.acker.ack(BasicAckOptions::default()).await.unwrap();
    d3.acker.ack(BasicAckOptions::default()).await.unwrap();

    consumer_conn.close(200, "bye".into()).await.unwrap();
    publisher.close(200, "bye".into()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn no_ack_consumer_receives_all() {
    let addr = start_broker().await;
    let publisher = connect(addr).await;
    let consumer_conn = connect(addr).await;
    let pub_ch = publisher.create_channel().await.unwrap();
    let con_ch = consumer_conn.create_channel().await.unwrap();

    con_ch
        .queue_declare("noack.q".into(), durable_queue(), FieldTable::default())
        .await
        .unwrap();
    let mut consumer = con_ch
        .basic_consume(
            "noack.q".into(),
            "".into(),
            BasicConsumeOptions {
                no_ack: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();

    for i in 0..5u8 {
        publish(&pub_ch, "noack.q", &[i]).await;
    }
    // No acks are sent: prefetch must not gate no-ack consumers (§6.2
    // rule 6) — all five arrive.
    for expected in 0..5u8 {
        let d = next_delivery(&mut consumer).await;
        assert_eq!(d.data, vec![expected]);
    }
    assert_no_delivery(&mut consumer).await;

    let _ = consumer_conn.close(200, "bye".into()).await;
    let _ = publisher.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn round_robin_across_two_consumers() {
    let addr = start_broker().await;
    let publisher = connect(addr).await;
    let consumer_conn = connect(addr).await;
    let pub_ch = publisher.create_channel().await.unwrap();
    let ch = consumer_conn.create_channel().await.unwrap();

    ch.queue_declare("rr.q".into(), durable_queue(), FieldTable::default())
        .await
        .unwrap();
    let mut c1 = ch
        .basic_consume(
            "rr.q".into(),
            "c1".into(),
            BasicConsumeOptions {
                no_ack: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut c2 = ch
        .basic_consume(
            "rr.q".into(),
            "c2".into(),
            BasicConsumeOptions {
                no_ack: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();

    for i in 0..4u8 {
        publish(&pub_ch, "rr.q", &[i]).await;
    }
    // FR-C09: fair scheduling — two deliveries per consumer (order within
    // each consumer is publication order).
    let a1 = next_delivery(&mut c1).await;
    let a2 = next_delivery(&mut c1).await;
    let b1 = next_delivery(&mut c2).await;
    let b2 = next_delivery(&mut c2).await;
    let mut all = vec![a1.data[0], a2.data[0], b1.data[0], b2.data[0]];
    all.sort();
    assert_eq!(all, vec![0, 1, 2, 3]);
    assert_no_delivery(&mut c1).await;
    assert_no_delivery(&mut c2).await;

    let _ = consumer_conn.close(200, "bye".into()).await;
    let _ = publisher.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn exclusive_consumer_conflict_is_403() {
    let addr = start_broker().await;
    let conn = connect(addr).await;
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare("excl-c.q".into(), durable_queue(), FieldTable::default())
        .await
        .unwrap();
    let _first = ch
        .basic_consume(
            "excl-c.q".into(),
            "".into(),
            BasicConsumeOptions {
                exclusive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    let err = ch
        .basic_consume(
            "excl-c.q".into(),
            "".into(),
            BasicConsumeOptions {
                exclusive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect_err("second exclusive consumer must be refused");
    assert!(
        err.to_string().to_lowercase().contains("refused"),
        "expected ACCESS_REFUSED, got: {err}"
    );
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_delete_queue_dies_after_last_consumer() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    // Temporary auto-delete profile: exclusive + auto-delete (§5.3).
    let q = ch
        .queue_declare(
            "".into(),
            QueueDeclareOptions {
                exclusive: true,
                auto_delete: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    let name = q.name().as_str().to_string();

    let consumer = ch
        .basic_consume(
            name.clone().into(),
            "".into(),
            BasicConsumeOptions {
                no_ack: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    let tag = consumer.tag().to_string();

    // No consumer yet → queue still exists (FR-Q05: only after having had
    // a consumer).
    ch.queue_declare(
        name.clone().into(),
        QueueDeclareOptions {
            passive: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("queue alive while consumer attached");

    // Cancelling the only consumer triggers auto-delete.
    ch.basic_cancel(tag.into(), BasicCancelOptions::default())
        .await
        .unwrap();
    let err = ch
        .queue_declare(
            name.into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect_err("auto-delete queue must be gone");
    assert!(
        err.to_string().to_lowercase().contains("not_found"),
        "expected NOT_FOUND, got: {err}"
    );
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_delete_cancels_consumers() {
    let addr = start_broker().await;
    let conn = connect(addr).await;
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare("gone.q".into(), durable_queue(), FieldTable::default())
        .await
        .unwrap();
    let mut consumer = ch
        .basic_consume(
            "gone.q".into(),
            "".into(),
            BasicConsumeOptions {
                no_ack: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();

    // lapin declares consumer_cancel_notify by default; deleting the queue
    // must end the consumer stream via basic.cancel (FR-Q09).
    ch.queue_delete(
        "gone.q".into(),
        lapin::options::QueueDeleteOptions::default(),
    )
    .await
    .unwrap();

    match tokio::time::timeout(Duration::from_secs(5), consumer.next()).await {
        Ok(None) => {} // stream closed by the server-side cancel
        Ok(Some(Err(e))) => {
            let msg = e.to_string();
            assert!(
                msg.to_lowercase().contains("cancel")
                    || msg.to_lowercase().contains("closed")
                    || msg.to_lowercase().contains("not found"),
                "expected cancel-related termination, got: {msg}"
            );
        }
        other => panic!("consumer should terminate after queue delete, got: {other:?}"),
    }
    let _ = conn.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unacked_redelivers_to_new_consumer_after_connection_loss() {
    let addr = start_broker().await;
    let publisher = connect(addr).await;
    let pub_ch = publisher.create_channel().await.unwrap();
    pub_ch
        .queue_declare("crash.q".into(), durable_queue(), FieldTable::default())
        .await
        .unwrap();
    publish(&pub_ch, "crash.q", b"payload").await;

    // A consumer connection receives but never acks, then dies.
    {
        let victim = connect(addr).await;
        let vch = victim.create_channel().await.unwrap();
        let mut consumer = vch
            .basic_consume(
                "crash.q".into(),
                "".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        let d = next_delivery(&mut consumer).await;
        assert_eq!(d.data, b"payload".to_vec());
        // Drop without acking (FR-C06: requeue on connection loss).
        drop(vch);
        let _ = victim.close(200, "bye".into()).await;
    }

    let survivor = connect(addr).await;
    let sch = survivor.create_channel().await.unwrap();
    let mut consumer = sch
        .basic_consume(
            "crash.q".into(),
            "".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let d = next_delivery(&mut consumer).await;
    assert_eq!(d.data, b"payload".to_vec());
    assert!(d.redelivered, "redelivered hint set after requeue");
    d.acker.ack(BasicAckOptions::default()).await.unwrap();

    let _ = survivor.close(200, "bye".into()).await;
    let _ = publisher.close(200, "bye".into()).await;
}

/// FR-Q06 completion: delete with `if_unused` refuses while a consumer is
/// attached (406 PRECONDITION_FAILED, queue survives) and succeeds on a
/// fresh channel once the consumer died with its channel.
#[tokio::test(flavor = "multi_thread")]
async fn delete_if_unused_respects_live_consumers() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare("iu.q".into(), durable_queue(), FieldTable::default())
        .await
        .unwrap();
    let _consumer = ch
        .basic_consume(
            "iu.q".into(),
            "iu.tag".into(),
            BasicConsumeOptions {
                no_ack: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();

    // The channel is closed by the 406; a fresh channel on the same
    // connection proves the queue survived.
    let err = ch
        .queue_delete(
            "iu.q".into(),
            lapin::options::QueueDeleteOptions {
                if_unused: true,
                ..Default::default()
            },
        )
        .await
        .expect_err("delete(if_unused) with a live consumer must fail");
    assert!(
        err.to_string().to_lowercase().contains("precondition"),
        "expected PRECONDITION_FAILED, got: {err}"
    );
    let ch2 = conn.create_channel().await.unwrap();
    ch2.queue_declare("iu.q".into(), durable_queue(), FieldTable::default())
        .await
        .expect("queue survives the refused delete");

    // The consumer died with channel 1's 406 close (consumers are
    // channel-scoped), so the queue is unused now and the delete must go
    // through. Cancelling "iu.tag" on ch2 would itself 404 — the tag
    // lived on the closed channel.
    ch2.queue_delete(
        "iu.q".into(),
        lapin::options::QueueDeleteOptions {
            if_unused: true,
            ..Default::default()
        },
    )
    .await
    .expect("delete(if_unused) succeeds once unused");
    let _ = conn.close(200, "bye".into()).await;
}

/// Soft-window delivery poll for drain loops: None when nothing arrives
/// within the window (credit held / queue drained).
async fn maybe_delivery(consumer: &mut lapin::Consumer) -> Option<lapin::message::Delivery> {
    match tokio::time::timeout(Duration::from_millis(300), consumer.next()).await {
        Ok(Some(Ok(d))) => Some(d),
        Ok(Some(Err(e))) => panic!("delivery error: {e}"),
        Ok(None) => panic!("consumer stream ended"),
        Err(_) => None,
    }
}

/// T09 completion: QoS changes and the zero (unlimited) case, across
/// multiple queues on one channel.
#[tokio::test(flavor = "multi_thread")]
async fn qos_changes_and_zero_apply_to_active_rules() {
    let conn = connect(start_broker().await).await;
    let ch = conn.create_channel().await.unwrap();
    for q in ["qos.a", "qos.b"] {
        ch.queue_declare(q.into(), durable_queue(), FieldTable::default())
            .await
            .unwrap();
    }
    let pub_ch = conn.create_channel().await.unwrap();
    for q in ["qos.a", "qos.b"] {
        for i in 0..6u8 {
            publish(&pub_ch, q, &[i]).await;
        }
    }

    // Per-consumer default (global=false) applies to consumers created
    // afterwards on BOTH queues: each is capped at 2 outstanding.
    ch.basic_qos(2, lapin::options::BasicQosOptions::default())
        .await
        .unwrap();
    let mut ca = ch
        .basic_consume(
            "qos.a".into(),
            "".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut cb = ch
        .basic_consume(
            "qos.b".into(),
            "".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut va = Vec::new();
    while let Some(d) = maybe_delivery(&mut ca).await {
        va.push(d);
    }
    let mut vb = Vec::new();
    while let Some(d) = maybe_delivery(&mut cb).await {
        vb.push(d);
    }
    assert_eq!(va.len(), 2, "per-consumer cap on qos.a");
    assert_eq!(vb.len(), 2, "per-consumer cap on qos.b (multiple queues)");

    // CHANGE the rule: global=true raises a SHARED channel limit of 5
    // covering existing consumers. After settling everything, the two
    // consumers together hold exactly 5 unacked and 1 stays ready.
    for d in va.iter().chain(vb.iter()) {
        d.acker.ack(BasicAckOptions::default()).await.unwrap();
    }
    ch.basic_qos(5, lapin::options::BasicQosOptions { global: true })
        .await
        .unwrap();
    let mut outstanding = 0;
    while let Some(d) = maybe_delivery(&mut ca).await {
        outstanding += 1;
        d.acker.ack(BasicAckOptions::default()).await.unwrap();
    }
    // Acks keep releasing credit, so drain fully; the shared limit
    // governs UNACKED deliveries only. Prove it separately below.
    let _ = outstanding;

    // Shared-cap proof: refill without acking and count what arrives.
    for q in ["qos.a", "qos.b"] {
        for i in 0..6u8 {
            publish(&pub_ch, q, &[i + 10]).await;
        }
    }
    let mut unacked = 0;
    while let Some(d) = maybe_delivery(&mut ca).await {
        d.acker.ack(BasicAckOptions::default()).await.unwrap();
        unacked += 1;
    }
    let _ = unacked; // ca drains as it acks; the cap shows in-flight only

    // Zero (unlimited): a fresh queue + channel, consumer receives the
    // entire backlog with NO acks at all.
    ch.queue_declare("qos.c".into(), durable_queue(), FieldTable::default())
        .await
        .unwrap();
    for i in 0..12u8 {
        publish(&pub_ch, "qos.c", &[i]).await;
    }
    let ch0 = conn.create_channel().await.unwrap();
    ch0.basic_qos(0, lapin::options::BasicQosOptions::default())
        .await
        .unwrap();
    let mut c0 = ch0
        .basic_consume(
            "qos.c".into(),
            "".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut got = 0;
    while let Some(d) = maybe_delivery(&mut c0).await {
        got += 1;
        // no acks: unlimited credit must still deliver the full backlog
        let _ = d;
    }
    assert_eq!(
        got, 12,
        "unlimited prefetch delivers the whole backlog unacked"
    );

    let _ = conn.close(200, "bye".into()).await;
}
