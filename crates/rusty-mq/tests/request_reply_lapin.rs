//! T25 (lapin slice): request/reply over a REAL temporary exclusive reply
//! queue (§2.3 workload 4) — `reply_to` + `correlation_id`, replies routed
//! through the default exchange to the server-named queue.

use std::sync::Arc;
use std::time::Duration;

use futures_lite::StreamExt;
use lapin::{
    options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};

fn dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("rmq-rpc-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

#[tokio::test(flavor = "multi_thread")]
async fn request_reply_over_exclusive_reply_queue() {
    let broker = Arc::new(rusty_mq::Broker::open_persistent(
        "guest".into(),
        "guest".into(),
        &dir("lapin"),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rusty_mq::server::serve_listener_shared(listener, broker));

    let uri = format!("amqp://guest:guest@{addr}/%2F");

    // Server connection: consumes the rpc queue and echoes with
    // correlation_id back through the default exchange (reply_to = queue
    // name of the client's exclusive temp queue).
    let server = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .unwrap();
    let sch = server.create_channel().await.unwrap();
    sch.queue_declare(
        "rpc.q".into(),
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    let mut consumer = sch
        .basic_consume(
            "rpc.q".into(),
            "".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let server_task = tokio::spawn(async move {
        while let Some(Ok(delivery)) = consumer.next().await {
            let reply_to = delivery.properties.reply_to().clone();
            let correlation = delivery.properties.correlation_id().clone();
            let body = delivery.data.clone();
            // Echo: "reply:<request>".
            let mut reply = b"reply:".to_vec();
            reply.extend_from_slice(&body);
            sch.basic_publish(
                "".into(), // default exchange: routing key = queue name
                reply_to.unwrap_or_default(),
                BasicPublishOptions::default(),
                reply.as_slice(),
                BasicProperties::default().with_correlation_id(correlation.unwrap_or_default()),
            )
            .await
            .unwrap();
            delivery
                .acker
                .ack(lapin::options::BasicAckOptions::default())
                .await
                .unwrap();
        }
    });

    // Client connection: real exclusive server-named reply queue.
    let client = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .unwrap();
    let cch = client.create_channel().await.unwrap();
    let reply_queue = cch
        .queue_declare(
            "".into(),
            QueueDeclareOptions {
                exclusive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    let reply_name = reply_queue.name().as_str().to_string();
    assert!(!reply_name.is_empty(), "server named the reply queue");

    let mut replies = cch
        .basic_consume(
            reply_queue.name().clone(),
            "".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    // Three requests, each with a distinct correlation id; all replies
    // must arrive on the exclusive queue with matching correlations.
    for i in 0..3u8 {
        let corr = format!("corr-{i}");
        cch.basic_publish(
            "".into(),
            "rpc.q".into(),
            BasicPublishOptions::default(),
            format!("ping-{i}").as_bytes(),
            BasicProperties::default()
                .with_reply_to(reply_name.clone().into())
                .with_correlation_id(corr.clone().into()),
        )
        .await
        .unwrap();

        let reply = tokio::time::timeout(Duration::from_secs(5), replies.next())
            .await
            .expect("reply within timeout")
            .expect("stream alive")
            .expect("delivery ok");
        assert_eq!(
            reply.data,
            format!("reply:ping-{i}").into_bytes(),
            "echoed payload"
        );
        assert_eq!(
            reply
                .properties
                .correlation_id()
                .as_ref()
                .map(|s| s.as_str()),
            Some(corr.as_str()),
            "correlation id round-tripped"
        );
    }

    // Cleanup: dropping the client connection must delete the exclusive
    // reply queue (FR-Q04); the rpc queue survives.
    client.close(200, "done".into()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let probe = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .unwrap();
    let pch = probe.create_channel().await.unwrap();
    let gone = pch
        .queue_declare(
            reply_name.clone().into(),
            lapin::options::QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await;
    assert!(
        gone.is_err(),
        "exclusive reply queue died with its connection"
    );
    // The 404 closed the probe channel (correct channel-scoped error);
    // verify the durable queue on a fresh one.
    let pch2 = probe.create_channel().await.unwrap();
    pch2.queue_declare(
        "rpc.q".into(),
        lapin::options::QueueDeclareOptions {
            passive: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .expect("durable rpc queue survives");

    server_task.abort();
    let _ = server.close(200, "bye".into()).await;
}
