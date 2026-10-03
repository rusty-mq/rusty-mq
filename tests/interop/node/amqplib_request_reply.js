#!/usr/bin/env node
/**
 * rusty-mq interop fixture: amqplib request/reply (T25, §2.3 workload 4).
 * Real exclusive reply queue (server-named), reply_to + correlation_id,
 * replies via the default exchange; the reply queue dies with its channel
 * owner (the connection).
 */
const amqp = require('amqplib');

const URL = process.env.AMQP_URL;

function assert(cond, what) {
  if (!cond) throw new Error(`assertion failed: ${what}`);
}

async function main() {
  // Server: consume rpc queue, echo with correlation.
  const server = await amqp.connect(URL);
  const sch = await server.createChannel();
  await sch.assertQueue('node.rpc', { durable: true });
  await sch.consume('node.rpc', (m) => {
    sch.publish(
      '',
      m.properties.replyTo,
      Buffer.concat([Buffer.from('reply:'), m.content]),
      { correlationId: m.properties.correlationId },
    );
    sch.ack(m);
  });
  console.log('PASS server topology + consumer');

  // Client: server-named exclusive reply queue.
  const client = await amqp.connect(URL);
  const cch = await client.createChannel();
  // amqplib defaults durable:true; an exclusive queue must be transient
  // (the V1 profile rejects durable+exclusive — correctly).
  const q = await cch.assertQueue('', { exclusive: true, durable: false });
  assert(q.queue, 'server must name the reply queue');
  console.log('PASS server-named exclusive reply queue');

  const replies = [];
  await cch.consume(q.queue, (m) => replies.push(m), { noAck: true });

  for (let i = 0; i < 3; i++) {
    const corr = `corr-${i}`;
    cch.publish('', 'node.rpc', Buffer.from(`ping-${i}`), {
      replyTo: q.queue,
      correlationId: corr,
      deliveryMode: 2,
    });
    const deadline = Date.now() + 5000;
    while (replies.length <= i && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 10));
    }
    assert(replies.length > i, `reply ${i} missing`);
    const m = replies[i];
    assert(m.content.equals(Buffer.from(`reply:ping-${i}`)), `payload ${m.content}`);
    assert(m.properties.correlationId === corr, `correlation ${m.properties.correlationId}`);
  }
  console.log('PASS 3 correlated request/reply roundtrips');

  await client.close();
  await new Promise((r) => setTimeout(r, 200));

  // Reply queue gone (404 on passive assert); rpc queue alive.
  const probe = await amqp.connect(URL);
  const pch = await probe.createChannel();
  // A 404 close surfaces BOTH as the rejected checkQueue promise and as an
  // async channel 'error' event; capture both so the process survives.
  const channelErrors = [];
  pch.once('error', (err) => channelErrors.push(err));
  let replyGone = false;
  try {
    await pch.checkQueue(q.queue);
  } catch (err) {
    const text = String(err && err.message ? err.message : err);
    replyGone = err.code === 404 || text.includes('404');
    assert(replyGone, `expected 404, got ${text}`);
  }
  await new Promise((r) => setTimeout(r, 100));
  assert(
    channelErrors.every((e) => String(e.message || e).includes('404')),
    `only 404 channel errors expected: ${channelErrors}`,
  );
  assert(replyGone || channelErrors.length > 0,
    'exclusive reply queue must die with its connection');
  const pch2 = await probe.createChannel();
  await pch2.checkQueue('node.rpc');
  console.log('PASS exclusive reply queue lifecycle');

  await probe.close();
  await server.close();
}

main().catch((err) => {
  console.error(`FAIL ${err && err.message ? err.message : err}`);
  process.exit(1);
});
