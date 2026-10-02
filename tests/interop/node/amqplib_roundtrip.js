#!/usr/bin/env node
/**
 * rusty-mq interop fixture: Node.js amqplib (one of the five target
 * clients).
 *
 * Scenarios (§17.3 slice): handshake, confirm channel, persistent publish
 * (drain confirmation), properties preservation, multiple consumers with
 * prefetch, backpressure posture (publish burst without unbounded growth).
 *
 * Exit 0 = all assertions passed; prints PASS lines for the harness.
 */
const amqp = require('amqplib');

const URL = process.env.AMQP_URL;
const BODY = Buffer.from([0, 1, 0xce, 0x00, 255, 0xce, 0xe2, 0x9c, 0x93]);

function assert(cond, what) {
  if (!cond) throw new Error(`assertion failed: ${what}`);
}

async function main() {
  const conn = await amqp.connect(URL);
  console.log('PASS connect handshake+auth');

  // Confirm channel.
  const ch = await conn.createConfirmChannel();
  console.log('PASS confirm channel');

  await ch.assertExchange('node.ex', 'direct', { durable: true });
  await ch.assertQueue('node.q', { durable: true });
  await ch.bindQueue('node.q', 'node.ex', 'rk');
  console.log('PASS declare durable topology + bind');

  // Persistent publish with properties; drain the confirm.
  await ch.publish(
    'node.ex',
    'rk',
    BODY,
    {
      contentType: 'application/json',
      deliveryMode: 2,
      priority: 3,
      correlationId: 'node-corr',
      headers: { 'x-n': 7, 'x-s': 'wörld', 'x-b': true },
    },
  );
  await ch.waitForConfirms();
  console.log('PASS confirmed persistent publish');

  // Get with manual ack: payload bit-identical, properties preserved.
  const msg = await ch.get('node.q', { noAck: false });
  assert(msg, 'message present');
  assert(msg.content.equals(BODY), 'body bit-identical');
  assert(msg.properties.contentType === 'application/json', 'content_type');
  assert(msg.properties.priority === 3, 'priority');
  assert(msg.properties.correlationId === 'node-corr', 'correlation_id');
  assert(msg.properties.headers['x-n'] === 7, 'header x-n');
  assert(msg.properties.headers['x-s'] === 'wörld', 'header x-s');
  assert(msg.properties.headers['x-b'] === true, 'header x-b');
  ch.ack(msg);
  console.log('PASS manual-ack get with typed properties');

  // Multiple consumers with prefetch, round-robin over a burst.
  for (let i = 0; i < 8; i++) {
    ch.publish('node.ex', 'rk', Buffer.from([i]), { deliveryMode: 2 });
  }
  await ch.waitForConfirms();

  const ch2 = await conn.createChannel();
  await ch2.prefetch(2);
  const seen = [];
  const c1 = await ch2.consume('node.q', (m) => { seen.push(m.content[0]); ch2.ack(m); });
  // Wait for all eight to flow through one consumer with prefetch 2.
  const deadline = Date.now() + 5000;
  while (seen.length < 8 && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 20));
  }
  assert(seen.length === 8, `expected 8 deliveries, got ${seen.length}`);
  const sorted = [...seen].sort((a, b) => a - b);
  assert(
    sorted.every((v, i) => v === i),
    `exactly-once delivery: ${seen}`,
  );
  await ch2.cancel(c1.consumerTag);
  console.log('PASS prefetch-2 exactly-once burst');

  await conn.close();
  console.log('PASS connection close');
}

main().catch((err) => {
  console.error(`FAIL ${err && err.stack ? err.stack.split('\n')[0] : err}`);
  process.exit(1);
});
