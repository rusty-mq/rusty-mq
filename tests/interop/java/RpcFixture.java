import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * rusty-mq interop fixture: Java RabbitMQ client (one of the five target
 * clients). Mirrors the pika/amqplib fixtures: handshake+auth, durable
 * topology, confirmed persistent publish, manual-ack get with typed
 * properties, and T25 request/reply over an exclusive server-named reply
 * queue (§2.3 workload 4).
 *
 * Build/run (CI):
 *   curl -LO $MAVEN/com/rabbitmq/amqp-client/5.21.0/amqp-client-5.21.0.jar
 *   javac -cp amqp-client-5.21.0.jar RpcFixture.java
 *   java -cp .:amqp-client-5.21.0.jar RpcFixture "$AMQP_URL"
 *
 * Exit 0 = all assertions passed; prints PASS lines for the harness.
 */
public class RpcFixture {
    public static void main(String[] args) throws Exception {
        String url = System.getenv("AMQP_URL");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(url);
        factory.setAutomaticRecoveryEnabled(false); // recovery profile is a later gate
        Connection conn = factory.newConnection();
        System.out.println("PASS connect handshake+auth");

        Channel ch = conn.createChannel();
        ch.confirmSelect();
        ch.exchangeDeclare("java.ex", "topic", true);
        ch.queueDeclare("java.q", true, false, false, null);
        ch.queueBind("java.q", "java.ex", "a.*.c");
        System.out.println("PASS declare durable topology + bind");

        byte[] body = new byte[]{0, 1, (byte) 0xCE, 0, (byte) 0xE2, (byte) 0x9C, (byte) 0x93};
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .deliveryMode(2)
                .priority(4)
                .correlationId("java-corr")
                .headers(java.util.Map.of("x-n", 9, "x-s", "wörld", "x-b", true))
                .build();
        ch.basicPublish("java.ex", "a.b.c", false, props, body);
        if (!ch.waitForConfirms(5_000)) {
            throw new AssertionError("publish not confirmed");
        }
        System.out.println("PASS confirmed persistent publish");

        GetResponse g = ch.basicGet("java.q", false);
        if (g == null) throw new AssertionError("message missing");
        if (!java.util.Arrays.equals(g.getBody(), body)) throw new AssertionError("body mismatch");
        if (!"application/json".equals(g.getProps().getContentType()))
            throw new AssertionError("content_type lost");
        if (g.getProps().getPriority() != 4) throw new AssertionError("priority lost");
        if (!"java-corr".equals(g.getProps().getCorrelationId()))
            throw new AssertionError("correlation lost");
        Object hn = g.getProps().getHeaders().get("x-n");
        if (!(hn instanceof Number n) || n.intValue() != 9)
            throw new AssertionError("header x-n lost");
        ch.basicAck(g.getEnvelope().getDeliveryTag(), false);
        System.out.println("PASS manual-ack get with typed properties");

        // Topic non-match discarded (confirm first so a wrongly-routed copy
        // would already be visible to the get below).
        ch.basicPublish("java.ex", "a.b.d", null, b("no"));
        ch.waitForConfirms(5_000);
        if (ch.basicGet("java.q", true) != null)
            throw new AssertionError("a.b.d must not route through a.*.c");
        System.out.println("PASS topic non-match discarded");

        // T25: RPC over an exclusive server-named reply queue.
        String replyQueue = ch.queueDeclare("", false, true, true, null).getQueue();
        if (replyQueue == null || replyQueue.isEmpty())
            throw new AssertionError("server must name the reply queue");
        System.out.println("PASS server-named exclusive reply queue");

        // Server side on a second connection echoes via the default exchange.
        Connection server = factory.newConnection();
        Channel sch = server.createChannel();
        sch.queueDeclare("java.rpc", true, false, false, null);
        sch.basicConsume("java.rpc", false, "rpc-server", new DefaultConsumer(sch) {
            @Override
            public void handleDelivery(String tag, Envelope env, AMQP.BasicProperties p, byte[] b) {
                try {
                    AMQP.BasicProperties replyProps =
                            new AMQP.BasicProperties.Builder()
                                    .correlationId(p.getCorrelationId())
                                    .build();
                    byte[] payload = ("reply:").getBytes(StandardCharsets.UTF_8);
                    byte[] out = new byte[payload.length + b.length];
                    System.arraycopy(payload, 0, out, 0, payload.length);
                    System.arraycopy(b, 0, out, payload.length, b.length);
                    sch.basicPublish("", p.getReplyTo(), replyProps, out);
                    sch.basicAck(env.getDeliveryTag(), false);
                } catch (Exception e) {
                    // surface via unchecked
                    throw new RuntimeException(e);
                }
            }
        });
        BlockingQueue<GetResponse> replies = new ArrayBlockingQueue<>(8);
        ch.basicConsume(replyQueue, true, "reply-consumer", new DefaultConsumer(ch) {
            @Override
            public void handleDelivery(String t, Envelope e, AMQP.BasicProperties p, byte[] b) {
                try {
                    replies.put(new GetResponse(e, p, b, 0));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        for (int i = 0; i < 3; i++) {
            String corr = "corr-" + i;
            AMQP.BasicProperties p = new AMQP.BasicProperties.Builder()
                    .replyTo(replyQueue)
                    .correlationId(corr)
                    .deliveryMode(2)
                    .build();
            ch.basicPublish("", "java.rpc", p, ("ping-" + i).getBytes(StandardCharsets.UTF_8));
            GetResponse r = replies.poll(5, TimeUnit.SECONDS);
            if (r == null) throw new AssertionError("reply " + i + " missing");
            String text = new String(r.getBody(), StandardCharsets.UTF_8);
            if (!text.equals("reply:ping-" + i)) throw new AssertionError("payload " + text);
            if (!corr.equals(r.getProps().getCorrelationId()))
                throw new AssertionError("correlation " + r.getProps().getCorrelationId());
        }
        System.out.println("PASS 3 correlated request/reply roundtrips");

        conn.close();
        server.close();
        System.out.println("PASS connection close");
    }

    private static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }
}
