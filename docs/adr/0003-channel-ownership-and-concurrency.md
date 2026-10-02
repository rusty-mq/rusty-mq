# ADR-0003: Connection/channel ownership model and concurrency layout

- Status: Accepted
- Date: 2026-10-02
- Deciders: rusty-mq baseline (PRD §8.1), maintainer review pending

## Context

AMQP 0-9-1 multiplexes channels over one TCP connection. Delivery tags, QoS
credit, and confirm sequences are channel-scoped; a numeric channel id reused
after close must not inherit old state (INV-04, §7.3). The first concurrency
design must make ordering guarantees cheap to reason about without a global
mutex across I/O.

## Decision

1. Per connection: exactly **one reader task** and **one ordered writer task**
   joined by a bounded mpsc queue (FR-P07). All socket writes are serialized
   through the writer; content ordering per channel is preserved by
   construction.
2. Every channel carries a **generation token** (`ChannelGeneration`) minted
   at `channel.open`. Outstanding deliveries, pending confirms, and credit
   reservations are owned by (channel id, generation); a settlement carrying
   a stale generation is a protocol error rather than a mutation of the new
   channel's state.
3. Connection state (handshake phase, negotiated limits, authenticated
   principal, vhost) is owned by the connection task; channels communicate
   with topology/queue schedulers only through typed commands to the broker
   coordinator (bounded queue), never by direct shared-map mutation.
4. The **broker command coordinator** is a single bounded consumer
   establishing a deterministic ordering point for topology mutations and
   admissions (INV-06). It must never block on disk I/O or socket writes
   inline: durable work is awaited via the storage executor's completion
   path, and replies are queued to the connection writer.
5. Queue schedulers own ready/in-flight state and dispatch only after
   reserving channel credit atomically (FR §6.2 rule 7).
6. Blocking disk/database work runs on a dedicated bounded blocking executor
   (not Tokio I/O workers).

The coordinator is a deliberate V1 simplification; sharding is allowed only
after profiling (PRD §8.1).

## Consequences

- Connection teardown (TCP loss, `connection.close`, handshake timeout)
   reclaims: channels with generations, exclusive/temporary queues, credit
   reservations, and requeues unacked manual-ack deliveries (FR-P09).
- Heartbeat accounting happens in the reader/writer pair without coordinator
  involvement.
- Test T03/T24 (interleaved channels, slow/mixed connections) validate the
  writer ordering and bounded-queue backpressure behavior.
