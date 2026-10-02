# rusty-mq AMQP 0-9-1 protocol profile

This document freezes the V1 protocol profile: framing rules, negotiated
limits, the supported method set, error responses, and intentional deviations.
It is derived from the AMQP 0-9-1 specification (input reference
[rabbitmq/amqp-0.9.1-spec](https://github.com/rabbitmq/amqp-0.9.1-spec)) and
the RabbitMQ behavior needed for the five-client matrix. Where rusty-mq is
deliberately stricter than common implementations, the deviation is listed in
[compatibility/features.yaml](../compatibility/features.yaml).

## Wire framing

- Protocol header accepted: `AMQP\x00\x00\x09\x01` only. Any other
  `AMQP\x00\x00...` header is answered with the supported header and the
  connection is closed; non-AMQP input is dropped and closed.
- Frame layout: `type(1) | channel(2) | size(4) | payload(size) | frame-end(0xCE)`.
- Frame types: `1` method, `2` content header, `3` content body, `4` heartbeat.
- Frames are decoded incrementally; arbitrary TCP chunk boundaries must not
  matter (FR-P05). Allocation is bounded by the negotiated `frame_max` before
  any payload length is trusted.
- Body frames are reassembled under the message byte cap; a header whose
  `body_size` exceeds the cap is rejected before any body frame is read.

## Negotiated limits (server defaults)

| Limit | Default | Notes |
| --- | --- | --- |
| `frame_max` | 131,072 B | inclusive of framing; floor 4096 (protocol minimum) |
| `channel_max` | 256 | excluding channel 0 |
| heartbeat | 60 s | standard negotiation: min(client, server); 0 disables |
| handshake timeout | 10 s | protocol header → `connection.open-ok` |
| assembly timeout | 30 s | content header → last body frame |
| message body cap | 16 MiB | 311 CONTENT_TOO_LARGE before admission |
| header/table budget | 64 KiB | also bounded by `frame_max` |
| table/array nesting depth | 16 | parse error beyond |
| field count per container | 1,024 | parse error beyond |

## Field table value types

The decoder follows the **RabbitMQ implementation** of field tables (this is
what all five target clients emit), which differs from the raw 0-9-1 spec text:

| Char | Type | Notes |
| --- | --- | --- |
| `t` | boolean (u8) | |
| `b` / `B` | i8 / u8 | |
| `s`, `U` | i16 | RabbitMQ uses `s`; spec `U` accepted on input |
| `u` | u16 | |
| `I` / `i` | i32 / u32 | |
| `l`, `L` | i64 | RabbitMQ treats both as signed 64-bit; `L` accepted on input |
| `f` / `d` | f32 / f64 | |
| `D` | decimal (scale u8, value i32) | |
| `S` | long string (u32 length) | |
| `A` | field array | |
| `T` | timestamp (u64) | |
| `F` | field table | |
| `x` | byte array | RabbitMQ extension |
| `V` | void | |

Unknown type characters are a parse error (the frame/connection is closed),
not silently skipped.

## Supported methods (V1 target set)

- **connection:** `start`, `start-ok`, `tune`, `tune-ok`, `open`, `open-ok`,
  `close`, `close-ok`. SASL PLAIN only in V1.
- **channel:** `open`, `open-ok`, `flow`, `flow-ok`, `close`, `close-ok`.
- **exchange:** `declare`, `declare-ok`, `delete`, `delete-ok` (direct,
  fanout, topic + built-ins; see features.yaml for rejections).
- **queue:** `declare`, `declare-ok`, `delete`, `delete-ok`, `bind`,
  `bind-ok`, `unbind`, `unbind-ok`, `purge`, `purge-ok`.
- **basic:** `publish`, `consume`, `consume-ok`, `cancel`, `cancel-ok`,
  `get`, `get-ok`, `get-empty`, `ack`, `reject`, `nack`, `recover`,
  `recover-ok`, `qos`, `qos-ok`, `return`, `deliver`, `ack`(confirm),
  `nack`(confirm).
- **confirm:** `select`, `select-ok`.

Anything outside this set (e.g. `tx.*`, `basic.recover-async`,
`exchange.bind`) is rejected with `540 NOT_IMPLEMENTED` at channel scope.

## Publish-time gates (frozen in this slice)

| Condition | Response |
| --- | --- |
| `immediate=true` | 540 NOT_IMPLEMENTED (channel) |
| `expiration` property present | 540 NOT_IMPLEMENTED (channel) |
| `delivery_mode` not in {absent, 1, 2} | 503 COMMAND_INVALID (channel) |
| `user_id` set and ≠ authenticated principal | 403 ACCESS_REFUSED (channel) |
| Nonexistent exchange | 404 NOT_FOUND (channel), before content processing |
| Internal exchange publication | 403 ACCESS_REFUSED (channel) |
| Aggregate in-memory byte budget exceeded | 506 RESOURCE_ERROR (channel); never silent acceptance |
| `mandatory=true`, zero destinations | basic.return 312 NO_ROUTE + content, on the publishing channel |

Content frames arriving for a channel the server has closed (close-handshake
interlude) are dropped, not escalated — matches RabbitMQ and prevents
in-flight client content from killing the connection after a publish-time
rejection.

## Error profile

| Condition | Reply code | Scope |
| --- | --- | --- |
| Missing queue/exchange | 404 NOT_FOUND | channel |
| Permission denied / internal-exchange publish | 403 ACCESS_REFUSED | channel |
| Exclusive resource owned elsewhere | 405 RESOURCE_LOCKED | channel |
| Conflicting redeclare / invalid delivery tag | 406 PRECONDITION_FAILED | channel |
| Deferred feature requested (TTL, DLX, quorum, tx, immediate, ...) | 540 NOT_IMPLEMENTED | channel |
| Oversized message | 311 CONTENT_TOO_LARGE | channel |
| `mandatory=true`, zero destinations | basic.return 312 NO_ROUTE | (plus content) |
| Unknown/unsupported frame type, bad frame-end | 501 FRAME_ERROR | connection |
| Unexpected frame sequence | 505 UNEXPECTED_FRAME | connection |
| Method invalid in current state / unknown method | 503 COMMAND_INVALID | connection |

Channel-scoped errors close only that channel with the offending
class/method ids carried in `channel.close`. Connection-scoped errors carry
them in `connection.close`.

## Server properties (FR/§4.3)

`connection.start` server-properties advertise `product=rusty-mq`, the true
version, `capabilities` restricted to implemented extensions, and PLAIN in
`mechanisms`. rusty-mq never impersonates a RabbitMQ version string.
Capability flags are added only when their paths are implemented and tested:
at M1 no extension capabilities are advertised.
