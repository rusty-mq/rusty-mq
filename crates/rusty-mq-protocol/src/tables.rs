//! Frozen field-table budget enforcement (docs/protocol-profile.md).
//!
//! Nesting depth (16) and per-container field count (1,024) are enforced on
//! the RAW method/header payload BEFORE amq-protocol's parser runs: that
//! parser recurses per nesting level without its own guard, and a hostile
//! table nested a few thousand levels deep fits inside a legal frame —
//! rejecting it here keeps the violation a frame error instead of a stack
//! overflow. ADR-0006 reuses amq-protocol for wire types; this module is
//! our own validation walk over the same frozen byte grammar (the
//! RabbitMQ field-table dialect in the profile). A mismatch between this
//! walker and the profile table is a bug: the cross-check tests below
//! generate real frames with amq-protocol and round-trip them through
//! this validator.

use crate::limits::ProtocolLimits;

/// A field-table budget violation; every variant is a fatal frame error
/// (connection closed) per the profile's "parse error beyond" wording.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum TableError {
    #[error("field table nesting depth {depth} exceeds limit {limit}")]
    DepthExceeded { depth: u32, limit: u8 },
    #[error("field container holds {fields} entries, limit {limit}")]
    FieldsExceeded { fields: u32, limit: u32 },
    #[error("malformed field table: {what}")]
    Malformed { what: &'static str },
}

impl TableError {
    /// The connection-close reply text.
    pub fn message(&self) -> String {
        format!("field table budget: {self}")
    }
}

/// Per-value fixed sizes / structure for the frozen type alphabet
/// (protocol-profile.md "Field table value types").
mod value {
    pub const BOOLEAN: u8 = b't';
    pub const I8: u8 = b'b';
    pub const U8: u8 = b'B';
    pub const SHORT_I: u8 = b's';
    pub const SHORT_I_ALT: u8 = b'U';
    pub const SHORT_U: u8 = b'u';
    pub const LONG_I: u8 = b'I';
    pub const LONG_U: u8 = b'i';
    pub const LONG_LONG_I: u8 = b'l';
    pub const LONG_LONG_ALT: u8 = b'L';
    pub const FLOAT: u8 = b'f';
    pub const DOUBLE: u8 = b'd';
    pub const DECIMAL: u8 = b'D';
    pub const LONG_STR: u8 = b'S';
    pub const ARRAY: u8 = b'A';
    pub const TIMESTAMP: u8 = b'T';
    pub const TABLE: u8 = b'F';
    pub const BYTE_ARRAY: u8 = b'x';
    pub const VOID: u8 = b'V';
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8], pos: usize) -> Self {
        Self { buf, pos }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], TableError> {
        let end = self.pos.checked_add(n).ok_or(TableError::Malformed {
            what: "length overflow",
        })?;
        if end > self.buf.len() {
            return Err(TableError::Malformed {
                what: "truncated value",
            });
        }
        let out = &self.buf[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, TableError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, TableError> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
}

/// Walk one field table starting at the reader position; on success the
/// reader is positioned just after the table. The table at depth 1 is the
/// top-level one; every nested container (table or array) increments depth.
fn check_table(r: &mut Reader<'_>, depth: u32, limits: &ProtocolLimits) -> Result<(), TableError> {
    if depth > u32::from(limits.max_table_depth) {
        return Err(TableError::DepthExceeded {
            depth,
            limit: limits.max_table_depth,
        });
    }
    let len = r.u32()? as usize;
    let end = r.pos.checked_add(len).ok_or(TableError::Malformed {
        what: "table length overflow",
    })?;
    if end > r.buf.len() {
        return Err(TableError::Malformed {
            what: "table runs past frame end",
        });
    }
    let mut fields: u32 = 0;
    while r.pos < end {
        fields += 1;
        if fields > limits.max_table_fields {
            return Err(TableError::FieldsExceeded {
                fields,
                limit: limits.max_table_fields,
            });
        }
        let key_len = r.u8()? as usize;
        r.take(key_len)?;
        check_value(r, depth, limits)?;
    }
    if r.pos != end {
        return Err(TableError::Malformed {
            what: "table length disagrees with entries",
        });
    }
    Ok(())
}

/// Walk one value (the type char is the next byte at the reader position).
fn check_value(r: &mut Reader<'_>, depth: u32, limits: &ProtocolLimits) -> Result<(), TableError> {
    let tag = r.u8()?;
    match tag {
        value::VOID => Ok(()),
        value::BOOLEAN | value::I8 | value::U8 => r.take(1).map(|_| ()),
        value::SHORT_I | value::SHORT_I_ALT | value::SHORT_U => r.take(2).map(|_| ()),
        value::LONG_I | value::LONG_U | value::FLOAT => r.take(4).map(|_| ()),
        value::LONG_LONG_I | value::LONG_LONG_ALT | value::DOUBLE | value::TIMESTAMP => {
            r.take(8).map(|_| ())
        }
        value::DECIMAL => r.take(5).map(|_| ()),
        value::LONG_STR | value::BYTE_ARRAY => {
            let n = r.u32()? as usize;
            r.take(n).map(|_| ())
        }
        value::ARRAY => {
            if depth + 1 > u32::from(limits.max_table_depth) {
                return Err(TableError::DepthExceeded {
                    depth: depth + 1,
                    limit: limits.max_table_depth,
                });
            }
            let len = r.u32()? as usize;
            let end = r.pos.checked_add(len).ok_or(TableError::Malformed {
                what: "array length overflow",
            })?;
            if end > r.buf.len() {
                return Err(TableError::Malformed {
                    what: "array runs past frame end",
                });
            }
            let mut elements: u32 = 0;
            while r.pos < end {
                elements += 1;
                if elements > limits.max_table_fields {
                    return Err(TableError::FieldsExceeded {
                        fields: elements,
                        limit: limits.max_table_fields,
                    });
                }
                check_value(r, depth + 1, limits)?;
            }
            if r.pos != end {
                return Err(TableError::Malformed {
                    what: "array length disagrees with elements",
                });
            }
            Ok(())
        }
        value::TABLE => check_table(r, depth + 1, limits),
        _ => Err(TableError::Malformed {
            what: "unknown field-table type char",
        }),
    }
}

/// The table-bearing methods in the frozen accepted set, with their prefix
/// layout (everything between the method id and the table). Bit fields pack
/// one u8 per method here (≤ 5 booleans each, none cross a byte boundary).
enum TableSite {
    /// Table is the first argument (offset relative to payload start).
    AtPayloadStart,
    /// u16 ticket, shortstr, then the table (queue.declare).
    TicketName,
    /// u16 ticket, shortstr queue, shortstr exchange, shortstr key,
    /// u8 bits, then the table (queue.bind).
    TicketNameNameNameBits,
    /// u16 ticket, three shortstrs, then the table — no bits
    /// (queue.unbind).
    TicketNameNameName,
    /// u16 ticket, shortstr name, shortstr kind, u8 bits, then the table
    /// (exchange.declare).
    TicketNameNameBits,
    /// u16 ticket, shortstr queue, shortstr tag, u8 bits, then the table
    /// (basic.consume).
    TicketNameNameBitsConsume,
}

fn table_site(class_id: u16, method_id: u16) -> Option<TableSite> {
    match (class_id, method_id) {
        (10, 11) => Some(TableSite::AtPayloadStart), // connection.start-ok
        (50, 10) => Some(TableSite::TicketName),     // queue.declare
        (50, 20) => Some(TableSite::TicketNameNameNameBits), // queue.bind
        (50, 50) => Some(TableSite::TicketNameNameName), // queue.unbind
        (40, 10) => Some(TableSite::TicketNameNameBits), // exchange.declare
        (60, 20) => Some(TableSite::TicketNameNameBitsConsume), // basic.consume
        _ => None,
    }
}

fn skip_shortstr(r: &mut Reader<'_>) -> Result<(), TableError> {
    let n = r.u8()? as usize;
    r.take(n).map(|_| ())
}

fn table_offset(site: TableSite, payload: &[u8]) -> Result<usize, TableError> {
    let mut r = Reader::new(payload, 4); // past class id + method id
    match site {
        TableSite::AtPayloadStart => Ok(4),
        TableSite::TicketName => {
            r.take(2)?; // ticket
            skip_shortstr(&mut r)?;
            r.take(1)?; // bit pack
            Ok(r.pos)
        }
        TableSite::TicketNameNameNameBits => {
            r.take(2)?;
            skip_shortstr(&mut r)?;
            skip_shortstr(&mut r)?;
            skip_shortstr(&mut r)?;
            r.take(1)?;
            Ok(r.pos)
        }
        TableSite::TicketNameNameName => {
            r.take(2)?;
            skip_shortstr(&mut r)?;
            skip_shortstr(&mut r)?;
            skip_shortstr(&mut r)?;
            Ok(r.pos)
        }
        TableSite::TicketNameNameBits => {
            r.take(2)?;
            skip_shortstr(&mut r)?;
            skip_shortstr(&mut r)?;
            r.take(1)?;
            Ok(r.pos)
        }
        TableSite::TicketNameNameBitsConsume => {
            r.take(2)?;
            skip_shortstr(&mut r)?;
            skip_shortstr(&mut r)?;
            r.take(1)?;
            Ok(r.pos)
        }
    }
}

/// Validate the field table(s) of a decoded method payload. Non
/// table-bearing methods (and methods outside the accepted set — the
/// dispatch layer rejects those per the error profile) pass through.
pub fn validate_method(payload: &[u8], limits: &ProtocolLimits) -> Result<(), TableError> {
    let Some(site) = table_site_from_payload(payload) else {
        return Ok(());
    };
    let offset = table_offset(site, payload)?;
    let mut r = Reader::new(payload, offset);
    check_table(&mut r, 1, limits)?;
    Ok(())
}

fn table_site_from_payload(payload: &[u8]) -> Option<TableSite> {
    if payload.len() < 4 {
        return None;
    }
    let class_id = u16::from_be_bytes([payload[0], payload[1]]);
    let method_id = u16::from_be_bytes([payload[2], payload[3]]);
    table_site(class_id, method_id)
}

/// Validate a content-header payload: the properties/headers byte budget
/// and (for the basic class, the only content class in V1) the headers
/// table nesting/field caps. Layout: class_id u16, weight u16, body size
/// u64, property flags u16, then the property values in flag order.
pub fn validate_content_header(payload: &[u8], limits: &ProtocolLimits) -> Result<(), TableError> {
    if payload.len() < 14 {
        return Err(TableError::Malformed {
            what: "content header shorter than fixed part",
        });
    }
    let props_budget = payload.len() - 14;
    if props_budget > limits.max_header_bytes as usize {
        return Err(TableError::Malformed {
            what: "properties exceed header budget",
        });
    }
    let class_id = u16::from_be_bytes([payload[0], payload[1]]);
    if class_id != 60 {
        // Only basic content exists in V1; anything else is rejected by
        // the frame layer before properties matter.
        return Ok(());
    }
    let flags = u16::from_be_bytes([payload[12], payload[13]]);
    let mut r = Reader::new(payload, 14);
    // Flags high-bit first: content_type, content_encoding, headers, ...
    const F_CONTENT_TYPE: u16 = 1 << 15;
    const F_CONTENT_ENCODING: u16 = 1 << 14;
    const F_HEADERS: u16 = 1 << 13;
    if flags & F_CONTENT_TYPE != 0 {
        skip_shortstr(&mut r)?;
    }
    if flags & F_CONTENT_ENCODING != 0 {
        skip_shortstr(&mut r)?;
    }
    if flags & F_HEADERS != 0 {
        check_table(&mut r, 1, limits)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use amq_protocol::frame::{gen_frame, AMQPContentHeader, AMQPFrame};
    use amq_protocol::protocol::{
        basic as basic7, connection as conn7, exchange as ex7, queue as q7, AMQPClass,
    };
    use amq_protocol::types::{AMQPValue, FieldTable, ShortString};
    use cookie_factory::gen_simple;
    use std::collections::BTreeMap;

    fn limits() -> ProtocolLimits {
        ProtocolLimits::default()
    }

    fn method_payload(frame: AMQPFrame) -> Vec<u8> {
        let bytes: Vec<u8> = gen_simple(gen_frame(&frame), Vec::new()).unwrap();
        bytes[7..bytes.len() - 1].to_vec() // strip frame header + frame end
    }

    fn header_payload(props: basic7::AMQPProperties, body_size: u64) -> Vec<u8> {
        let frame = AMQPFrame::Header(
            1,
            60,
            Box::new(AMQPContentHeader {
                class_id: 60,
                body_size,
                properties: props,
            }),
        );
        let bytes: Vec<u8> = gen_simple(gen_frame(&frame), Vec::new()).unwrap();
        bytes[7..bytes.len() - 1].to_vec()
    }

    fn table(fields: &[(&str, AMQPValue)]) -> FieldTable {
        fields
            .iter()
            .map(|(k, v)| (ShortString::from(*k), v.clone()))
            .collect::<BTreeMap<ShortString, AMQPValue>>()
            .into()
    }

    fn declare_args(arguments: FieldTable) -> q7::Declare {
        q7::Declare {
            queue: "q.name-with.dots".into(),
            passive: false,
            durable: true,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments,
        }
    }

    fn nested(depth: u32) -> AMQPValue {
        let mut v = AMQPValue::Void;
        for _ in 0..depth {
            v = AMQPValue::FieldTable(table(&[("x", v)]));
        }
        v
    }

    #[test]
    fn amqp_protocol_generated_methods_round_trip_the_walker() {
        // Cross-check: real client-shaped frames pass the raw walker, so
        // the prefix layouts match amq-protocol's wire grammar exactly.
        let declare = q7::Declare {
            queue: "q.name-with.dots".into(),
            passive: false,
            durable: true,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: table(&[
                ("x-queue-type", AMQPValue::LongString("classic".into())),
                ("x-n", AMQPValue::LongInt(7)),
            ]),
        };
        let payload = method_payload(AMQPFrame::Method(
            1,
            AMQPClass::Queue(q7::AMQPMethod::Declare(declare)),
        ));
        validate_method(&payload, &limits()).unwrap();

        let bind = q7::Bind {
            queue: "q".into(),
            exchange: "ex.ch".into(),
            routing_key: "a.b.c".into(),
            nowait: false,
            arguments: FieldTable::default(),
        };
        let payload = method_payload(AMQPFrame::Method(
            1,
            AMQPClass::Queue(q7::AMQPMethod::Bind(bind)),
        ));
        validate_method(&payload, &limits()).unwrap();

        let unbind = q7::Unbind {
            queue: "q".into(),
            exchange: "ex".into(),
            routing_key: "rk".into(),
            arguments: table(&[("k", AMQPValue::Boolean(true))]),
        };
        let payload = method_payload(AMQPFrame::Method(
            1,
            AMQPClass::Queue(q7::AMQPMethod::Unbind(unbind)),
        ));
        validate_method(&payload, &limits()).unwrap();

        let ex_declare = ex7::Declare {
            exchange: "topic.ex".into(),
            kind: "topic".into(),
            passive: false,
            durable: true,
            auto_delete: false,
            internal: false,
            nowait: false,
            arguments: FieldTable::default(),
        };
        let payload = method_payload(AMQPFrame::Method(
            1,
            AMQPClass::Exchange(ex7::AMQPMethod::Declare(ex_declare)),
        ));
        validate_method(&payload, &limits()).unwrap();

        let consume = basic7::Consume {
            queue: "q".into(),
            consumer_tag: "tag".into(),
            no_local: false,
            no_ack: true,
            exclusive: false,
            nowait: false,
            arguments: table(&[("x-priority", AMQPValue::ShortShortUInt(1))]),
        };
        let payload = method_payload(AMQPFrame::Method(
            1,
            AMQPClass::Basic(basic7::AMQPMethod::Consume(consume)),
        ));
        validate_method(&payload, &limits()).unwrap();

        let start_ok = conn7::StartOk {
            client_properties: table(&[
                ("product", AMQPValue::LongString("probe".into())),
                ("version", AMQPValue::LongString("1.0".into())),
                (
                    "capabilities",
                    AMQPValue::FieldTable(table(&[("f", AMQPValue::Boolean(true))])),
                ),
            ]),
            mechanism: "PLAIN".into(),
            response: vec![].into(),
            locale: "en_US".into(),
        };
        let payload = method_payload(AMQPFrame::Method(
            0,
            AMQPClass::Connection(conn7::AMQPMethod::StartOk(start_ok)),
        ));
        validate_method(&payload, &limits()).unwrap();
    }

    #[test]
    fn depth_limit_is_enforced_on_raw_bytes() {
        // 16 levels of nesting is legal, 17 is a frame error.
        let legal = q7::Declare {
            arguments: table(&[("x", nested(15))]), // value adds one level
            ..declare_args(FieldTable::default())
        };
        let payload = method_payload(AMQPFrame::Method(
            1,
            AMQPClass::Queue(q7::AMQPMethod::Declare(legal)),
        ));
        validate_method(&payload, &limits()).unwrap();

        let over = q7::Declare {
            arguments: table(&[("x", nested(16))]),
            ..declare_args(FieldTable::default())
        };
        let payload = method_payload(AMQPFrame::Method(
            1,
            AMQPClass::Queue(q7::AMQPMethod::Declare(over)),
        ));
        match validate_method(&payload, &limits()) {
            Err(TableError::DepthExceeded { depth, limit: 16 }) => assert_eq!(depth, 17),
            other => panic!("expected depth error, got {other:?}"),
        }
    }

    #[test]
    fn hostile_deep_table_is_rejected_before_any_parser_recursion() {
        // Hand-crafted bytes: 50k nested tables, far beyond the depth cap
        // but small in bytes. The walker must stop at the cap, never
        // walking (or recursing) anywhere near all 50k levels.
        let mut payload = vec![0, 50, 0, 10]; // queue.declare
        payload.extend_from_slice(&0u16.to_be_bytes()); // ticket
        payload.push(1);
        payload.push(b'q'); // queue name
        payload.push(0); // bits
                         // Level k serializes as u32(7k) + [keylen "x", 'F'] + level k-1
                         // (level 0 is the empty table u32(0)). Built bottom-up in a flat
                         // loop: a recursive builder would itself overflow at this depth.
        let levels = 50_000usize;
        payload.extend_from_slice(&((levels * 7) as u32).to_be_bytes());
        for k in (0..levels).rev() {
            payload.extend_from_slice(&[1, b'x', value::TABLE]);
            payload.extend_from_slice(&((k * 7) as u32).to_be_bytes());
        }
        let start = std::time::Instant::now();
        match validate_method(&payload, &limits()) {
            Err(TableError::DepthExceeded { .. }) => {}
            other => panic!("expected depth error, got {other:?}"),
        }
        assert!(
            start.elapsed().as_millis() < 100,
            "walker descended past the depth cap"
        );
    }

    #[test]
    fn field_count_limit_is_enforced() {
        let mut map = BTreeMap::new();
        for i in 0..1_025 {
            map.insert(ShortString::from(format!("k{i}")), AMQPValue::Void);
        }
        let declare = q7::Declare {
            arguments: map.into(),
            ..declare_args(FieldTable::default())
        };
        let payload = method_payload(AMQPFrame::Method(
            1,
            AMQPClass::Queue(q7::AMQPMethod::Declare(declare)),
        ));
        match validate_method(&payload, &limits()) {
            Err(TableError::FieldsExceeded {
                fields,
                limit: 1_024,
            }) => {
                assert_eq!(fields, 1_025)
            }
            other => panic!("expected fields error, got {other:?}"),
        }
    }

    #[test]
    fn unknown_type_char_and_truncation_are_errors() {
        // Table: len=12, key "x", type char 0xFF (unknown).
        let payload = [
            &[0u8, 50, 0, 10][..],         // queue.declare
            &0u16.to_be_bytes(),           // ticket
            &[1u8, b'q', 0u8],             // name + bits
            &12u32.to_be_bytes(),          // table length
            &[1u8, b'x', 0xFFu8, 0, 0, 0], // entry with unknown char
        ]
        .concat();
        assert!(matches!(
            validate_method(&payload, &limits()),
            Err(TableError::Malformed { .. })
        ));

        // Table length runs past the frame end.
        let payload = [
            &[0u8, 50, 0, 10][..],
            &0u16.to_be_bytes(),
            &[1u8, b'q', 0u8],
            &0xFFFF_FFFFu32.to_be_bytes(),
        ]
        .concat();
        assert!(matches!(
            validate_method(&payload, &limits()),
            Err(TableError::Malformed { .. })
        ));
    }

    #[test]
    fn content_header_table_and_budget_are_enforced() {
        let props = basic7::AMQPProperties::default()
            .with_content_type(ShortString::from("application/json"))
            .with_headers(table(&[("h", nested(20))]));
        let payload = header_payload(props, 8);
        assert!(matches!(
            validate_content_header(&payload, &limits()),
            Err(TableError::DepthExceeded { .. })
        ));

        let ok_props = basic7::AMQPProperties::default().with_headers(table(&[(
            "h",
            AMQPValue::FieldArray(vec![AMQPValue::LongString("v".into()); 3].into()),
        )]));
        let payload = header_payload(ok_props, 8);
        validate_content_header(&payload, &limits()).unwrap();
    }

    #[test]
    fn array_element_count_shares_the_cap() {
        // 1,025 array elements inside one table entry.
        let arguments = table(&[(
            "arr",
            AMQPValue::FieldArray(vec![AMQPValue::Void; 1_025].into()),
        )]);
        let declare = q7::Declare {
            arguments,
            ..declare_args(FieldTable::default())
        };
        let payload = method_payload(AMQPFrame::Method(
            1,
            AMQPClass::Queue(q7::AMQPMethod::Declare(declare)),
        ));
        assert!(matches!(
            validate_method(&payload, &limits()),
            Err(TableError::FieldsExceeded { .. })
        ));
    }
}
