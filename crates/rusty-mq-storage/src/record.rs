//! Journal record model and its versioned encoding (docs/storage-format.md).
//!
//! Hand-rolled, endianness-explicit, layout-independent. No serde: journal
//! bytes must remain stable across compiler versions.

/// Record kind bytes (see the format table).
pub mod kind {
    pub const TX_BEGIN: u8 = 0x01;
    pub const TX_COMMIT: u8 = 0x02;
    pub const QUEUE_DECLARE: u8 = 0x10;
    pub const QUEUE_DELETE: u8 = 0x11;
    pub const EXCHANGE_DECLARE: u8 = 0x12;
    pub const EXCHANGE_DELETE: u8 = 0x13;
    pub const BIND: u8 = 0x14;
    pub const VHOST_DECLARE: u8 = 0x16;
    pub const VHOST_DELETE: u8 = 0x17;
    pub const UNBIND: u8 = 0x15;
    pub const ENQUEUE: u8 = 0x20;
    pub const SETTLE_ACK: u8 = 0x21;
    pub const SETTLE_DISCARD: u8 = 0x22;
    pub const DELIVERED: u8 = 0x23;
    pub const PURGE: u8 = 0x30;
    pub const PRINCIPAL_UPSERT: u8 = 0x40;
    pub const PRINCIPAL_DELETE: u8 = 0x41;
    pub const PERMISSION_SET: u8 = 0x42;
    pub const PERMISSION_DELETE: u8 = 0x43;
    pub const END_MARKER: u8 = 0xF1;
}

/// A logical journal record payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    QueueDeclare(QueueRecord),
    QueueDelete {
        id: u64,
    },
    ExchangeDeclare(ExchangeRecord),
    ExchangeDelete {
        id: u64,
    },
    Bind(Binding),
    Unbind(Binding),
    /// A vhost created via the management plane (§12.1); rebuilt by
    /// re-adding the name (idempotent by name).
    VhostDeclare {
        name: String,
    },
    /// A vhost deleted via the management plane (§12.1 destructive
    /// checks live at the Broker layer; the fold removes the name).
    VhostDelete {
        name: String,
    },
    Enqueue(Enqueue),
    SettleAck {
        queue: u64,
        seq: u64,
    },
    SettleDiscard {
        queue: u64,
        seq: u64,
    },
    /// Delivery-attempt marker: the entry was (or is about to be) exposed
    /// to a manual-ack consumer; recovery restores it with the
    /// conservative redelivered hint (§9.6).
    Delivered {
        queue: u64,
        seq: u64,
    },
    Purge {
        queue: u64,
        seqs: Vec<u64>,
    },
    /// A principal: username + Argon2id password hash (PHC string) + role.
    PrincipalUpsert(PrincipalRecord),
    PrincipalDelete {
        username: String,
    },
    /// Permissions for (user, vhost): configure/write/read regex patterns.
    PermissionSet(PermissionRecord),
    PermissionDelete {
        username: String,
        vhost: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrincipalRecord {
    pub username: String,
    /// Argon2id PHC string (params + salt + hash); never a plaintext.
    pub password_phc: String,
    /// 0 = ordinary, 1 = monitor, 2 = operator, 3 = admin.
    pub role: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PermissionRecord {
    pub username: String,
    pub vhost: String,
    pub configure: String,
    pub write: String,
    pub read: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueRecord {
    pub name: String,
    pub id: u64,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    /// Owning connection id; 0 = none.
    pub owner: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExchangeRecord {
    pub name: String,
    pub id: u64,
    /// 0 direct, 1 fanout, 2 topic.
    pub kind: u8,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Binding {
    pub exchange: u64,
    pub queue: u64,
    pub routing_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Enqueue {
    pub message_id: u64,
    pub property_bytes: Vec<u8>,
    pub body: Vec<u8>,
    pub exchange: String,
    pub routing_key: String,
    pub persistent: bool,
    /// Destination set: (queue id, assigned queue sequence) pairs (§9.4 —
    /// stable ids, never names).
    pub destinations: Vec<(u64, u64)>,
}

impl Record {
    /// Kind byte for this record.
    pub fn kind(&self) -> u8 {
        match self {
            Record::QueueDeclare(_) => kind::QUEUE_DECLARE,
            Record::QueueDelete { .. } => kind::QUEUE_DELETE,
            Record::ExchangeDeclare(_) => kind::EXCHANGE_DECLARE,
            Record::ExchangeDelete { .. } => kind::EXCHANGE_DELETE,
            Record::Bind(_) => kind::BIND,
            Record::Unbind(_) => kind::UNBIND,
            Record::VhostDeclare { .. } => kind::VHOST_DECLARE,
            Record::VhostDelete { .. } => kind::VHOST_DELETE,
            Record::Enqueue(_) => kind::ENQUEUE,
            Record::SettleAck { .. } => kind::SETTLE_ACK,
            Record::SettleDiscard { .. } => kind::SETTLE_DISCARD,
            Record::Delivered { .. } => kind::DELIVERED,
            Record::Purge { .. } => kind::PURGE,
            Record::PrincipalUpsert(_) => kind::PRINCIPAL_UPSERT,
            Record::PrincipalDelete { .. } => kind::PRINCIPAL_DELETE,
            Record::PermissionSet(_) => kind::PERMISSION_SET,
            Record::PermissionDelete { .. } => kind::PERMISSION_DELETE,
        }
    }

    /// Encode payload bytes (kind byte NOT included).
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Buf::default();
        match self {
            Record::QueueDeclare(q) => {
                b.str(&q.name);
                b.u64(q.id);
                b.bool(q.durable);
                b.bool(q.exclusive);
                b.bool(q.auto_delete);
                b.u64(q.owner);
            }
            Record::QueueDelete { id } => b.u64(*id),
            Record::ExchangeDeclare(e) => {
                b.str(&e.name);
                b.u64(e.id);
                b.u8(e.kind);
                b.bool(e.durable);
                b.bool(e.auto_delete);
                b.bool(e.internal);
            }
            Record::ExchangeDelete { id } => b.u64(*id),
            Record::Bind(x) | Record::Unbind(x) => {
                b.u64(x.exchange);
                b.u64(x.queue);
                b.str(&x.routing_key);
            }
            Record::VhostDeclare { name } => b.str(name),
            Record::VhostDelete { name } => b.str(name),
            Record::Enqueue(e) => {
                b.u64(e.message_id);
                b.bytes(&e.property_bytes);
                b.bytes(&e.body);
                b.str(&e.exchange);
                b.str(&e.routing_key);
                b.bool(e.persistent);
                b.u32(e.destinations.len() as u32);
                for (q, s) in &e.destinations {
                    b.u64(*q);
                    b.u64(*s);
                }
            }
            Record::SettleAck { queue, seq }
            | Record::SettleDiscard { queue, seq }
            | Record::Delivered { queue, seq } => {
                b.u64(*queue);
                b.u64(*seq);
            }
            Record::Purge { queue, seqs } => {
                b.u64(*queue);
                b.u32(seqs.len() as u32);
                for s in seqs {
                    b.u64(*s);
                }
            }
            Record::PrincipalUpsert(p) => {
                b.str(&p.username);
                b.str(&p.password_phc);
                b.u8(p.role);
            }
            Record::PrincipalDelete { username } => b.str(username),
            Record::PermissionSet(p) => {
                b.str(&p.username);
                b.str(&p.vhost);
                b.str(&p.configure);
                b.str(&p.write);
                b.str(&p.read);
            }
            Record::PermissionDelete { username, vhost } => {
                b.str(username);
                b.str(vhost);
            }
        }
        b.0
    }

    /// Decode a payload for a known kind byte. `Err` means corruption.
    pub fn decode(kind_byte: u8, payload: &[u8]) -> Result<Record, FormatError> {
        let mut r = Reader {
            buf: payload,
            pos: 0,
        };
        let rec = match kind_byte {
            kind::QUEUE_DECLARE => Record::QueueDeclare(QueueRecord {
                name: r.str()?,
                id: r.u64()?,
                durable: r.bool()?,
                exclusive: r.bool()?,
                auto_delete: r.bool()?,
                owner: r.u64()?,
            }),
            kind::QUEUE_DELETE => Record::QueueDelete { id: r.u64()? },
            kind::EXCHANGE_DECLARE => Record::ExchangeDeclare(ExchangeRecord {
                name: r.str()?,
                id: r.u64()?,
                kind: {
                    let k = r.u8()?;
                    if k > 2 {
                        return Err(FormatError::Corruption(format!("exchange kind {k}")));
                    }
                    k
                },
                durable: r.bool()?,
                auto_delete: r.bool()?,
                internal: r.bool()?,
            }),
            kind::EXCHANGE_DELETE => Record::ExchangeDelete { id: r.u64()? },
            kind::BIND => Record::Bind(Binding {
                exchange: r.u64()?,
                queue: r.u64()?,
                routing_key: r.str()?,
            }),
            kind::UNBIND => Record::Unbind(Binding {
                exchange: r.u64()?,
                queue: r.u64()?,
                routing_key: r.str()?,
            }),
            kind::VHOST_DECLARE => Record::VhostDeclare {
                name: r.str()?.to_string(),
            },
            kind::VHOST_DELETE => Record::VhostDelete {
                name: r.str()?.to_string(),
            },
            kind::ENQUEUE => {
                let message_id = r.u64()?;
                let property_bytes = r.bytes()?.to_vec();
                let body = r.bytes()?.to_vec();
                let exchange = r.str()?.to_string();
                let routing_key = r.str()?.to_string();
                let persistent = r.bool()?;
                let count = r.u32()? as usize;
                if count > r.remaining() / 16 {
                    return Err(FormatError::Corruption(format!(
                        "destination count {count} exceeds payload"
                    )));
                }
                let mut destinations = Vec::with_capacity(count);
                for _ in 0..count {
                    destinations.push((r.u64()?, r.u64()?));
                }
                Record::Enqueue(Enqueue {
                    message_id,
                    property_bytes,
                    body,
                    exchange,
                    routing_key,
                    persistent,
                    destinations,
                })
            }
            kind::SETTLE_ACK => {
                let queue = r.u64()?;
                let seq = r.u64()?;
                Record::SettleAck { queue, seq }
            }
            kind::SETTLE_DISCARD => {
                let queue = r.u64()?;
                let seq = r.u64()?;
                Record::SettleDiscard { queue, seq }
            }
            kind::DELIVERED => {
                let queue = r.u64()?;
                let seq = r.u64()?;
                Record::Delivered { queue, seq }
            }
            kind::PRINCIPAL_UPSERT => Record::PrincipalUpsert(PrincipalRecord {
                username: r.str()?,
                password_phc: r.str()?,
                role: {
                    let role = r.u8()?;
                    if role > 3 {
                        return Err(FormatError::Corruption(format!("role {role}")));
                    }
                    role
                },
            }),
            kind::PRINCIPAL_DELETE => Record::PrincipalDelete { username: r.str()? },
            kind::PERMISSION_SET => Record::PermissionSet(PermissionRecord {
                username: r.str()?,
                vhost: r.str()?,
                configure: r.str()?,
                write: r.str()?,
                read: r.str()?,
            }),
            kind::PERMISSION_DELETE => Record::PermissionDelete {
                username: r.str()?,
                vhost: r.str()?,
            },
            kind::PURGE => {
                let queue = r.u64()?;
                let count = r.u32()? as usize;
                if count > r.remaining() / 8 {
                    return Err(FormatError::Corruption(format!(
                        "purge count {count} exceeds payload"
                    )));
                }
                let mut seqs = Vec::with_capacity(count);
                for _ in 0..count {
                    seqs.push(r.u64()?);
                }
                Record::Purge { queue, seqs }
            }
            other => return Err(FormatError::UnknownKind(other)),
        };
        if r.remaining() != 0 {
            return Err(FormatError::Corruption(format!(
                "{} trailing bytes in payload",
                r.remaining()
            )));
        }
        Ok(rec)
    }
}

/// Journal-level format failures (explicit, never silent skips).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FormatError {
    #[error("bad segment magic")]
    BadMagic,
    #[error("unsupported format major {0}")]
    UnsupportedMajor(u32),
    #[error("segment chain break: expected segment {0}")]
    ChainBreak(u64),
    #[error("unknown record kind 0x{0:02x}")]
    UnknownKind(u8),
    #[error("corruption: {0}")]
    Corruption(String),
    #[error("checksum mismatch at LSN {0}")]
    Checksum(u64),
    #[error("io error: {0}")]
    Io(String),
}

// ---------------------------------------------------------------------
// Little encoding helpers (private).
// ---------------------------------------------------------------------

#[derive(Default)]
struct Buf(Vec<u8>);

impl Buf {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn bool(&mut self, v: bool) {
        self.u8(u8::from(v));
    }
    fn bytes(&mut self, v: &[u8]) {
        self.u32(v.len() as u32);
        self.0.extend_from_slice(v);
    }
    fn str(&mut self, v: &str) {
        self.bytes(v.as_bytes());
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], FormatError> {
        if self.pos + n > self.buf.len() {
            return Err(FormatError::Corruption(format!(
                "truncated payload at {} (+{n} > {})",
                self.pos,
                self.buf.len()
            )));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, FormatError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, FormatError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, FormatError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn bool(&mut self) -> Result<bool, FormatError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(FormatError::Corruption(format!("bad bool {other}"))),
        }
    }
    fn bytes(&mut self) -> Result<&'a [u8], FormatError> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    fn str(&mut self) -> Result<String, FormatError> {
        let b = self.bytes()?;
        std::str::from_utf8(b)
            .map(|s| s.to_string())
            .map_err(|_| FormatError::Corruption("invalid UTF-8 in string".into()))
    }
    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_records() -> Vec<Record> {
        vec![
            Record::QueueDeclare(QueueRecord {
                name: "jobs".into(),
                id: 7,
                durable: true,
                exclusive: false,
                auto_delete: false,
                owner: 0,
            }),
            Record::ExchangeDeclare(ExchangeRecord {
                name: "ex.topic".into(),
                id: 9,
                kind: 2,
                durable: true,
                auto_delete: false,
                internal: false,
            }),
            Record::Bind(Binding {
                exchange: 9,
                queue: 7,
                routing_key: "a.*.c".into(),
            }),
            Record::Enqueue(Enqueue {
                message_id: 1,
                property_bytes: vec![1, 2, 3],
                body: vec![0, 0xCE, 255],
                exchange: "ex.topic".into(),
                routing_key: "a.b.c".into(),
                persistent: true,
                destinations: vec![(7, 1), (8, 5)],
            }),
            Record::SettleAck { queue: 7, seq: 1 },
            Record::SettleDiscard { queue: 8, seq: 5 },
            Record::Purge {
                queue: 7,
                seqs: vec![2, 3, 4],
            },
            Record::QueueDelete { id: 7 },
            Record::ExchangeDelete { id: 9 },
            Record::Unbind(Binding {
                exchange: 9,
                queue: 8,
                routing_key: "x".into(),
            }),
        ]
    }

    #[test]
    fn records_roundtrip_bit_identical() {
        for rec in sample_records() {
            let payload = rec.encode();
            let back = Record::decode(rec.kind(), &payload).unwrap();
            assert_eq!(back, rec, "roundtrip for {:?}", rec);
        }
    }

    #[test]
    fn truncated_payload_is_corruption() {
        let rec = Record::Enqueue(Enqueue {
            message_id: 1,
            property_bytes: vec![],
            body: vec![1, 2, 3],
            exchange: "".into(),
            routing_key: "".into(),
            persistent: false,
            destinations: vec![(1, 1)],
        });
        let payload = rec.encode();
        assert!(Record::decode(rec.kind(), &payload[..payload.len() - 1]).is_err());
    }

    #[test]
    fn unknown_kind_rejected() {
        assert!(matches!(
            Record::decode(0x77, &[]),
            Err(FormatError::UnknownKind(0x77))
        ));
    }

    #[test]
    fn bad_exchange_kind_is_corruption() {
        let mut b = Buf::default();
        b.str("x");
        b.u64(1);
        b.u8(9); // invalid kind
        assert!(matches!(
            Record::decode(kind::EXCHANGE_DECLARE, &b.0),
            Err(FormatError::Corruption(_))
        ));
    }
}
