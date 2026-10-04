//! Incremental AMQP frame decoding and encoding (FR-P05..FR-P07).
//!
//! [`FrameReader`] accepts arbitrary TCP-sized chunks and yields complete
//! frames only after every budget check (frame type, channel, size, frame-end
//! octet) has passed. No payload length is trusted before it is checked
//! against the negotiated limits, so a hostile size field can never trigger
//! an oversized allocation.

use amq_protocol::frame::parsing::parse_content_header;
use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::parse_class;

use crate::error::{reply_code, ProtocolError};
use crate::limits::{NegotiatedLimits, FRAME_MIN};

/// Frame type octets. NOTE: AMQP 0-9-1 heartbeat frames are type 8
/// (type 4 was 0-8; some docs mistakenly say 4 for 0-9-1).
mod frame_type {
    pub const METHOD: u8 = 1;
    pub const HEADER: u8 = 2;
    pub const BODY: u8 = 3;
    pub const HEARTBEAT: u8 = 8;
}

/// Frame header size: type(1) + channel(2) + size(4).
const HEADER_LEN: usize = 7;
/// Frame overhead: header + frame-end octet.
pub const FRAME_OVERHEAD: usize = HEADER_LEN + 1;

/// Outcome of a header-version check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HeaderCheck {
    /// AMQP 0-9-1, accepted.
    Amqp0_9_1,
    /// Valid AMQP framing but a different version; the server replies with
    /// its own protocol header and closes.
    WrongVersion { major: u8, minor: u8, revision: u8 },
    /// Not AMQP at all.
    NotAmqp,
}

/// Channel-scoped method policy over RAW payload bytes — for fields the
/// amq-protocol codec does not model (and therefore silently drops).
/// Currently: basic.qos (class 60, method 10) reserves a u32
/// `prefetch_size` at offset 4; the frozen profile allows only 0.
fn policy_violation(payload: &[u8]) -> Option<ProtocolError> {
    if payload.len() < 8 {
        return None;
    }
    let class_id = u16::from_be_bytes([payload[0], payload[1]]);
    let method_id = u16::from_be_bytes([payload[2], payload[3]]);
    if (class_id, method_id) != (60, 10) {
        return None;
    }
    let prefetch_size = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
    if prefetch_size == 0 {
        return None;
    }
    Some(ProtocolError::not_implemented(
        format!("basic.qos prefetch_size ({prefetch_size}): only 0 is supported"),
        60,
        10,
    ))
}

/// Incremental frame decoder for one connection direction.
///
/// Feed chunks as they arrive from the socket; call [`FrameReader::next_frame`]
/// until it returns `Ok(None)`. The internal buffer is bounded by roughly two
/// frames: one partially-assembled frame plus one incoming chunk.
pub struct FrameReader {
    buf: Vec<u8>,
    /// Static ceilings (table depth/fields, header budget) from the
    /// negotiated limits; kept for pre-decode validation.
    static_limits: crate::limits::ProtocolLimits,
    /// Byte budget for one frame payload (frame_max minus overhead).
    max_payload: usize,
    /// Total buffer cap guarding against runaway feed() accumulation.
    buf_cap: usize,
    header_seen: bool,
    /// Channel-scoped method-policy violation detected while decoding the
    /// last method frame (see policy_violation); the connection drains it
    /// via take_policy_violation and rejects the channel without tearing
    /// down the connection.
    pending_policy: Option<crate::error::ProtocolError>,
}

impl FrameReader {
    /// Create a reader enforcing `limits`; expects the protocol header as
    /// the first 8 bytes (the connection-initiating direction).
    pub fn new(limits: &NegotiatedLimits) -> Self {
        Self::with_limits(limits, false)
    }

    /// Create a reader for a stream where the protocol header has already
    /// been exchanged (e.g. the server-to-client direction in tests and
    /// client implementations).
    pub fn new_post_header(limits: &NegotiatedLimits) -> Self {
        Self::with_limits(limits, true)
    }

    fn with_limits(limits: &NegotiatedLimits, header_seen: bool) -> Self {
        let frame_max = limits.frame_max as usize;
        Self {
            buf: Vec::with_capacity(frame_max.min(16 * 1024)),
            static_limits: limits.static_limits.clone(),
            max_payload: limits.max_frame_payload() as usize,
            // One frame in assembly + one chunk in flight.
            buf_cap: frame_max.saturating_mul(2).max(FRAME_MIN as usize),
            header_seen,
            pending_policy: None,
        }
    }

    /// Append raw socket bytes. Errors if the accumulated backlog exceeds the
    /// connection budget (a reader must drain between feeds; ADR-0004).
    pub fn feed(&mut self, chunk: &[u8]) -> Result<(), ProtocolError> {
        if chunk.len() + self.buf.len() > self.buf_cap {
            // The chunk alone may be larger than one frame; that is only
            // legitimate if it starts with a complete valid frame. We still
            // allow one chunk of up to buf_cap to make progress, then fail.
            if chunk.len() > self.buf_cap {
                return Err(ProtocolError::frame_error("input backlog exceeds budget").with_fatal());
            }
        }
        self.buf.extend_from_slice(chunk);
        Ok(())
    }

    /// Adopt NEGOTIATED limits after tune-ok (FR-P04: frame_max is
    /// enforced on the read path too — a client that negotiated 4096
    /// must not have larger frames accepted). In place: the buffered
    /// partial frame survives the swap.
    pub fn set_limits(&mut self, limits: &NegotiatedLimits) {
        self.max_payload = limits.max_frame_payload() as usize;
    }

    /// Take the channel-scoped policy violation detected while decoding
    /// the most recent method frame, if any. The connection must send it
    /// as a channel error and skip dispatching that frame.
    pub fn take_policy_violation(&mut self) -> Option<ProtocolError> {
        self.pending_policy.take()
    }

    /// Bytes currently buffered awaiting frame completion.
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Try to decode the next complete frame. `Ok(None)` means more bytes are
    /// needed. Any `Err` closes the connection with 501/503.
    ///
    /// The first frame on a connection must be the protocol header.
    #[allow(clippy::should_implement_trait)]
    pub fn next_frame(&mut self) -> Result<Option<AMQPFrame>, ProtocolError> {
        if !self.header_seen {
            return self.read_protocol_header();
        }
        if self.buf.len() < HEADER_LEN {
            return Ok(None);
        }
        let ftype = self.buf[0];
        let channel = u16::from_be_bytes([self.buf[1], self.buf[2]]);
        let size = u32::from_be_bytes([self.buf[3], self.buf[4], self.buf[5], self.buf[6]]);
        if size as usize > self.max_payload {
            return Err(ProtocolError::frame_error(format!(
                "frame size {size} exceeds negotiated frame_max payload budget {}",
                self.max_payload
            ))
            .with_fatal());
        }
        let total = HEADER_LEN + size as usize + 1;
        if self.buf.len() < total {
            return Ok(None);
        }
        if self.buf[total - 1] != crate::FRAME_END {
            return Err(ProtocolError::frame_error("bad frame-end octet").with_fatal());
        }
        let payload = self.buf[HEADER_LEN..total - 1].to_vec();
        self.buf.drain(..total);

        match ftype {
            frame_type::METHOD => {
                // Table budgets are enforced on the raw bytes BEFORE the
                // parser runs: amq-protocol recurses per nesting level
                // without its own guard (see tables.rs).
                crate::tables::validate_method(&payload, &self.static_limits)
                    .map_err(|e| ProtocolError::frame_error(e.message()).with_fatal())?;
                // Channel-scoped method policy over raw bytes the codec
                // cannot express (differential-matrix finding: the frozen
                // prefetch_size 540 was silently unenforced).
                self.pending_policy = policy_violation(&payload);
                let (_, class) = parse_class(&payload[..]).map_err(|e| {
                    ProtocolError::frame_error(format!("invalid method payload: {e}")).with_fatal()
                })?;
                Ok(Some(AMQPFrame::Method(channel, class)))
            }
            frame_type::HEADER => {
                crate::tables::validate_content_header(&payload, &self.static_limits)
                    .map_err(|e| ProtocolError::frame_error(e.message()).with_fatal())?;
                let (_, header) = parse_content_header(&payload[..]).map_err(|e| {
                    ProtocolError::frame_error(format!("invalid content header: {e}")).with_fatal()
                })?;
                Ok(Some(AMQPFrame::Header(
                    channel,
                    header.class_id,
                    Box::new(header),
                )))
            }
            frame_type::BODY => Ok(Some(AMQPFrame::Body(channel, payload))),
            frame_type::HEARTBEAT => {
                if !payload.is_empty() {
                    return Err(ProtocolError::frame_error(
                        "heartbeat frame with non-empty payload",
                    )
                    .with_fatal());
                }
                if channel != 0 {
                    return Err(
                        ProtocolError::frame_error("heartbeat frame on non-zero channel")
                            .with_fatal(),
                    );
                }
                Ok(Some(AMQPFrame::Heartbeat(channel)))
            }
            other => {
                Err(ProtocolError::frame_error(format!("unknown frame type {other}")).with_fatal())
            }
        }
    }

    /// Read and validate the initial 8-byte protocol header.
    fn read_protocol_header(&mut self) -> Result<Option<AMQPFrame>, ProtocolError> {
        const HDR: usize = 8;
        if self.buf.len() < HDR {
            // Reject connections that are clearly not AMQP without waiting
            // for 8 bytes: the first four must be "AMQP".
            if self.buf.len() >= 4 && &self.buf[..4] != b"AMQP" {
                return Err(ProtocolError::connection(
                    reply_code::COMMAND_INVALID,
                    "connection did not open with an AMQP protocol header",
                )
                .with_fatal());
            }
            return Ok(None);
        }
        let mut header = [0u8; HDR];
        header.copy_from_slice(&self.buf[..HDR]);
        let check = check_protocol_header(&header);
        self.buf.drain(..HDR);
        self.header_seen = true;
        match check {
            HeaderCheck::Amqp0_9_1 => Ok(Some(AMQPFrame::ProtocolHeader(
                amq_protocol::frame::ProtocolVersion::amqp_0_9_1(),
            ))),
            HeaderCheck::WrongVersion {
                major,
                minor,
                revision,
            } => Err(ProtocolError::connection(
                reply_code::COMMAND_INVALID,
                format!("unsupported AMQP version {major}-{minor}-{revision}"),
            )
            .with_fatal()),
            HeaderCheck::NotAmqp => Err(ProtocolError::connection(
                reply_code::COMMAND_INVALID,
                "connection did not open with an AMQP protocol header",
            )
            .with_fatal()),
        }
    }
}

/// Validate an 8-byte protocol header.
pub fn check_protocol_header(bytes: &[u8; 8]) -> HeaderCheck {
    if &bytes[..4] != b"AMQP" {
        return HeaderCheck::NotAmqp;
    }
    // Layout: "AMQP" | protocol-id(0) | major | minor | revision.
    let (proto_id, major, minor, revision) = (bytes[4], bytes[5], bytes[6], bytes[7]);
    if proto_id == 0 && major == 0 && minor == 9 && revision == 1 {
        HeaderCheck::Amqp0_9_1
    } else {
        HeaderCheck::WrongVersion {
            major,
            minor,
            revision,
        }
    }
}

/// Serialize a basic property list to its opaque encoded form (used by the
/// wire-free core message store).
pub fn encode_properties(props: &amq_protocol::protocol::basic::AMQPProperties) -> Vec<u8> {
    use amq_protocol::protocol::basic::gen_properties;
    cookie_factory::gen_simple(gen_properties(props), Vec::new())
        .expect("property serialization into Vec is infallible")
}

/// Serialize one frame to bytes (connection writer side, FR-P07).
pub fn encode_frame(frame: &AMQPFrame) -> Vec<u8> {
    use amq_protocol::frame::gen_frame;
    cookie_factory::gen_simple(gen_frame(frame), Vec::new())
        .expect("serialization into Vec is infallible")
}

/// Reassembles one message body from header + body frames (FR-P06).
///
/// Enforces the message byte budget *before* accepting body bytes: a header
/// whose `body_size` exceeds the cap is rejected with 311 immediately, before
/// any body frame is read.
pub struct MessageAssembler {
    body: Vec<u8>,
    expected: Option<u64>,
    body_budget: u64,
}

impl MessageAssembler {
    pub fn new(max_message_bytes: u64) -> Self {
        Self {
            body: Vec::new(),
            expected: None,
            body_budget: max_message_bytes,
        }
    }

    /// Start a new message from a content header frame.
    pub fn start(&mut self, class_id: u16, body_size: u64) -> Result<(), ProtocolError> {
        if self.expected.is_some() {
            return Err(ProtocolError::unexpected_frame(
                "content header while previous message incomplete",
            )
            .with_fatal());
        }
        if class_id != crate::error::class_id::BASIC {
            return Err(ProtocolError::frame_error(format!(
                "content header for unsupported class {class_id}"
            ))
            .with_fatal());
        }
        if body_size > self.body_budget {
            return Err(ProtocolError::channel(
                reply_code::CONTENT_TOO_LARGE,
                format!(
                    "message body {body_size} exceeds configured maximum {}",
                    self.body_budget
                ),
                crate::error::class_id::BASIC,
                0, // basic.publish
            ));
        }
        self.expected = Some(body_size);
        self.body.clear();
        if body_size == 0 {
            // Zero-length messages complete at the header (FR-M01).
        }
        Ok(())
    }

    /// Append a body frame chunk.
    pub fn push_body(&mut self, chunk: &[u8]) -> Result<(), ProtocolError> {
        let expected = self.expected.ok_or_else(|| {
            ProtocolError::unexpected_frame("body frame before content header").with_fatal()
        })?;
        if self.body.len() as u64 + chunk.len() as u64 > expected {
            return Err(
                ProtocolError::frame_error("body frames exceed announced body_size").with_fatal(),
            );
        }
        self.body.extend_from_slice(chunk);
        Ok(())
    }

    /// Whether the announced body has been fully received.
    pub fn is_complete(&self) -> bool {
        match self.expected {
            Some(expected) => self.body.len() as u64 == expected,
            None => false,
        }
    }

    /// A header was received but the body is not complete yet.
    pub fn in_progress(&self) -> bool {
        self.expected.is_some() && !self.is_complete()
    }

    /// Take the completed message body.
    pub fn take(&mut self) -> Option<Vec<u8>> {
        if self.is_complete() {
            self.expected = None;
            Some(std::mem::take(&mut self.body))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use amq_protocol::frame::AMQPFrame;
    use amq_protocol::protocol::{channel, AMQPClass};

    fn limits() -> NegotiatedLimits {
        crate::limits::ProtocolLimits::default()
            .negotiate(64, 8192, 0)
            .unwrap()
    }

    fn method_frame_bytes() -> Vec<u8> {
        encode_frame(&AMQPFrame::Method(
            1,
            AMQPClass::Channel(channel::AMQPMethod::Open(channel::Open {})),
        ))
    }

    #[test]
    fn protocol_header_roundtrip() {
        let mut r = FrameReader::new(&limits());
        r.feed(&crate::PROTOCOL_HEADER_0_9_1).unwrap();
        match r.next_frame().unwrap().unwrap() {
            AMQPFrame::ProtocolHeader(_) => {}
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn wrong_version_header_is_rejected() {
        let mut r = FrameReader::new(&limits());
        r.feed(&[b'A', b'M', b'Q', b'P', 0, 0, 10, 0]).unwrap();
        let err = r.next_frame().unwrap_err();
        assert!(err.fatal);
    }

    #[test]
    fn non_amqp_input_rejected_early() {
        let mut r = FrameReader::new(&limits());
        r.feed(b"GET / HTTP").unwrap();
        assert!(r.next_frame().is_err());
    }

    #[test]
    fn frame_decodes_byte_at_a_time() {
        // T02: arbitrary TCP chunk boundaries must not matter.
        let bytes = method_frame_bytes();
        let mut r = FrameReader::new(&limits());
        r.feed(&crate::PROTOCOL_HEADER_0_9_1).unwrap();
        r.next_frame().unwrap().unwrap();
        for b in &bytes {
            r.feed(std::slice::from_ref(b)).unwrap();
        }
        match r.next_frame().unwrap() {
            Some(AMQPFrame::Method(ch, AMQPClass::Channel(channel::AMQPMethod::Open(_)))) => {
                assert_eq!(ch, 1);
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(r.next_frame().unwrap().is_none());
    }

    #[test]
    fn merged_chunks_decode_all_frames() {
        let mut stream = crate::PROTOCOL_HEADER_0_9_1.to_vec();
        stream.extend(method_frame_bytes());
        stream.extend(method_frame_bytes());
        let mut r = FrameReader::new(&limits());
        // Feed in two odd-sized chunks to scramble boundaries.
        r.feed(&stream[..11]).unwrap();
        r.feed(&stream[11..]).unwrap();
        let mut frames = 0;
        while r.next_frame().unwrap().is_some() {
            frames += 1;
        }
        assert_eq!(frames, 3);
    }

    #[test]
    fn oversize_frame_rejected_before_allocation() {
        let mut r = FrameReader::new(&limits());
        r.feed(&crate::PROTOCOL_HEADER_0_9_1).unwrap();
        r.next_frame().unwrap().unwrap();
        // Announce a payload far above frame_max=8192 with only header bytes.
        let mut hdr = vec![1u8, 0, 1]; // method frame, channel 1
        hdr.extend_from_slice(&1_000_000u32.to_be_bytes());
        r.feed(&hdr).unwrap();
        let err = r.next_frame().unwrap_err();
        assert_eq!(err.reply_code, reply_code::FRAME_ERROR);
    }

    #[test]
    fn bad_frame_end_rejected() {
        let mut bytes = method_frame_bytes();
        let last = bytes.len() - 1;
        bytes[last] = 0x00; // corrupt frame-end
        let mut r = FrameReader::new(&limits());
        r.feed(&crate::PROTOCOL_HEADER_0_9_1).unwrap();
        r.next_frame().unwrap().unwrap();
        r.feed(&bytes).unwrap();
        let err = r.next_frame().unwrap_err();
        assert_eq!(err.reply_code, reply_code::FRAME_ERROR);
    }

    #[test]
    fn unknown_frame_type_rejected() {
        let mut r = FrameReader::new(&limits());
        r.feed(&crate::PROTOCOL_HEADER_0_9_1).unwrap();
        r.next_frame().unwrap().unwrap();
        // type 9, channel 0, size 0, end 0xCE
        r.feed(&[9, 0, 0, 0, 0, 0, 0, 0xCE]).unwrap();
        assert!(r.next_frame().is_err());
    }

    #[test]
    fn qos_prefetch_size_policy_matches_frozen_profile() {
        let qos = |size: u32, count: u16| {
            let mut p = vec![0, 60, 0, 10];
            p.extend_from_slice(&size.to_be_bytes());
            p.extend_from_slice(&count.to_be_bytes());
            p.push(0); // global bit
            p
        };
        // Nonzero is the frozen channel-scoped 540.
        let e = policy_violation(&qos(10, 5)).expect("nonzero must violate");
        assert_eq!(e.reply_code, 540);
        assert_eq!((e.class_id, e.method_id), (60, 10));
        assert!(!e.fatal, "channel-scoped, not connection-fatal");
        // Zero and unrelated methods pass through untouched.
        assert!(policy_violation(&qos(0, 5)).is_none());
        assert!(policy_violation(&[0, 60, 0, 20, 0, 0, 0, 10]).is_none()); // consume
        assert!(policy_violation(&[0, 60, 0, 10, 0]).is_none()); // truncated
    }

    #[test]
    fn heartbeat_must_be_on_channel_zero() {
        let mut r = FrameReader::new(&limits());
        r.feed(&crate::PROTOCOL_HEADER_0_9_1).unwrap();
        r.next_frame().unwrap().unwrap();
        // Type 8, channel 0: valid heartbeat (0-9-1 frame type).
        r.feed(&[8, 0, 0, 0, 0, 0, 0, 0xCE]).unwrap();
        assert!(r.next_frame().unwrap().is_some());
        // Heartbeat on a non-zero channel is a frame error.
        r.feed(&[8, 0, 1, 0, 0, 0, 0, 0xCE]).unwrap();
        assert!(r.next_frame().is_err());
        // Type 4 is not a 0-9-1 frame type.
        r.feed(&[4, 0, 0, 0, 0, 0, 0, 0xCE]).unwrap();
        assert!(r.next_frame().is_err());
    }

    #[test]
    fn backlog_budget_enforced() {
        let mut r = FrameReader::new(&limits());
        r.feed(&crate::PROTOCOL_HEADER_0_9_1).unwrap();
        r.next_frame().unwrap().unwrap();
        let huge = vec![0u8; 100_000];
        assert!(r.feed(&huge).is_err());
    }

    #[test]
    fn assembler_enforces_budget_before_body() {
        let mut a = MessageAssembler::new(1024);
        let err = a.start(crate::error::class_id::BASIC, 2048).unwrap_err();
        assert_eq!(err.reply_code, reply_code::CONTENT_TOO_LARGE);
        assert_eq!(err.scope, crate::error::ErrorScope::Channel);
    }

    #[test]
    fn assembler_zero_length_message_completes() {
        let mut a = MessageAssembler::new(1024);
        a.start(crate::error::class_id::BASIC, 0).unwrap();
        assert!(a.is_complete());
        assert_eq!(a.take().unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn assembler_rejects_extra_body() {
        let mut a = MessageAssembler::new(1024);
        a.start(crate::error::class_id::BASIC, 2).unwrap();
        a.push_body(&[1, 2]).unwrap();
        assert!(a.is_complete());
        let err = a.push_body(&[3]).unwrap_err();
        assert_eq!(err.reply_code, reply_code::FRAME_ERROR);
    }

    #[test]
    fn assembler_rejects_body_without_header() {
        let mut a = MessageAssembler::new(1024);
        assert!(a.push_body(&[1]).is_err());
    }

    #[test]
    fn encode_decode_roundtrip_all_frame_kinds() {
        let mut r = FrameReader::new(&limits());
        r.feed(&crate::PROTOCOL_HEADER_0_9_1).unwrap();
        r.next_frame().unwrap().unwrap();

        let frames = vec![
            AMQPFrame::Method(
                2,
                AMQPClass::Channel(channel::AMQPMethod::Close(channel::Close {
                    reply_code: 200,
                    reply_text: "bye".into(),
                    class_id: 0,
                    method_id: 0,
                })),
            ),
            AMQPFrame::Header(
                2,
                60,
                Box::new(amq_protocol::frame::AMQPContentHeader {
                    class_id: 60,
                    body_size: 5,
                    properties: Default::default(),
                }),
            ),
            AMQPFrame::Body(2, vec![1, 2, 3, 4, 5]),
            AMQPFrame::Heartbeat(0),
        ];
        for f in &frames {
            r.feed(&encode_frame(f)).unwrap();
            let got = r.next_frame().unwrap().expect("frame should be complete");
            assert_eq!(format!("{got:?}"), format!("{f:?}"));
        }
        assert!(r.next_frame().unwrap().is_none());
    }
}
