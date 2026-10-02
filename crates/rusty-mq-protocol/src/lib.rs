//! AMQP 0-9-1 wire handling for rusty-mq.
//!
//! This crate owns everything between the TCP stream and typed AMQP method
//! calls: incremental frame decoding with negotiated-size budgets, frame
//! validation, content-body reassembly, negotiated limits, and the error
//! vocabulary used to close channels/connections with the correct reply
//! codes (see `docs/protocol-profile.md`).
//!
//! Wire *types* (field tables, method argument structs, codecs) are reused
//! from the `amq-protocol` crate per ADR-0006; the framing state machines
//! and limit enforcement are implemented here.

pub mod error;
pub mod framing;
pub mod limits;

pub use amq_protocol::{
    frame::AMQPFrame,
    protocol::{basic::AMQPProperties, AMQPClass, AMQPError},
    types::{FieldTable, ShortString},
};

pub use error::ProtocolError;
pub use framing::{encode_frame, encode_properties, FrameReader, MessageAssembler};
pub use limits::{NegotiatedLimits, ProtocolLimits};

/// AMQP 0-9-1 frame-end octet.
pub const FRAME_END: u8 = 0xCE;

/// Protocol header bytes for AMQP 0-9-1 (`AMQP\x00\x00\x09\x01`).
pub const PROTOCOL_HEADER_0_9_1: [u8; 8] = [b'A', b'M', b'Q', b'P', 0, 0, 9, 1];
