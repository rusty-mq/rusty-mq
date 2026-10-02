//! Error vocabulary mapped to AMQP close replies.
//!
//! AMQP 0-9-1 has two error scopes: channel errors (`channel.close` carrying
//! class/method ids of the offending operation) and connection errors
//! (`connection.close`). This type is the single source of the reply codes
//! documented in `docs/protocol-profile.md`.

use amq_protocol::types::ShortString;

/// AMQP reply codes used by rusty-mq (subset; see protocol profile).
pub mod reply_code {
    pub const CONTENT_TOO_LARGE: u16 = 311;
    pub const NO_ROUTE: u16 = 312;
    pub const ACCESS_REFUSED: u16 = 403;
    pub const NOT_FOUND: u16 = 404;
    pub const RESOURCE_LOCKED: u16 = 405;
    pub const PRECONDITION_FAILED: u16 = 406;
    pub const FRAME_ERROR: u16 = 501;
    pub const SYNTAX_ERROR: u16 = 502;
    pub const COMMAND_INVALID: u16 = 503;
    pub const CHANNEL_ERROR: u16 = 504;
    pub const UNEXPECTED_FRAME: u16 = 505;
    pub const RESOURCE_ERROR: u16 = 506;
    pub const NOT_ALLOWED: u16 = 530;
    pub const NOT_IMPLEMENTED: u16 = 540;
    pub const INTERNAL_ERROR: u16 = 541;
}

/// Class ids (AMQP 0-9-1).
pub mod class_id {
    pub const CONNECTION: u16 = 10;
    pub const CHANNEL: u16 = 20;
    pub const EXCHANGE: u16 = 40;
    pub const QUEUE: u16 = 50;
    pub const BASIC: u16 = 60;
    pub const CONFIRM: u16 = 85;
}

/// Where the error must be reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorScope {
    /// Close only the offending channel.
    Channel,
    /// Close the whole connection.
    Connection,
}

/// An error carrying the exact AMQP close reply to emit.
#[derive(Clone, Debug, thiserror::Error)]
#[error("{scope:?} error {reply_code}: {reply_text}")]
pub struct ProtocolError {
    pub scope: ErrorScope,
    pub reply_code: u16,
    pub reply_text: String,
    /// Offending class id, where the protocol requires it in the close.
    pub class_id: u16,
    /// Offending method id, where the protocol requires it in the close.
    pub method_id: u16,
    /// True when this error is fatal for the connection regardless of scope
    /// (e.g. could not serialize the close itself).
    pub fatal: bool,
}

impl ProtocolError {
    /// Channel-scoped error with offending method identifiers.
    pub fn channel(
        reply_code: u16,
        reply_text: impl Into<String>,
        class_id: u16,
        method_id: u16,
    ) -> Self {
        Self {
            scope: ErrorScope::Channel,
            reply_code,
            reply_text: reply_text.into(),
            class_id,
            method_id,
            fatal: false,
        }
    }

    /// Connection-scoped error.
    pub fn connection(reply_code: u16, reply_text: impl Into<String>) -> Self {
        Self {
            scope: ErrorScope::Connection,
            reply_code,
            reply_text: reply_text.into(),
            class_id: 0,
            method_id: 0,
            fatal: false,
        }
    }

    /// Connection-scoped error with offending method identifiers.
    pub fn connection_with_method(
        reply_code: u16,
        reply_text: impl Into<String>,
        class_id: u16,
        method_id: u16,
    ) -> Self {
        Self {
            scope: ErrorScope::Connection,
            reply_code,
            reply_text: reply_text.into(),
            class_id,
            method_id,
            fatal: false,
        }
    }

    /// Mark the error as fatal (connection must terminate after replying).
    pub fn with_fatal(mut self) -> Self {
        self.fatal = true;
        self
    }

    pub fn reply_text(&self) -> ShortString {
        self.reply_text.clone().into()
    }
}

/// Convenience constructors matching the frozen error profile.
impl ProtocolError {
    pub fn not_implemented(what: impl std::fmt::Display, class_id: u16, method_id: u16) -> Self {
        Self::channel(
            reply_code::NOT_IMPLEMENTED,
            format!("NOT_IMPLEMENTED - {what} is not supported by rusty-mq"),
            class_id,
            method_id,
        )
    }

    pub fn precondition_failed(
        what: impl std::fmt::Display,
        class_id: u16,
        method_id: u16,
    ) -> Self {
        Self::channel(
            reply_code::PRECONDITION_FAILED,
            format!("PRECONDITION_FAILED - {what}"),
            class_id,
            method_id,
        )
    }

    pub fn not_found(what: impl std::fmt::Display, class_id: u16, method_id: u16) -> Self {
        Self::channel(
            reply_code::NOT_FOUND,
            format!("NOT_FOUND - {what}"),
            class_id,
            method_id,
        )
    }

    pub fn frame_error(what: impl std::fmt::Display) -> Self {
        Self::connection(reply_code::FRAME_ERROR, format!("FRAME_ERROR - {what}"))
    }

    pub fn command_invalid(what: impl std::fmt::Display) -> Self {
        Self::connection(
            reply_code::COMMAND_INVALID,
            format!("COMMAND_INVALID - {what}"),
        )
    }

    pub fn unexpected_frame(what: impl std::fmt::Display) -> Self {
        Self::connection(
            reply_code::UNEXPECTED_FRAME,
            format!("UNEXPECTED_FRAME - {what}"),
        )
    }
}
