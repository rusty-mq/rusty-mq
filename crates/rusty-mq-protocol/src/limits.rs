//! Negotiated and static protocol limits (PRD §5.1, docs/protocol-profile.md).
//!
//! Every limit is validated *before* allocating from an untrusted size field.
//! A peer-supplied zero that means "unlimited" never removes server resource
//! protection: the server-side ceiling always applies.

/// Static, non-negotiable resource ceilings for one connection.
///
/// These are rusty-mq defaults (not claims about other brokers) and are
/// configuration surface, not protocol constants.
#[derive(Clone, Debug)]
pub struct ProtocolLimits {
    /// Maximum `frame_max` the server will negotiate (inclusive of framing).
    pub max_frame_max: u32,
    /// Maximum `channel_max` the server will negotiate (excluding channel 0).
    pub max_channel_max: u16,
    /// Heartbeat timeout proposed by the server, in seconds (0 = disabled).
    pub heartbeat_seconds: u32,
    /// Handshake timeout: protocol header to `connection.open-ok`, in seconds.
    pub handshake_timeout_seconds: u32,
    /// Content assembly timeout: header frame to last body frame, in seconds.
    pub assembly_timeout_seconds: u32,
    /// Maximum total message body size in bytes.
    pub max_message_bytes: u64,
    /// Maximum encoded properties/headers budget in bytes.
    pub max_header_bytes: u32,
    /// Maximum field table/array nesting depth.
    pub max_table_depth: u8,
    /// Maximum field count per table/array container.
    pub max_table_fields: u32,
}

impl Default for ProtocolLimits {
    fn default() -> Self {
        Self {
            max_frame_max: 131_072,
            max_channel_max: 256,
            heartbeat_seconds: 60,
            handshake_timeout_seconds: 10,
            assembly_timeout_seconds: 30,
            max_message_bytes: 16 * 1024 * 1024,
            max_header_bytes: 64 * 1024,
            max_table_depth: 16,
            max_table_fields: 1_024,
        }
    }
}

impl ProtocolLimits {
    /// Validate raw `tune` values from a client's `connection.tune-ok` and
    /// produce the limits actually in force afterwards.
    ///
    /// Per AMQP negotiation rules the *client* picks the values it will use,
    /// but it may not exceed the server's proposal: a client value above the
    /// server proposal is a protocol violation (FRAME_ERROR), and a value of
    /// zero means "no opinion", falling back to the server proposal.
    pub fn negotiate(
        &self,
        client_channel_max: u16,
        client_frame_max: u32,
        client_heartbeat: u16,
    ) -> Result<NegotiatedLimits, NegotiationError> {
        let channel_max = if client_channel_max == 0 {
            self.max_channel_max
        } else if client_channel_max > self.max_channel_max {
            return Err(NegotiationError::ChannelMaxTooHigh {
                client: client_channel_max,
                server: self.max_channel_max,
            });
        } else {
            client_channel_max
        };

        let frame_max = if client_frame_max == 0 {
            self.max_frame_max
        } else if client_frame_max > self.max_frame_max {
            return Err(NegotiationError::FrameMaxTooHigh {
                client: client_frame_max,
                server: self.max_frame_max,
            });
        } else {
            client_frame_max
        };
        if frame_max < FRAME_MIN {
            return Err(NegotiationError::FrameMaxTooLow { client: frame_max });
        }

        // Heartbeat: min(client, server); 0 on either side disables.
        let heartbeat = client_heartbeat.min(self.heartbeat_seconds as u16);

        Ok(NegotiatedLimits {
            channel_max,
            frame_max,
            heartbeat_seconds: heartbeat,
            static_limits: self.clone(),
        })
    }
}

/// Protocol minimum frame size (AMQP 0-9-1 spec: 4096 octets).
pub const FRAME_MIN: u32 = 4096;

/// An invalid client-proposed value in `connection.tune-ok`.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum NegotiationError {
    #[error("client channel_max {client} exceeds server proposal {server}")]
    ChannelMaxTooHigh { client: u16, server: u16 },
    #[error("client frame_max {client} exceeds server proposal {server}")]
    FrameMaxTooHigh { client: u32, server: u32 },
    #[error("client frame_max {client} below protocol minimum {FRAME_MIN}")]
    FrameMaxTooLow { client: u32 },
}

/// Limits in force after a successful `tune`/`tune-ok` exchange.
#[derive(Clone, Debug)]
pub struct NegotiatedLimits {
    /// Negotiated `channel_max` (excluding channel 0); 0 is not possible here.
    pub channel_max: u16,
    /// Negotiated `frame_max`, inclusive of framing overhead.
    pub frame_max: u32,
    /// Negotiated heartbeat interval in seconds; 0 = heartbeats disabled.
    pub heartbeat_seconds: u16,
    /// Static ceilings that were not negotiated.
    pub static_limits: ProtocolLimits,
}

impl NegotiatedLimits {
    /// Validate a channel id used by an incoming frame.
    pub fn validate_channel(&self, channel: u16) -> bool {
        channel != 0 && channel <= self.channel_max
    }

    /// Largest payload length a frame may carry under `frame_max`
    /// (frame overhead is 8 bytes: type+channel+size+frame-end).
    pub fn max_frame_payload(&self) -> u32 {
        self.frame_max - 8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_means_server_default() {
        let l = ProtocolLimits::default();
        let n = l.negotiate(0, 0, 0).unwrap();
        assert_eq!(n.channel_max, 256);
        assert_eq!(n.frame_max, 131_072);
        assert_eq!(n.heartbeat_seconds, 0); // client disables -> disabled
    }

    #[test]
    fn min_heartbeat_wins() {
        let l = ProtocolLimits::default();
        let n = l.negotiate(10, 32 * 1024, 30).unwrap();
        assert_eq!(n.heartbeat_seconds, 30);
    }

    #[test]
    fn client_above_server_proposal_is_rejected() {
        let l = ProtocolLimits::default();
        assert!(matches!(
            l.negotiate(512, 4096, 0),
            Err(NegotiationError::ChannelMaxTooHigh { .. })
        ));
        assert!(matches!(
            l.negotiate(10, 262_144, 0),
            Err(NegotiationError::FrameMaxTooHigh { .. })
        ));
    }

    #[test]
    fn below_protocol_minimum_is_rejected() {
        let l = ProtocolLimits {
            max_frame_max: 1024,
            ..Default::default()
        };
        assert!(matches!(
            l.negotiate(10, 0, 0),
            Err(NegotiationError::FrameMaxTooLow { .. })
        ));
    }

    #[test]
    fn validate_channel_rejects_zero_and_overflow() {
        let n = ProtocolLimits::default().negotiate(16, 4096, 0).unwrap();
        assert!(!n.validate_channel(0));
        assert!(n.validate_channel(1));
        assert!(n.validate_channel(16));
        assert!(!n.validate_channel(17));
    }
}
