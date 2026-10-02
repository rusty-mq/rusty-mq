//! Native HTTP management API for rusty-mq (M7).
//!
//! **Not implemented yet.** V1 exposes `/v1/...` endpoints only; the
//! RabbitMQ `/api/...` compatibility surface is a later, separately tested
//! release (see `compatibility/features.yaml`). The endpoint contract is
//! specified in PRD §12.1 and will be captured in `openapi.yaml` at M7.

/// Marker: management API is not part of the M1 development binary.
pub const NOT_IMPLEMENTED_UNTIL_M7: &str = "management API lands in M7";
