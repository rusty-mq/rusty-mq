//! Native HTTP management API (§12.1, V1 `/v1/...` surface).
//!
//! Authentication: HTTP Basic against the broker's principal store
//! (Argon2id verify, the same path SASL PLAIN uses). Roles gate the
//! surface per FR-S04: reads need Monitor+, mutations need Admin.
//! Health endpoints are unauthenticated probes (no sensitive detail).
//!
//! IDs in URLs: V1 uses the vhost name, username, and queue name as the
//! opaque identifiers (the only identities the pre-management broker
//! mints); a dedicated opaque-id registry arrives with the full admin
//! model. Names are echoed as fields in bodies.

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use rusty_mq_core::auth::{Permissions, Role};
use rusty_mq_core::topology::TopologyError;

use crate::broker_facade::BrokerHandle;

pub mod broker_facade;

/// The minimum role an endpoint demands.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MinRole {
    None,
    Monitor,
    Admin,
}

/// Consistent error envelope (§12.1): stable code + message.
fn error_body(code: &str, message: impl Into<String>) -> Json<serde_json::Value> {
    Json(json!({ "error": { "code": code, "message": message.into() } }))
}

fn status(code: StatusCode, code_str: &str, message: impl Into<String>) -> Response {
    (code, error_body(code_str, message)).into_response()
}

/// Build the management router over a broker handle.
pub fn router<B: crate::broker_facade::BrokerHandle>(broker: B) -> Router {
    Router::new()
        .route("/health/live", get(health_live))
        .route("/health/ready", get(health_ready))
        .route("/metrics", get(metrics_text))
        .route("/v1/capabilities", get(capabilities))
        .route("/v1/status", get(status_ep))
        .route("/v1/vhosts", get(list_vhosts).post(create_vhost))
        .route("/v1/vhosts/{vhost}/queues", get(list_queues))
        .route("/v1/vhosts/{vhost}/queues/{queue}/purge", post(purge_queue))
        .route("/v1/vhosts/{vhost}/queues/{queue}", delete(delete_queue))
        .route("/v1/users", get(list_users).post(create_user))
        .route("/v1/users/{username}/credentials", put(rotate_credentials))
        .route("/v1/users/{username}", delete(delete_user))
        .route(
            "/v1/permissions/{username}/{vhost}",
            get(get_permissions)
                .put(put_permissions)
                .delete(delete_permissions),
        )
        .with_state(std::sync::Arc::new(broker))
}

// ---------------------------------------------------------------------
// Auth middleware (inline: each handler declares its floor).
// ---------------------------------------------------------------------

#[allow(clippy::result_large_err)] // Response is the natural error unit
fn authenticate<B: crate::broker_facade::BrokerHandle>(
    broker: &B,
    headers: &HeaderMap,
) -> Result<Role, Response> {
    let encoded = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Basic "))
        .ok_or_else(|| {
            status(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "missing Basic credentials",
            )
        })?;
    let decoded = base64_decode(encoded).ok_or_else(|| {
        status(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "malformed Basic credentials",
        )
    })?;
    let decoded = String::from_utf8(decoded).map_err(|_| {
        status(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "credentials not UTF-8",
        )
    })?;
    let (user, pass) = decoded.split_once(':').ok_or_else(|| {
        status(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "malformed Basic credentials",
        )
    })?;
    if !broker.authenticate(user, pass) {
        return Err(status(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "invalid credentials",
        ));
    }
    let role = broker.role_of(user).unwrap_or(Role::Ordinary);
    Ok(role)
}

#[allow(clippy::result_large_err)]
fn require_role<B: crate::broker_facade::BrokerHandle>(
    broker: &B,
    headers: &HeaderMap,
    min: MinRole,
) -> Result<(), Response> {
    let role = authenticate(broker, headers)?;
    let ok = match min {
        MinRole::None => true,
        MinRole::Monitor => role >= Role::Monitor,
        MinRole::Admin => role >= Role::Admin,
    };
    if ok {
        Ok(())
    } else {
        Err(status(
            StatusCode::FORBIDDEN,
            "forbidden",
            "insufficient role for this operation",
        ))
    }
}

fn base64_decode(input: &str) -> Option<Vec<u8>> {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::with_capacity(input.len() / 4 * 3);
    let mut acc = 0u32;
    let mut bits = 0u32;
    for c in input.bytes() {
        if c == b'=' || c == b'\n' || c == b'\r' {
            continue;
        }
        let v = TABLE.iter().position(|&t| t == c)? as u32;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

// ---------------------------------------------------------------------
// Handlers.
// ---------------------------------------------------------------------

async fn health_live() -> impl IntoResponse {
    Json(json!({ "status": "live" }))
}

async fn health_ready<B: crate::broker_facade::BrokerHandle>(
    State(broker): State<std::sync::Arc<B>>,
) -> impl IntoResponse {
    // Ready = recovery complete (the broker only serves after open) and
    // admissions available (no disk alarm wired yet; alarms land with
    // FR-R04). Honest single-bit answer today.
    if broker.is_ready() {
        Json(json!({ "status": "ready" })).into_response()
    } else {
        status(
            StatusCode::SERVICE_UNAVAILABLE,
            "not_ready",
            "recovery incomplete",
        )
    }
}

async fn metrics_text<B: crate::broker_facade::BrokerHandle>(
    State(broker): State<std::sync::Arc<B>>,
) -> Response {
    let text = broker.render_metrics();
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        text,
    )
        .into_response()
}

async fn capabilities() -> impl IntoResponse {
    Json(json!({
        "product": "rusty-mq",
        "api": "/v1",
        "amqp": { "0-9-1": true },
        "features": {
            "publisher_confirms": true,
            "basic_nack": true,
            "consumer_cancel_notify": true,
            "queue_ttl": false,
            "dead_letter_exchange": false,
            "quorum_queues": false,
            "transactions": false
        }
    }))
}

async fn status_ep<B: crate::broker_facade::BrokerHandle>(
    State(broker): State<std::sync::Arc<B>>,
    headers: HeaderMap,
) -> Response {
    if let Err(e) = require_role(broker.as_ref(), &headers, MinRole::Monitor) {
        return e;
    }
    Json(broker.status_summary()).into_response()
}

async fn list_vhosts<B: crate::broker_facade::BrokerHandle>(
    State(broker): State<std::sync::Arc<B>>,
    headers: HeaderMap,
) -> Response {
    if let Err(e) = require_role(broker.as_ref(), &headers, MinRole::Monitor) {
        return e;
    }
    Json(json!({ "vhosts": broker.list_vhosts() })).into_response()
}

async fn create_vhost<B: crate::broker_facade::BrokerHandle>(
    State(broker): State<std::sync::Arc<B>>,
    headers: HeaderMap,
    body: Option<Json<serde_json::Value>>,
) -> Response {
    if let Err(e) = require_role(broker.as_ref(), &headers, MinRole::Admin) {
        return e;
    }
    // Destructive-operation posture: creating a vhost redefines the
    // permission surface; V1 wires durable vhosts with the topology
    // journal record — until the record kind exists, refuse honestly.
    let _ = body;
    status(
        StatusCode::NOT_IMPLEMENTED,
        "not_implemented",
        "durable vhost creation lands with the topology vhost record",
    )
}

async fn list_queues<B: crate::broker_facade::BrokerHandle>(
    State(broker): State<std::sync::Arc<B>>,
    Path(vhost): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(e) = require_role(broker.as_ref(), &headers, MinRole::Monitor) {
        return e;
    }
    Json(json!({ "queues": broker.list_queues(&vhost) })).into_response()
}

async fn purge_queue<B: crate::broker_facade::BrokerHandle>(
    State(broker): State<std::sync::Arc<B>>,
    Path((vhost, queue)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Err(e) = require_role(broker.as_ref(), &headers, MinRole::Admin) {
        return e;
    }
    match broker.purge_queue(&vhost, &queue) {
        Ok(n) => Json(json!({ "purged": n })).into_response(),
        Err(e) => map_topology_error(e),
    }
}

async fn delete_queue<B: crate::broker_facade::BrokerHandle>(
    State(broker): State<std::sync::Arc<B>>,
    Path((vhost, queue)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Err(e) = require_role(broker.as_ref(), &headers, MinRole::Admin) {
        return e;
    }
    match broker.delete_queue(&vhost, &queue) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => map_topology_error(e),
    }
}

#[derive(Deserialize)]
struct CreateUserBody {
    username: String,
    password: String,
    #[serde(default = "default_role")]
    role: String,
}

fn default_role() -> String {
    "ordinary".into()
}

async fn create_user<B: crate::broker_facade::BrokerHandle>(
    State(broker): State<std::sync::Arc<B>>,
    headers: HeaderMap,
    Json(body): Json<CreateUserBody>,
) -> Response {
    if let Err(e) = require_role(broker.as_ref(), &headers, MinRole::Admin) {
        return e;
    }
    let role = match body.role.as_str() {
        "ordinary" => Role::Ordinary,
        "monitor" => Role::Monitor,
        "operator" => Role::Operator,
        "admin" => Role::Admin,
        other => {
            return status(
                StatusCode::BAD_REQUEST,
                "bad_request",
                format!("unknown role '{other}'"),
            )
        }
    };
    if body.username.is_empty() || body.password.is_empty() {
        return status(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "username and password are required",
        );
    }
    match broker.create_user(&body.username, &body.password, role) {
        Ok(()) => (
            StatusCode::CREATED,
            Json(json!({ "username": body.username, "role": body.role })),
        )
            .into_response(),
        Err(e) => status(StatusCode::INTERNAL_SERVER_ERROR, "internal", e),
    }
}

async fn list_users<B: crate::broker_facade::BrokerHandle>(
    State(broker): State<std::sync::Arc<B>>,
    headers: HeaderMap,
) -> Response {
    if let Err(e) = require_role(broker.as_ref(), &headers, MinRole::Admin) {
        return e;
    }
    Json(json!({ "users": broker.list_users() })).into_response()
}

#[derive(Deserialize)]
struct CredentialsBody {
    password: String,
}

async fn rotate_credentials<B: crate::broker_facade::BrokerHandle>(
    State(broker): State<std::sync::Arc<B>>,
    Path(username): Path<String>,
    headers: HeaderMap,
    Json(body): Json<CredentialsBody>,
) -> Response {
    if let Err(e) = require_role(broker.as_ref(), &headers, MinRole::Admin) {
        return e;
    }
    match broker.rotate_credentials(&username, &body.password) {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => status(StatusCode::NOT_FOUND, "not_found", "no such user"),
        Err(e) => status(StatusCode::INTERNAL_SERVER_ERROR, "internal", e),
    }
}

async fn delete_user<B: crate::broker_facade::BrokerHandle>(
    State(broker): State<std::sync::Arc<B>>,
    Path(username): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(e) = require_role(broker.as_ref(), &headers, MinRole::Admin) {
        return e;
    }
    match broker.delete_user(&username) {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => status(StatusCode::NOT_FOUND, "not_found", "no such user"),
        Err(e) => status(StatusCode::INTERNAL_SERVER_ERROR, "internal", e),
    }
}

#[derive(Serialize, Deserialize)]
struct PermissionsBody {
    configure: String,
    write: String,
    read: String,
}

async fn get_permissions<B: crate::broker_facade::BrokerHandle>(
    State(broker): State<std::sync::Arc<B>>,
    Path((username, vhost)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Err(e) = require_role(broker.as_ref(), &headers, MinRole::Admin) {
        return e;
    }
    match broker.get_permissions(&username, &vhost) {
        Some(p) => Json(PermissionsBody {
            configure: p.configure,
            write: p.write,
            read: p.read,
        })
        .into_response(),
        None => status(
            StatusCode::NOT_FOUND,
            "not_found",
            "no permissions for that user/vhost",
        ),
    }
}

async fn put_permissions<B: crate::broker_facade::BrokerHandle>(
    State(broker): State<std::sync::Arc<B>>,
    Path((username, vhost)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<PermissionsBody>,
) -> Response {
    if let Err(e) = require_role(broker.as_ref(), &headers, MinRole::Admin) {
        return e;
    }
    // Validate patterns up front (fail closed on bad regex later is not
    // good enough for a management API: refuse to store invalid patterns).
    for (label, pattern) in [
        ("configure", &body.configure),
        ("write", &body.write),
        ("read", &body.read),
    ] {
        if regex_lite_valid(pattern).is_none() {
            return status(
                StatusCode::BAD_REQUEST,
                "bad_request",
                format!("invalid {label} regex"),
            );
        }
    }
    let perms = Permissions {
        configure: body.configure,
        write: body.write,
        read: body.read,
    };
    match broker.set_permissions(&username, &vhost, perms) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => status(StatusCode::INTERNAL_SERVER_ERROR, "internal", e),
    }
}

async fn delete_permissions<B: crate::broker_facade::BrokerHandle>(
    State(broker): State<std::sync::Arc<B>>,
    Path((username, vhost)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Err(e) = require_role(broker.as_ref(), &headers, MinRole::Admin) {
        return e;
    }
    match broker.delete_permissions(&username, &vhost) {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => status(
            StatusCode::NOT_FOUND,
            "not_found",
            "no permissions for that user/vhost",
        ),
        Err(e) => status(StatusCode::INTERNAL_SERVER_ERROR, "internal", e),
    }
}

fn map_topology_error(e: TopologyError) -> Response {
    match e {
        TopologyError::QueueNotFound(n) => status(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("no queue '{n}'"),
        ),
        TopologyError::VhostNotFound => status(StatusCode::NOT_FOUND, "not_found", "no such vhost"),
        other => status(StatusCode::CONFLICT, "conflict", other.to_string()),
    }
}

/// Validate a regex using the same engine core uses at check time.
fn regex_lite_valid(pattern: &str) -> Option<()> {
    regex::Regex::new(pattern).ok().map(|_| ()).or(None)
}
