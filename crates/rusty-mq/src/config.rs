//! Configuration (§13.2): strict TOML schema mirroring the PRD example
//! exactly, environment overrides (`RUSTY_MQ__SECTION__KEY`), and
//! validation that rejects impossible combinations — unknown fields,
//! budgets that don't fit, TLS enabled with missing material, unsafe
//! remote exposure without opt-in.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub server: Server,
    #[serde(default)]
    pub amqp: Amqp,
    #[serde(default)]
    pub tls: Tls,
    #[serde(default)]
    pub storage: Storage,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub compatibility: Compatibility,
    #[serde(default)]
    pub management: Management,
    #[serde(default)]
    pub metrics: Metrics,
    #[serde(default)]
    pub logging: Logging,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Server {
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default = "default_shutdown_grace")]
    pub shutdown_grace_seconds: u32,
}

impl Default for Server {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./data")
}

fn default_shutdown_grace() -> u32 {
    30
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Amqp {
    #[serde(default = "default_amqp_listen")]
    pub listen: String,
    #[serde(default)]
    pub allow_insecure_remote: bool,
    #[serde(default = "default_frame_max")]
    pub frame_max_bytes: u32,
    #[serde(default = "default_channel_max")]
    pub channel_max: u16,
    #[serde(default = "default_heartbeat")]
    pub heartbeat_seconds: u32,
    #[serde(default = "default_handshake_timeout")]
    pub handshake_timeout_seconds: u32,
    #[serde(default = "default_assembly_timeout")]
    pub assembly_timeout_seconds: u32,
    #[serde(default = "default_max_message")]
    pub max_message_bytes: u64,
    #[serde(default = "default_max_header")]
    pub max_header_bytes: u32,
}

impl Default for Amqp {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

fn default_amqp_listen() -> String {
    "127.0.0.1:5672".into()
}
fn default_frame_max() -> u32 {
    131_072
}
fn default_channel_max() -> u16 {
    256
}
fn default_heartbeat() -> u32 {
    60
}
fn default_handshake_timeout() -> u32 {
    10
}
fn default_assembly_timeout() -> u32 {
    30
}
fn default_max_message() -> u64 {
    16 * 1024 * 1024
}
fn default_max_header() -> u32 {
    65_536
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_tls_listen")]
    pub listen: String,
    #[serde(default)]
    pub cert_file: PathBuf,
    #[serde(default)]
    pub key_file: PathBuf,
}

impl Default for Tls {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

fn default_tls_listen() -> String {
    "127.0.0.1:5671".into()
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Storage {
    #[serde(default = "default_segment_bytes")]
    pub segment_bytes: u64,
    #[serde(default = "default_commit_delay")]
    pub commit_batch_delay_ms: u32,
    #[serde(default = "default_commit_bytes")]
    pub commit_batch_bytes: u64,
    #[serde(default = "default_disk_min")]
    pub disk_free_min_bytes: u64,
    #[serde(default = "default_disk_ratio")]
    pub disk_free_min_ratio: f64,
    #[serde(default = "default_index_cache")]
    pub index_cache_bytes: u64,
}

impl Default for Storage {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

fn default_segment_bytes() -> u64 {
    268_435_456
}
fn default_commit_delay() -> u32 {
    2
}
fn default_commit_bytes() -> u64 {
    1_048_576
}
fn default_disk_min() -> u64 {
    1_073_741_824
}
fn default_disk_ratio() -> f64 {
    0.10
}
fn default_index_cache() -> u64 {
    67_108_864
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_max_queues")]
    pub max_queues_per_vhost: u32,
    #[serde(default = "default_max_bindings")]
    pub max_bindings_per_vhost: u32,
    #[serde(default = "default_max_destinations")]
    pub max_destinations_per_publish: u32,
    #[serde(default = "default_max_confirms")]
    pub max_pending_confirms_per_channel: u32,
    #[serde(default = "default_managed_buffer")]
    pub managed_buffer_bytes: u64,
    #[serde(default = "default_memory_alarm")]
    pub memory_alarm_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

fn default_max_connections() -> u32 {
    1024
}
fn default_max_queues() -> u32 {
    10_000
}
fn default_max_bindings() -> u32 {
    100_000
}
fn default_max_destinations() -> u32 {
    1024
}
fn default_max_confirms() -> u32 {
    10_000
}
fn default_managed_buffer() -> u64 {
    268_435_456
}
fn default_memory_alarm() -> u64 {
    536_870_912
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Compatibility {
    #[serde(default)]
    pub allow_transient_nonexclusive_queues: bool,
    #[serde(default = "default_reject_unknown")]
    pub reject_unknown_arguments: bool,
}

impl Default for Compatibility {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

fn default_reject_unknown() -> bool {
    true
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Management {
    #[serde(default = "default_mgmt_listen")]
    pub listen: String,
    #[serde(default = "default_remote_tls")]
    pub remote_requires_tls: bool,
    #[serde(default = "default_max_request")]
    pub max_request_bytes: u32,
    /// TLS material for the management plane (§13: remote exposure
    /// requires authenticated TLS). Both or neither.
    #[serde(default)]
    pub tls_cert: Option<String>,
    #[serde(default)]
    pub tls_key: Option<String>,
}

impl Default for Management {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

fn default_mgmt_listen() -> String {
    "127.0.0.1:15672".into()
}
fn default_remote_tls() -> bool {
    true
}
fn default_max_request() -> u32 {
    1_048_576
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Metrics {
    #[serde(default = "default_metrics_listen")]
    pub listen: String,
    #[serde(default)]
    pub queue_labels_enabled: bool,
    /// The metrics plane is unauthenticated: non-loopback binds need
    /// this explicit opt-in (§13 posture).
    #[serde(default)]
    pub allow_insecure_remote: bool,
}

impl Default for Metrics {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

fn default_metrics_listen() -> String {
    "127.0.0.1:15692".into()
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Logging {
    #[serde(default = "default_log_format")]
    pub format: String,
    #[serde(default = "default_log_level")]
    pub level: String,
}

impl Default for Logging {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

fn default_log_format() -> String {
    "json".into()
}
fn default_log_level() -> String {
    "info".into()
}

/// Load from a TOML file (missing file is an explicit error — never a
/// silent default when the operator pointed at a path).
impl Config {
    /// §13.2 -> protocol limits in force per connection.
    pub fn protocol_limits(&self) -> rusty_mq_protocol::ProtocolLimits {
        rusty_mq_protocol::ProtocolLimits {
            max_frame_max: self.amqp.frame_max_bytes,
            max_channel_max: self.amqp.channel_max,
            heartbeat_seconds: self.amqp.heartbeat_seconds,
            handshake_timeout_seconds: self.amqp.handshake_timeout_seconds,
            assembly_timeout_seconds: self.amqp.assembly_timeout_seconds,
            max_message_bytes: self.amqp.max_message_bytes,
            max_header_bytes: self.amqp.max_header_bytes,
            max_table_depth: self.limits_table_depth(),
            max_table_fields: self.limits_table_fields(),
        }
    }

    fn limits_table_depth(&self) -> u8 {
        // The frozen profile fixes table budgets; the config schema does
        // not carry them (see docs/protocol-profile.md).
        rusty_mq_protocol::ProtocolLimits::default().max_table_depth
    }

    fn limits_table_fields(&self) -> u32 {
        rusty_mq_protocol::ProtocolLimits::default().max_table_fields
    }

    /// §13.2 -> journal writer settings.
    pub fn journal_config(&self) -> rusty_mq_storage::journal::JournalConfig {
        rusty_mq_storage::journal::JournalConfig {
            segment_bytes: self.storage.segment_bytes as usize,
            commit_batch_delay_ms: self.storage.commit_batch_delay_ms,
            commit_batch_bytes: self.storage.commit_batch_bytes as usize,
            ..Default::default()
        }
    }

    /// §13.2 -> compatibility switches.
    pub fn compatibility(&self) -> rusty_mq_core::topology::CompatibilitySwitches {
        rusty_mq_core::topology::CompatibilitySwitches {
            allow_transient_nonexclusive_queues: self
                .compatibility
                .allow_transient_nonexclusive_queues,
            reject_unknown_arguments: self.compatibility.reject_unknown_arguments,
        }
    }

    /// §13.2 -> (disk_free_min_bytes, disk_free_min_ratio).
    pub fn alarm_settings(&self) -> (u64, f64) {
        (
            self.storage.disk_free_min_bytes,
            self.storage.disk_free_min_ratio,
        )
    }
}

/// Parse + validate from a string (tests; the file path is load_file).
pub fn load_str(body: &str) -> Result<Config, String> {
    let mut cfg: Config = toml::from_str(body).map_err(|e| format!("parse: {e}"))?;
    apply_env_overrides(&mut cfg)?;
    validate(&cfg, None)?;
    Ok(cfg)
}

pub fn load_file(path: &Path) -> Result<Config, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let mut cfg: Config = toml::from_str(&raw).map_err(|e| format!("{}: {e}", path.display()))?;
    apply_env_overrides(&mut cfg)?;
    validate(&cfg, Some(path))?;
    Ok(cfg)
}

/// `RUSTY_MQ__SECTION__KEY=value` overrides (documented precedence:
/// defaults → TOML → env). Only a small, explicitly supported key set —
/// anything else errors instead of being silently ignored.
fn apply_env_overrides(cfg: &mut Config) -> Result<(), String> {
    let vars: Vec<(String, String)> = std::env::vars()
        .filter(|(k, _)| k.starts_with("RUSTY_MQ__"))
        .collect();
    for (key, value) in vars {
        let parts: Vec<&str> = key.trim_start_matches("RUSTY_MQ__").split("__").collect();
        match parts.as_slice() {
            ["SERVER", "DATA_DIR"] => cfg.server.data_dir = PathBuf::from(&value),
            ["AMQP", "LISTEN"] => cfg.amqp.listen = value,
            ["AMQP", "ALLOW_INSECURE_REMOTE"] => {
                cfg.amqp.allow_insecure_remote = parse_bool(&key, &value)?;
            }
            ["TLS", "ENABLED"] => cfg.tls.enabled = parse_bool(&key, &value)?,
            ["TLS", "LISTEN"] => cfg.tls.listen = value,
            ["TLS", "CERT_FILE"] => cfg.tls.cert_file = PathBuf::from(&value),
            ["TLS", "KEY_FILE"] => cfg.tls.key_file = PathBuf::from(&value),
            ["MANAGEMENT", "LISTEN"] => cfg.management.listen = value,
            ["MANAGEMENT", "TLS_CERT"] => cfg.management.tls_cert = Some(value),
            ["MANAGEMENT", "TLS_KEY"] => cfg.management.tls_key = Some(value),
            ["METRICS", "LISTEN"] => cfg.metrics.listen = value,
            ["METRICS", "ALLOW_INSECURE_REMOTE"] => {
                cfg.metrics.allow_insecure_remote = value == "true"
            }
            ["LOGGING", "LEVEL"] => cfg.logging.level = value,
            _ => {
                return Err(format!(
                    "unknown environment override {key} (supported: SERVER__DATA_DIR, \
                     AMQP__LISTEN, AMQP__ALLOW_INSECURE_REMOTE, TLS__ENABLED/LISTEN/\
                     CERT_FILE/KEY_FILE, MANAGEMENT__LISTEN, METRICS__LISTEN, LOGGING__LEVEL)"
                ))
            }
        }
    }
    Ok(())
}

fn parse_bool(key: &str, value: &str) -> Result<bool, String> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(format!("{key}: not a boolean: {value}")),
    }
}

/// Validation (§13.2): reject impossible combinations up front.
pub fn validate(cfg: &Config, origin: Option<&Path>) -> Result<(), String> {
    let ctx = |what: &str| -> String {
        match origin {
            Some(p) => format!("{}: {what}", p.display()),
            None => what.to_string(),
        }
    };

    // Protocol floors.
    if cfg.amqp.frame_max_bytes < 4096 {
        return Err(ctx("amqp.frame_max_bytes below the protocol minimum 4096"));
    }
    if cfg.amqp.channel_max == 0 {
        return Err(ctx("amqp.channel_max must be >= 1"));
    }
    if cfg.amqp.max_header_bytes >= cfg.amqp.frame_max_bytes {
        return Err(ctx("amqp.max_header_bytes must fit inside frame_max_bytes"));
    }

    // Budgets must be sane relative to each other.
    if cfg.limits.managed_buffer_bytes >= cfg.limits.memory_alarm_bytes {
        return Err(ctx(
            "limits.managed_buffer_bytes must be below limits.memory_alarm_bytes",
        ));
    }
    if cfg.storage.disk_free_min_ratio <= 0.0 || cfg.storage.disk_free_min_ratio >= 1.0 {
        return Err(ctx("storage.disk_free_min_ratio must be in (0, 1)"));
    }

    // TLS enabled requires material that exists.
    if cfg.tls.enabled {
        if cfg.tls.cert_file.as_os_str().is_empty() || cfg.tls.key_file.as_os_str().is_empty() {
            return Err(ctx("tls.enabled requires tls.cert_file and tls.key_file"));
        }
        if !cfg.tls.cert_file.exists() {
            return Err(ctx(&format!(
                "tls.cert_file {} does not exist",
                cfg.tls.cert_file.display()
            )));
        }
        if !cfg.tls.key_file.exists() {
            return Err(ctx(&format!(
                "tls.key_file {} does not exist",
                cfg.tls.key_file.display()
            )));
        }
    }

    // Management exposed remotely requires TLS (§13; PRD line: remote
    // management must be authenticated TLS). Loopback stays plaintext.
    match (&cfg.management.tls_cert, &cfg.management.tls_key) {
        (Some(_), None) | (None, Some(_)) => {
            return Err("management.tls_cert and management.tls_key must be set together".into());
        }
        (Some(cert), Some(key)) => {
            if !std::path::Path::new(cert).is_file() {
                return Err(format!("management.tls_cert file not found: {cert}"));
            }
            if !std::path::Path::new(key).is_file() {
                return Err(format!("management.tls_key file not found: {key}"));
            }
        }
        (None, None) => {
            if cfg.management.remote_requires_tls && !is_loopback(&cfg.management.listen) {
                return Err(
                    "management.listen is not loopback and remote_requires_tls is true; \
                     configure management.tls_cert + management.tls_key"
                        .into(),
                );
            }
        }
    }
    // The metrics plane is unauthenticated Prometheus text: it binds
    // loopback by default and needs an explicit opt-in to expose
    // remotely (§12.3/§13 posture).
    if !cfg.metrics.allow_insecure_remote && !is_loopback(&cfg.metrics.listen) {
        return Err(
            "metrics.listen is not loopback; set metrics.allow_insecure_remote = true to force \
             an unauthenticated remote metrics bind"
                .into(),
        );
    }
    // Non-loopback plaintext AMQP requires the explicit insecure opt-in.
    if !cfg.amqp.allow_insecure_remote && !is_loopback(&cfg.amqp.listen) {
        return Err(ctx(
            "amqp.listen is not loopback; set amqp.allow_insecure_remote = true to force \
             plaintext exposure (production should use TLS)",
        ));
    }
    if cfg.amqp.allow_insecure_remote && is_wildcard(&cfg.amqp.listen) {
        // Allowed only with the explicit flag above; warn loudly at load.
        eprintln!("WARNING: plaintext AMQP exposed on a wildcard address; this is insecure");
    }

    if !matches!(cfg.logging.format.as_str(), "json" | "text") {
        return Err(ctx("logging.format must be 'json' or 'text'"));
    }
    if !matches!(
        cfg.logging.level.as_str(),
        "trace" | "debug" | "info" | "warn" | "error"
    ) {
        return Err(ctx("logging.level is not a recognized level"));
    }
    Ok(())
}

fn is_loopback(listen: &str) -> bool {
    let host = listen.rsplit_once(':').map(|(h, _)| h).unwrap_or(listen);
    matches!(host, "127.0.0.1" | "::1" | "[::1]" | "localhost")
}

fn is_wildcard(listen: &str) -> bool {
    let host = listen.rsplit_once(':').map(|(h, _)| h).unwrap_or(listen);
    matches!(host, "0.0.0.0" | "::" | "[::]")
}
