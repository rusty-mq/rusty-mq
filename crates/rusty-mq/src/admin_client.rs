//! Minimal HTTP/1.1 client for the CLI admin commands (§13.1): plain
//! request/response against our own management API with Basic auth. No
//! redirects, no chunked requests — responses are small JSON bodies.

use std::time::Duration;

#[derive(Debug)]
pub struct ApiResponse {
    pub status: u16,
    pub body: String,
}

impl ApiResponse {
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.body).unwrap_or(serde_json::Value::Null)
    }
}

/// Credentials for the admin CLI: never logged; the password comes from
/// the explicit flag or the documented environment variable.
pub struct AdminCredentials {
    pub user: String,
    pub password: String,
}

pub fn env_password() -> Option<String> {
    std::env::var("RUSTY_MQ_ADMIN_PASSWORD")
        .ok()
        .filter(|p| !p.is_empty())
}

pub async fn request(
    base_url: &str,
    method: &str,
    path: &str,
    creds: &AdminCredentials,
    body: Option<&serde_json::Value>,
) -> Result<ApiResponse, String> {
    use base64::Engine;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let auth = base64::engine::general_purpose::STANDARD
        .encode(format!("{}:{}", creds.user, creds.password));
    let payload = body.map(|b| b.to_string());
    let host = base_url.trim_start_matches("http://").to_string();
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\nAuthorization: Basic {auth}\r\nConnection: close\r\n"
    );
    if let Some(p) = &payload {
        req.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            p.len()
        ));
    }
    req.push_str("\r\n");
    if let Some(p) = &payload {
        req.push_str(p);
    }

    let mut stream = tokio::net::TcpStream::connect(&host)
        .await
        .map_err(|e| format!("connect {host}: {e}"))?;
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| format!("write: {e}"))?;
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut response))
        .await
        .map_err(|_| "request timed out".to_string())?
        .map_err(|e| format!("read: {e}"))?;
    let text = String::from_utf8_lossy(&response);
    parse_response(&text).ok_or_else(|| "malformed HTTP response".to_string())
}

fn parse_response(raw: &str) -> Option<ApiResponse> {
    let (head, body) = raw.split_once("\r\n\r\n")?;
    let status_line = head.lines().next()?;
    let status: u16 = status_line.split_whitespace().nth(1)?.parse().ok()?;
    // Our API never uses chunked encoding; read the body as-is (the
    // connection closes thanks to Connection: close).
    Some(ApiResponse {
        status,
        body: body.to_string(),
    })
}
