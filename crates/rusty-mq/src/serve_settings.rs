//! Resolution of effective serve settings: explicit flag > config file
//! (§13.2) > builtin default, for every listener surface plus logging.
//! Pure and unit-tested; `main` maps clap args onto [`ServeFlags`] and
//! consumes the result.

use std::net::SocketAddr;
use std::path::PathBuf;

/// The CLI flag surface of `serve` (Option everywhere: a Some is an
/// EXPLICIT flag; defaults resolve here, not in clap — a clap
/// default_value would silently clobber file values).
#[derive(Default)]
pub struct ServeFlags {
    pub listen: Option<SocketAddr>,
    pub data_dir: Option<PathBuf>,
    pub metrics_listen: Option<String>,
    pub management_listen: Option<SocketAddr>,
    pub management_tls_cert: Option<PathBuf>,
    pub management_tls_key: Option<PathBuf>,
    pub tls_listen: Option<SocketAddr>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
}

/// Effective settings after precedence resolution.
#[derive(Debug)]
pub struct ServeSettings {
    pub listen: SocketAddr,
    pub data_dir: Option<PathBuf>,
    /// None = metrics listener disabled.
    pub metrics: Option<String>,
    /// None = management plane disabled.
    pub management: Option<SocketAddr>,
    /// Management TLS material (cert, key).
    pub management_tls: Option<(PathBuf, PathBuf)>,
    /// AMQP TLS listener (addr, cert, key).
    pub tls: Option<(SocketAddr, PathBuf, PathBuf)>,
    /// Log level for the subscriber (RUST_LOG still wins — see main).
    pub log_level: String,
}

const DEFAULT_LISTEN: &str = "127.0.0.1:5672";
const DEFAULT_METRICS: &str = "127.0.0.1:15692";
const DEFAULT_LOG_LEVEL: &str = "info";

fn parse_addr(s: &str, what: &str) -> SocketAddr {
    s.parse()
        .unwrap_or_else(|e| panic!("invalid {what} address {s:?}: {e}"))
}

/// Resolve effective settings. Invalid file addresses fail loudly here
/// rather than halfway through startup.
pub fn resolve(flags: ServeFlags, file: Option<&crate::config::Config>) -> ServeSettings {
    let cfg = file;

    let listen = flags
        .listen
        .or_else(|| cfg.and_then(|c| c.amqp.listen.parse().ok()))
        .unwrap_or_else(|| parse_addr(DEFAULT_LISTEN, "listen"));

    let data_dir = flags
        .data_dir
        .or_else(|| cfg.map(|c| c.server.data_dir.clone()));

    let metrics = flags
        .metrics_listen
        .or_else(|| cfg.map(|c| c.metrics.listen.clone()))
        .or_else(|| Some(DEFAULT_METRICS.into()));

    let management = flags
        .management_listen
        .or_else(|| cfg.and_then(|c| c.management.listen.parse().ok()));
    let management_tls = match (flags.management_tls_cert, flags.management_tls_key) {
        (Some(c), Some(k)) => Some((c, k)),
        (None, None) => cfg.and_then(|c| match (&c.management.tls_cert, &c.management.tls_key) {
            (Some(c), Some(k)) => Some((PathBuf::from(c), PathBuf::from(k))),
            _ => None,
        }),
        // Half-specified flags are a CLI usage error; refuse loudly.
        (c, k) => {
            panic!("management TLS material must be both cert and key (cert={c:?} key={k:?})")
        }
    };

    let tls = match (flags.tls_listen, flags.tls_cert, flags.tls_key) {
        (Some(a), Some(c), Some(k)) => Some((a, c, k)),
        (None, None, None) => cfg.and_then(|c| {
            if !c.tls.enabled {
                return None;
            }
            Some((
                parse_addr(&c.tls.listen, "tls.listen"),
                c.tls.cert_file.clone(),
                c.tls.key_file.clone(),
            ))
        }),
        (a, c, k) => panic!(
            "TLS listener needs listen+cert+key together (listen={a:?} cert={c:?} key={k:?})"
        ),
    };

    let log_level = cfg
        .map(|c| c.logging.level.clone())
        .unwrap_or_else(|| DEFAULT_LOG_LEVEL.into());

    ServeSettings {
        listen,
        data_dir,
        metrics,
        management,
        management_tls,
        tls,
        log_level,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(body: &str) -> crate::config::Config {
        crate::config::load_str(body).expect("test config must parse")
    }

    #[test]
    fn defaults_without_file_or_flags() {
        let s = resolve(ServeFlags::default(), None);
        assert_eq!(s.listen, DEFAULT_LISTEN.parse::<SocketAddr>().unwrap());
        assert!(s.data_dir.is_none());
        assert_eq!(s.metrics.as_deref(), Some(DEFAULT_METRICS));
        assert!(s.management.is_none());
        assert!(s.tls.is_none());
        assert_eq!(s.log_level, "info");
    }

    #[test]
    fn file_drives_every_surface() {
        let dir = std::env::temp_dir().join(format!("rmq-ss-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (c, k) = (dir.join("c.pem"), dir.join("k.pem"));
        std::fs::write(&c, "x").unwrap();
        std::fs::write(&k, "x").unwrap();
        let cfg = config(&format!(
            "[server]\ndata_dir = \"{}\"\n[amqp]\nlisten = \"127.0.0.1:5673\"\n\
             [tls]\nenabled = true\nlisten = \"127.0.0.1:5671\"\ncert_file = \"{}\"\nkey_file = \"{}\"\n\
             [management]\nlisten = \"127.0.0.1:15673\"\ntls_cert = \"{}\"\ntls_key = \"{}\"\n\
             [metrics]\nlisten = \"127.0.0.1:15693\"\n[logging]\nlevel = \"debug\"\n",
            dir.display(),
            c.display(),
            k.display(),
            c.display(),
            k.display(),
        ));
        let s = resolve(ServeFlags::default(), Some(&cfg));
        assert_eq!(s.listen, "127.0.0.1:5673".parse().unwrap());
        assert_eq!(s.data_dir.as_deref(), Some(dir.as_path()));
        assert_eq!(s.metrics.as_deref(), Some("127.0.0.1:15693"));
        assert_eq!(s.management, Some("127.0.0.1:15673".parse().unwrap()));
        assert_eq!(s.management_tls, Some((c.clone(), k.clone())));
        let (addr, cert, key) = s.tls.expect("file tls");
        assert_eq!(addr, "127.0.0.1:5671".parse().unwrap());
        assert_eq!((cert, key), (c, k));
        assert_eq!(s.log_level, "debug");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn flags_beat_file() {
        let cfg = config(
            "[amqp]\nlisten = \"127.0.0.1:5673\"\n[metrics]\nlisten = \"127.0.0.1:15693\"\n",
        );
        let s = resolve(
            ServeFlags {
                listen: Some("127.0.0.1:9999".parse().unwrap()),
                metrics_listen: Some("disabled".into()),
                ..Default::default()
            },
            Some(&cfg),
        );
        assert_eq!(s.listen, "127.0.0.1:9999".parse().unwrap());
        assert_eq!(s.metrics.as_deref(), Some("disabled")); // flag wins
    }

    #[test]
    fn tls_file_section_off_by_default() {
        let s = resolve(
            ServeFlags::default(),
            Some(&crate::config::Config::default()),
        );
        assert!(s.tls.is_none(), "tls.enabled defaults false");
    }
}
