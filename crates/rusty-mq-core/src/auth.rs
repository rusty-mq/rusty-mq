//! Authentication and authorization state (§11).
//!
//! Principals (Argon2id PHC password hashes) and per-vhost
//! configure/write/read permission patterns. Mutations are journaled
//! (§11: the auth database uses the durable journal); a version counter
//! gives every lookup a cache-invalidation token (FR-S08).

use std::collections::HashMap;

/// Management role (FR-S04).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    Ordinary,
    Monitor,
    Operator,
    Admin,
}

impl Role {
    pub fn as_u8(self) -> u8 {
        match self {
            Role::Ordinary => 0,
            Role::Monitor => 1,
            Role::Operator => 2,
            Role::Admin => 3,
        }
    }
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Role::Ordinary),
            1 => Some(Role::Monitor),
            2 => Some(Role::Operator),
            3 => Some(Role::Admin),
            _ => None,
        }
    }
}

/// A stored principal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    pub username: String,
    /// Argon2id PHC string; never plaintext.
    pub password_phc: String,
    pub role: Role,
}

/// Permissions for (user, vhost).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Permissions {
    pub configure: String,
    pub write: String,
    pub read: String,
}

/// The kind of access an operation needs (§11.2 table).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    Configure,
    Write,
    Read,
}

/// Broker-wide auth state with versioned invalidation.
pub struct AuthState {
    principals: HashMap<String, Principal>,
    permissions: HashMap<(String, String), Permissions>,
    /// Bumped on every mutation (FR-S08 cache token).
    version: u64,
}

impl Default for AuthState {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthState {
    pub fn new() -> Self {
        Self {
            principals: HashMap::new(),
            permissions: HashMap::new(),
            version: 1,
        }
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn principal(&self, username: &str) -> Option<&Principal> {
        self.principals.get(username)
    }

    /// Usernames (sorted for stable listings).
    pub fn principal_names(&self) -> impl Iterator<Item = &String> {
        let mut names: Vec<&String> = self.principals.keys().collect();
        names.sort();
        names.into_iter()
    }

    /// All permission rows (management listings; sorted for stability).
    pub fn all_permissions(&self) -> Vec<(String, String, Permissions)> {
        let mut rows: Vec<_> = self
            .permissions
            .iter()
            .map(|((u, v), p)| (u.clone(), v.clone(), p.clone()))
            .collect();
        rows.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        rows
    }

    /// Doctor summary: (principal count, permission grant count).
    pub fn list_counts_for_doctor(&self) -> (usize, usize) {
        (self.principals.len(), self.permissions.len())
    }

    /// Direct permissions read (management listings).
    pub fn get_permissions(&self, username: &str, vhost: &str) -> Option<Permissions> {
        self.permissions
            .get(&(username.to_string(), vhost.to_string()))
            .cloned()
    }

    pub fn upsert_principal(&mut self, principal: Principal) {
        self.version += 1;
        self.principals
            .insert(principal.username.clone(), principal);
    }

    pub fn delete_principal(&mut self, username: &str) -> Option<Principal> {
        self.version += 1;
        // Permissions die with the principal.
        self.permissions.retain(|(u, _), _| u != username);
        self.principals.remove(username)
    }

    pub fn set_permissions(&mut self, username: &str, vhost: &str, perms: Permissions) {
        self.version += 1;
        self.permissions
            .insert((username.to_string(), vhost.to_string()), perms);
    }

    pub fn delete_permissions(&mut self, username: &str, vhost: &str) -> Option<Permissions> {
        self.version += 1;
        self.permissions
            .remove(&(username.to_string(), vhost.to_string()))
    }

    /// Whether the user may enter the vhost at all (has any permission
    /// entry there). INV-08: no cross-vhost anything without it.
    pub fn may_access_vhost(&self, username: &str, vhost: &str) -> bool {
        self.permissions
            .contains_key(&(username.to_string(), vhost.to_string()))
    }

    /// The §11.2 check: `access` on `resource` in `vhost` for `username`.
    /// Patterns are unanchored Rust regexes (RabbitMQ-style: the pattern
    /// may match anywhere in the name); an invalid stored pattern fails
    /// closed (deny).
    pub fn check(&self, username: &str, vhost: &str, access: Access, resource: &str) -> bool {
        let Some(perms) = self
            .permissions
            .get(&(username.to_string(), vhost.to_string()))
        else {
            return false;
        };
        let pattern = match access {
            Access::Configure => &perms.configure,
            Access::Write => &perms.write,
            Access::Read => &perms.read,
        };
        match regex::Regex::new(pattern) {
            Ok(re) => re.is_match(resource),
            Err(_) => false,
        }
    }

    /// Passive-declare profile (frozen at M0/ADR-0005): any one of the
    /// three permissions on the resource suffices.
    pub fn check_any(&self, username: &str, vhost: &str, resource: &str) -> bool {
        [Access::Configure, Access::Write, Access::Read]
            .iter()
            .any(|a| self.check(username, vhost, *a, resource))
    }

    /// Default-exchange publish uses the normalized permission name
    /// `amq.default` (§11.2).
    pub const DEFAULT_EXCHANGE_PERMISSION_NAME: &'static str = "amq.default";
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> AuthState {
        let mut a = AuthState::new();
        a.upsert_principal(Principal {
            username: "app".into(),
            password_phc: "$argon2id$...".into(),
            role: Role::Ordinary,
        });
        a.set_permissions(
            "app",
            "/",
            Permissions {
                configure: "^jobs$".into(),
                write: "jobs|events".into(),
                read: ".*".into(),
            },
        );
        a
    }

    #[test]
    fn regex_permissions_gate_resources() {
        let a = state();
        assert!(a.check("app", "/", Access::Configure, "jobs"));
        assert!(
            !a.check("app", "/", Access::Configure, "other"),
            "unanchored ^..$ exact"
        );
        assert!(a.check("app", "/", Access::Write, "jobs"));
        assert!(a.check("app", "/", Access::Write, "events"));
        assert!(!a.check("app", "/", Access::Write, "logs"));
        assert!(a.check("app", "/", Access::Read, "anything"));
    }

    #[test]
    fn vhost_isolation() {
        let a = state();
        assert!(a.may_access_vhost("app", "/"));
        assert!(!a.may_access_vhost("app", "other"));
        assert!(!a.check("app", "other", Access::Read, "jobs"));
    }

    #[test]
    fn version_bumps_and_revocation_takes_effect() {
        let mut a = state();
        let v0 = a.version();
        a.delete_permissions("app", "/");
        assert!(a.version() > v0);
        assert!(!a.may_access_vhost("app", "/"), "revocation is immediate");
    }

    #[test]
    fn invalid_pattern_fails_closed() {
        let mut a = AuthState::new();
        a.set_permissions(
            "u",
            "/",
            Permissions {
                configure: "([".into(), // invalid regex
                write: ".*".into(),
                read: ".*".into(),
            },
        );
        assert!(!a.check("u", "/", Access::Configure, "jobs"));
    }

    #[test]
    fn principal_deletion_drops_permissions() {
        let mut a = state();
        a.upsert_principal(Principal {
            username: "tmp".into(),
            password_phc: "x".into(),
            role: Role::Ordinary,
        });
        a.set_permissions(
            "tmp",
            "/",
            Permissions {
                configure: ".*".into(),
                write: ".*".into(),
                read: ".*".into(),
            },
        );
        a.delete_principal("tmp");
        assert!(!a.may_access_vhost("tmp", "/"));
    }
}
