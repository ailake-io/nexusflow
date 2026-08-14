use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not runtime).
/// Deserialized from the DAG node's raw `config` JSON — see ARCHITECTURE.md §3.
///
/// The connection can be supplied either as a complete URI through
/// `connection_string` (which takes precedence) or through the individual
/// `hosts`, `username`, `password` and `auth_database` fields, which are
/// assembled into a standard MongoDB URI when no explicit string is given.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MongoConnectorConfig {
    /// Full MongoDB connection URI. When provided, it overrides any separate
    /// host, port, credential or option fields. Use this for Atlas
    /// `mongodb+srv://` strings, replica-set URIs with multiple seed hosts, or
    /// any connection string that already encodes authentication, TLS and
    /// read-preference options. Example:
    /// `mongodb://user:pass@host1:27017,host2:27017/db?replicaSet=rs0`.
    #[serde(default)]
    pub connection_string: Option<String>,

    /// List of MongoDB server endpoints as `host:port` strings. Used only when
    /// `connection_string` is empty. For a standalone server use a single entry
    /// such as `["localhost:27017"]`. For a replica set, list the seed members.
    /// The entries are joined into a comma-separated host list in the generated
    /// URI. Example: `["mongo1:27017", "mongo2:27017"]`.
    #[serde(default)]
    pub hosts: Vec<String>,

    /// MongoDB user name for authentication. Leave empty for unauthenticated
    /// connections. When supplied together with `password`, the credentials are
    /// embedded in the generated URI before the host list.
    #[serde(default)]
    pub username: Option<String>,

    /// Password matching `username`. Stored in plain text in the node config;
    /// the frontend masks it as a secret input field. Only used when
    /// `connection_string` is empty.
    #[serde(default)]
    pub password: Option<String>,

    /// Authentication database against which the user credentials are verified.
    /// Common values are `admin` or the database itself. When omitted, the
    /// driver falls back to the value of `database`.
    #[serde(default)]
    pub auth_database: Option<String>,

    /// Default database name for the connector to read from or write to. This
    /// becomes the path component of the generated URI and is used to resolve
    /// `collection`.
    pub database: String,

    /// Collection name within `database` that the source scans or the sink
    /// writes to.
    pub collection: String,

    /// Document field used as the upsert key on the sink side — see
    /// ARCHITECTURE.md §5 (idempotency is a `Sink` contract, not optional).
    pub primary_key: String,

    /// MongoDB read preference for the source cursor, e.g. `primary`,
    /// `primaryPreferred`, `secondary`, `secondaryPreferred` or `nearest`. Only
    /// affects reads; sinks always write to the primary. Applied as a URI
    /// option when `connection_string` is not provided.
    #[serde(default)]
    pub read_preference: Option<String>,

    /// Whether to require TLS for the generated connection URI. When
    /// `connection_string` is provided, this field is ignored. Equivalent to
    /// adding `tls=true` to the URI options.
    #[serde(default)]
    pub tls: Option<bool>,

    /// Explicit target schema — a MongoDB collection carries no fixed schema
    /// of its own, so the node config must say what to project each field to.
    pub fields: Vec<MongoFieldSpec>,

    /// How many documents to fold into a single `RecordBatch` while scanning.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Timeout in seconds for each call to MongoDB (connect, buildInfo,
    /// bulk_write/replace_one/delete_one) — a stalled connection would
    /// otherwise block the pipeline indefinitely (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl MongoConnectorConfig {
    /// Resolves the MongoDB connection URI that the driver should use.
    ///
    /// If `connection_string` is set, it is returned verbatim. Otherwise a URI
    /// is built from `hosts`, `username`, `password`, `auth_database`,
    /// `database` and the optional TLS/read-preference flags.
    pub fn connection_string(&self) -> String {
        if let Some(uri) = &self.connection_string {
            if !uri.trim().is_empty() {
                return uri.clone();
            }
        }

        let hosts = if self.hosts.is_empty() {
            "localhost:27017".to_string()
        } else {
            self.hosts.join(",")
        };

        let auth = match (&self.username, &self.password) {
            (Some(u), Some(p)) if !u.is_empty() && !p.is_empty() => {
                format!("{}:{}@", encode_userinfo(u), encode_userinfo(p))
            }
            (Some(u), _) if !u.is_empty() => format!("{}@", encode_userinfo(u)),
            _ => String::new(),
        };

        let mut options: Vec<(String, String)> = Vec::new();

        if let Some(auth_db) = self.auth_database.as_ref().filter(|s| !s.is_empty()) {
            options.push(("authSource".to_string(), auth_db.clone()));
        }

        if let Some(rp) = self.read_preference.as_ref().filter(|s| !s.is_empty()) {
            options.push(("readPreference".to_string(), rp.clone()));
        }

        if self.tls == Some(true) {
            options.push(("tls".to_string(), "true".to_string()));
        }

        let opt_string = if options.is_empty() {
            String::new()
        } else {
            format!(
                "?{}",
                options
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join("&")
            )
        };

        format!(
            "mongodb://{}{}/{}{}",
            auth, hosts, self.database, opt_string
        )
    }
}

/// Percent-encodes characters that are not allowed unescaped in the userinfo
/// part of a URI. This is a minimal encoder for `:` and `@`, which are the
/// characters most likely to appear in MongoDB credentials and would otherwise
/// break URI parsing.
fn encode_userinfo(value: &str) -> String {
    value
        .replace('%', "%25")
        .replace(':', "%3A")
        .replace('@', "%40")
}

fn default_timeout_seconds() -> u64 {
    30
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MongoFieldSpec {
    /// Document field name to project — supports dot notation for nested
    /// fields (e.g. `"address.city"`).
    pub name: String,
    /// Arrow type this field's value gets converted to.
    pub data_type: MongoDataType,
    /// Whether a missing/null value for this field is allowed — if false
    /// and the document lacks it, that row fails instead of being written
    /// with a null.
    #[serde(default)]
    pub nullable: bool,
}

/// Arrow type a document field is projected onto — one of these four
/// primitives, matched by name in the node config's `data_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MongoDataType {
    Int64,
    Float64,
    Boolean,
    Utf8,
}

fn default_batch_size() -> usize {
    1000
}

fn default_max_batch_events() -> u64 {
    1000
}

/// Config for the native Change Streams CDC source (`"mongodb-cdc"`) — a
/// separate connector name from `"mongodb"` rather than a mode flag on
/// `MongoConnectorConfig`, so the existing batch connector's config shape
/// never changes. Requires MongoDB running as a replica set (even a
/// single-node one) — Change Streams don't work on a standalone server.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MongoCdcConfig {
    /// Full MongoDB connection URI. When provided, it overrides any separate
    /// host, port, credential or option fields. Replica-set URIs are common
    /// here because Change Streams require a replica set.
    #[serde(default)]
    pub connection_string: Option<String>,

    /// List of MongoDB server endpoints as `host:port` strings. Used only when
    /// `connection_string` is empty. For a replica set, list the seed members.
    #[serde(default)]
    pub hosts: Vec<String>,

    /// MongoDB user name for authentication. Leave empty for unauthenticated
    /// connections.
    #[serde(default)]
    pub username: Option<String>,

    /// Password matching `username`. Only used when `connection_string` is
    /// empty.
    #[serde(default)]
    pub password: Option<String>,

    /// Authentication database against which the user credentials are verified.
    /// When omitted, the driver falls back to the value of `database`.
    #[serde(default)]
    pub auth_database: Option<String>,

    /// Database name that contains the watched collection.
    pub database: String,

    /// Collection name to watch with the Change Stream.
    pub collection: String,

    /// Same projection contract as the batch connector — a change event's
    /// full document is projected onto these fields, see
    /// `MongoConnectorConfig::fields`.
    pub fields: Vec<MongoFieldSpec>,

    /// Resume token from a previous run's last processed event (as returned
    /// by `MongoCdcSource`'s checkpoint), so a restart picks up where it left
    /// off instead of only seeing events from now on. Stored as the token's
    /// extended-JSON form. `None` starts watching from the current moment —
    /// same "static config field, not server-injected" resume model as
    /// Kafka's `start_offsets` (ARCHITECTURE.md §7 doesn't cover this, but
    /// `nexus-server` doesn't round-trip checkpoints into any source's
    /// config today, streaming or batch).
    #[serde(default)]
    pub resume_token: Option<String>,

    /// Maximum number of change events to fold into a single `RecordBatch`
    /// before yielding downstream.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Timeout in seconds for each MongoDB Change Stream call (connect, watch,
    /// resume) — a stalled connection would otherwise block the pipeline
    /// indefinitely (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Maximum number of change events to collect in a single run. After this
    /// many events the source ends cleanly, letting the run finish and the
    /// scheduler start the next micro-batch. The MongoDB resume token persists
    /// the last processed event, so the next run picks up where this one left
    /// off.
    #[serde(default = "default_max_batch_events")]
    pub max_batch_events: u64,
}

impl MongoCdcConfig {
    /// Resolves the MongoDB connection URI for the CDC source.
    ///
    /// If `connection_string` is set, it is returned verbatim. Otherwise a URI
    /// is built from `hosts`, `username`, `password`, `auth_database` and
    /// `database`, mirroring `MongoConnectorConfig::connection_string`.
    pub fn connection_string(&self) -> String {
        if let Some(uri) = &self.connection_string {
            if !uri.trim().is_empty() {
                return uri.clone();
            }
        }

        let hosts = if self.hosts.is_empty() {
            "localhost:27017".to_string()
        } else {
            self.hosts.join(",")
        };

        let auth = match (&self.username, &self.password) {
            (Some(u), Some(p)) if !u.is_empty() && !p.is_empty() => {
                format!("{}:{}@", encode_userinfo(u), encode_userinfo(p))
            }
            (Some(u), _) if !u.is_empty() => format!("{}@", encode_userinfo(u)),
            _ => String::new(),
        };

        let mut options: Vec<(String, String)> = Vec::new();

        if let Some(auth_db) = self.auth_database.as_ref().filter(|s| !s.is_empty()) {
            options.push(("authSource".to_string(), auth_db.clone()));
        }

        let opt_string = if options.is_empty() {
            String::new()
        } else {
            format!(
                "?{}",
                options
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join("&")
            )
        };

        format!(
            "mongodb://{}{}/{}{}",
            auth, hosts, self.database, opt_string
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch_config() -> MongoConnectorConfig {
        MongoConnectorConfig {
            connection_string: None,
            hosts: Vec::new(),
            username: None,
            password: None,
            auth_database: None,
            database: "nexus".to_string(),
            collection: "events".to_string(),
            primary_key: "id".to_string(),
            read_preference: None,
            tls: None,
            fields: Vec::new(),
            batch_size: 1000,
            timeout_seconds: 30,
            max_batch_events: 1000,
        }
    }

    #[test]
    fn connection_string_takes_priority_over_fields() {
        let mut cfg = batch_config();
        cfg.connection_string = Some("mongodb://legacy/host/db".to_string());
        cfg.hosts = vec!["new:27017".to_string()];
        assert_eq!(cfg.connection_string(), "mongodb://legacy/host/db");
    }

    #[test]
    fn empty_connection_string_falls_back_to_fields() {
        let mut cfg = batch_config();
        cfg.connection_string = Some("".to_string());
        cfg.hosts = vec!["mongo1:27017".to_string(), "mongo2:27017".to_string()];
        assert_eq!(
            cfg.connection_string(),
            "mongodb://mongo1:27017,mongo2:27017/nexus"
        );
    }

    #[test]
    fn builds_uri_with_credentials_and_auth_db() {
        let mut cfg = batch_config();
        cfg.hosts = vec!["mongo:27017".to_string()];
        cfg.username = Some("user".to_string());
        cfg.password = Some("p@ss:w0rd".to_string());
        cfg.auth_database = Some("admin".to_string());
        cfg.read_preference = Some("secondaryPreferred".to_string());
        cfg.tls = Some(true);
        assert_eq!(
            cfg.connection_string(),
            "mongodb://user:p%40ss%3Aw0rd@mongo:27017/nexus?authSource=admin&readPreference=secondaryPreferred&tls=true"
        );
    }

    #[test]
    fn defaults_to_localhost_when_no_hosts_or_uri() {
        let cfg = batch_config();
        assert_eq!(cfg.connection_string(), "mongodb://localhost:27017/nexus");
    }

    #[test]
    fn cdc_connection_string_mirrors_batch_logic() {
        let cfg = MongoCdcConfig {
            connection_string: None,
            hosts: vec!["rs0/mongo1:27017".to_string()],
            username: Some("u".to_string()),
            password: Some("p".to_string()),
            auth_database: Some("admin".to_string()),
            database: "cdc".to_string(),
            collection: "events".to_string(),
            fields: Vec::new(),
            resume_token: None,
            batch_size: 1000,
            timeout_seconds: 30,
            max_batch_events: 1000,
        };
        assert_eq!(
            cfg.connection_string(),
            "mongodb://u:p@rs0/mongo1:27017/cdc?authSource=admin"
        );
    }
}
