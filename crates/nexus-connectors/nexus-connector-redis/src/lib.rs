//! Generic bridging connector for Redis Streams (`XADD`/`XREAD`) —
//! not a generic key/value dump. `redis` (redis-rs) is a real async
//! network dependency, so the client is behind the `client` Cargo
//! feature (CLAUDE.md §8.5) — building this crate with no features
//! enabled compiles config parsing only, no network client linkage.

mod config;
#[cfg(feature = "client")]
mod sink;
#[cfg(feature = "client")]
mod source;

pub use config::{RedisConnectorConfig, RedisFieldSpec, RedisStartingPosition};
#[cfg(feature = "client")]
pub use sink::RedisSink;
#[cfg(feature = "client")]
pub use source::RedisSource;

#[cfg(feature = "client")]
nexus_core::submit_connector!(
    "redis",
    nexus_core::ConnectorCapability::Bridged,
    RedisConnectorConfig
);
