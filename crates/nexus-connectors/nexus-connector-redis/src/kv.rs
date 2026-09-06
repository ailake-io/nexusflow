use nexus_core::{with_timeout, NexusError};
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client};

/// Plain Redis `GET`/`SETEX` — separate from `RedisSource`/`RedisSink`
/// (Streams only, `XADD`/`XREAD`) on purpose, same reasoning their own doc
/// comments give for not mixing the two usage patterns in one struct. Used
/// by `nexus-server`'s LLM response cache
/// (LLMOPS_IMPLEMENTATION_PLAN.md Marco L3) — nothing here is
/// LLM-specific, this is just a generic Redis KV client.
pub struct RedisKvClient {
    connection: MultiplexedConnection,
}

/// Same default as `RedisConnectorConfig::default_timeout_seconds` (not
/// reused directly — that config carries Streams-specific fields this
/// client has no use for).
const DEFAULT_TIMEOUT_SECONDS: u64 = 10;

impl RedisKvClient {
    pub async fn connect(url: &str) -> Result<Self, NexusError> {
        let client =
            Client::open(url).map_err(|e| NexusError::Connector(format!("redis client: {e}")))?;
        let connection = with_timeout(DEFAULT_TIMEOUT_SECONDS, "redis connect", async {
            client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| NexusError::Connector(format!("redis connect: {e}")))
        })
        .await?;
        Ok(Self { connection })
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>, NexusError> {
        let mut conn = self.connection.clone();
        conn.get(key)
            .await
            .map_err(|e| NexusError::Connector(format!("redis GET: {e}")))
    }

    pub async fn set_ex(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<(), NexusError> {
        let mut conn = self.connection.clone();
        conn.set_ex::<_, _, ()>(key, value, ttl_seconds)
            .await
            .map_err(|e| NexusError::Connector(format!("redis SETEX: {e}")))
    }
}
