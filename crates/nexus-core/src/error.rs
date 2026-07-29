use thiserror::Error;

#[derive(Debug, Error)]
pub enum NexusError {
    #[error("connector error: {0}")]
    Connector(String),

    #[error("schema error: {0}")]
    Schema(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("checkpoint error: {0}")]
    Checkpoint(String),
}
