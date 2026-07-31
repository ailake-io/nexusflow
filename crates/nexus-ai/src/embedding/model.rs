use std::path::PathBuf;
use thiserror::Error;

/// Pinned model reference — repo + revision, never an implicit "latest", for
/// reproducibility. See ARCHITECTURE.md §8.
#[derive(Debug, Clone)]
pub struct ModelConfig {
    pub repo_id: String,
    pub revision: String,
    pub filename: String,
}

#[derive(Debug, Error)]
pub enum ModelError {
    #[error("could not resolve a local cache directory for the model")]
    NoCacheDir,
    #[error("hugging face hub error: {0}")]
    Hub(#[from] hf_hub::api::tokio::ApiError),
}

fn cache_root() -> Result<PathBuf, ModelError> {
    dirs::cache_dir()
        .map(|dir| dir.join("nexusflow").join("models"))
        .ok_or(ModelError::NoCacheDir)
}

/// Resolves the local path for `cfg`, downloading from the Hugging Face Hub
/// into the nexusflow cache dir on first use — subsequent calls for the same
/// repo/revision/filename are served from cache, no network needed.
pub async fn resolve_model_path(cfg: &ModelConfig) -> Result<PathBuf, ModelError> {
    let api = hf_hub::api::tokio::ApiBuilder::new()
        .with_cache_dir(cache_root()?)
        .build()?;
    let repo = api.repo(hf_hub::Repo::with_revision(
        cfg.repo_id.clone(),
        hf_hub::RepoType::Model,
        cfg.revision.clone(),
    ));
    Ok(repo.get(&cfg.filename).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_root_is_scoped_under_nexusflow() {
        let root = cache_root().expect("cache dir resolvable in test env");
        assert!(root.ends_with("nexusflow/models"));
    }
}
