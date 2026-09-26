use crate::config::DropboxConnectorConfig;
use nexus_core::{retry_with_backoff, NexusError};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
struct ListFolderEntry {
    #[serde(rename = ".tag")]
    tag: String,
    name: String,
    path_lower: String,
}

#[derive(Deserialize)]
struct ListFolderResponse {
    entries: Vec<ListFolderEntry>,
    cursor: String,
    has_more: bool,
}

/// Lists every file (not subfolder) directly inside `folder_path`,
/// sorted by name — same "non-recursive, sorted" contract
/// `nexus-connector-csv`'s local-directory listing has. Paginates via
/// `list_folder`/`list_folder/continue` (real, documented Dropbox API
/// v2 cursor pagination).
pub(crate) async fn list_files(
    client: &reqwest::Client,
    cfg: &DropboxConnectorConfig,
) -> Result<Vec<String>, NexusError> {
    let mut paths = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let (url, body) = match &cursor {
            None => (
                format!("{}/2/files/list_folder", cfg.api_base_url),
                json!({ "path": cfg.folder_path }),
            ),
            Some(cursor) => (
                format!("{}/2/files/list_folder/continue", cfg.api_base_url),
                json!({ "cursor": cursor }),
            ),
        };

        let client = client.clone();
        let access_token = cfg.access_token.clone();
        let timeout_seconds = cfg.timeout_seconds;
        let url_c = url.clone();
        let body_c = body.clone();
        let response: ListFolderResponse =
            retry_with_backoff(&cfg.retry, "dropbox list_folder", || {
                let client = client.clone();
                let access_token = access_token.clone();
                let url = url_c.clone();
                let body = body_c.clone();
                async move {
                    let response = tokio::time::timeout(
                        std::time::Duration::from_secs(timeout_seconds),
                        client
                            .post(&url)
                            .bearer_auth(&access_token)
                            .json(&body)
                            .send(),
                    )
                    .await
                    .map_err(|e| {
                        NexusError::Connector(format!("dropbox list_folder timed out: {e}"))
                    })?
                    .map_err(|e| {
                        NexusError::Connector(format!("dropbox list_folder failed: {e}"))
                    })?;

                    if !response.status().is_success() {
                        let status = response.status();
                        let text = response.text().await.unwrap_or_default();
                        return Err(NexusError::Connector(format!(
                            "dropbox list_folder failed ({status}): {text}"
                        )));
                    }
                    response.json().await.map_err(|e| {
                        NexusError::Connector(format!(
                            "dropbox list_folder response parse failed: {e}"
                        ))
                    })
                }
            })
            .await?;

        for entry in response.entries {
            if entry.tag == "file" {
                paths.push(entry.path_lower.clone());
                let _ = entry.name;
            }
        }

        if !response.has_more {
            break;
        }
        cursor = Some(response.cursor);
    }

    paths.sort();
    if paths.is_empty() {
        return Err(NexusError::Connector(format!(
            "dropbox: folder '{}' has no files",
            cfg.folder_path
        )));
    }
    Ok(paths)
}

/// Downloads one file's raw bytes via `POST /2/files/download` — real
/// Dropbox API v2 quirk: the target path goes in a `Dropbox-API-Arg`
/// header (JSON-encoded), not the body, and the response body is the
/// raw file content directly (no JSON envelope).
pub(crate) async fn download_file(
    client: &reqwest::Client,
    cfg: &DropboxConnectorConfig,
    path: &str,
) -> Result<Vec<u8>, NexusError> {
    let url = format!("{}/2/files/download", cfg.content_base_url);
    let arg = json!({ "path": path }).to_string();

    let client = client.clone();
    let access_token = cfg.access_token.clone();
    let timeout_seconds = cfg.timeout_seconds;
    retry_with_backoff(&cfg.retry, "dropbox download", || {
        let client = client.clone();
        let access_token = access_token.clone();
        let url = url.clone();
        let arg = arg.clone();
        async move {
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(timeout_seconds),
                client
                    .post(&url)
                    .bearer_auth(&access_token)
                    .header("Dropbox-API-Arg", arg)
                    .send(),
            )
            .await
            .map_err(|e| NexusError::Connector(format!("dropbox download timed out: {e}")))?
            .map_err(|e| NexusError::Connector(format!("dropbox download failed: {e}")))?;

            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                return Err(NexusError::Connector(format!(
                    "dropbox download failed ({status}): {text}"
                )));
            }
            response.bytes().await.map(|b| b.to_vec()).map_err(|e| {
                NexusError::Connector(format!("dropbox download body read failed: {e}"))
            })
        }
    })
    .await
}

/// Uploads `bytes` as a new file via `POST /2/files/upload` — same
/// `Dropbox-API-Arg` header quirk as download, plus
/// `Content-Type: application/octet-stream` and the raw bytes as the
/// body. `mode: "add"` never overwrites an existing file (Dropbox
/// auto-renames on conflict, e.g. `events (1).csv`) — matches the
/// sink's "one new file per batch, never overwrite" contract.
pub(crate) async fn upload_file(
    client: &reqwest::Client,
    cfg: &DropboxConnectorConfig,
    path: &str,
    bytes: Vec<u8>,
) -> Result<(), NexusError> {
    let url = format!("{}/2/files/upload", cfg.content_base_url);
    let arg = json!({ "path": path, "mode": "add" }).to_string();

    let client = client.clone();
    let access_token = cfg.access_token.clone();
    let timeout_seconds = cfg.timeout_seconds;
    retry_with_backoff(&cfg.retry, "dropbox upload", || {
        let client = client.clone();
        let access_token = access_token.clone();
        let url = url.clone();
        let arg = arg.clone();
        let bytes = bytes.clone();
        async move {
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(timeout_seconds),
                client
                    .post(&url)
                    .bearer_auth(&access_token)
                    .header("Dropbox-API-Arg", arg)
                    .header("Content-Type", "application/octet-stream")
                    .body(bytes)
                    .send(),
            )
            .await
            .map_err(|e| NexusError::Connector(format!("dropbox upload timed out: {e}")))?
            .map_err(|e| NexusError::Connector(format!("dropbox upload failed: {e}")))?;

            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                return Err(NexusError::Connector(format!(
                    "dropbox upload failed ({status}): {text}"
                )));
            }
            Ok(())
        }
    })
    .await
}
