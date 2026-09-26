use crate::config::GoogleDriveConnectorConfig;
use nexus_core::{retry_with_backoff, NexusError};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
struct DriveFile {
    id: String,
    #[serde(default)]
    name: String,
}

#[derive(Deserialize)]
struct FilesListResponse {
    #[serde(default)]
    files: Vec<DriveFile>,
    #[serde(default)]
    next_page_token: Option<String>,
}

/// Lists every non-trashed file directly inside `folder_id` (real
/// Drive API v3 query: `'{id}' in parents and trashed=false`),
/// paginated via `pageToken`/`nextPageToken` — same "concatenate
/// every file in the folder" contract `nexus-connector-csv`'s
/// local-directory mode and `nexus-connector-dropbox`'s folder mode
/// both have. Returns `(file_id, name)` pairs sorted by name so file
/// order is deterministic (Drive's own listing order is not
/// guaranteed to be stable).
pub(crate) async fn list_files(
    client: &reqwest::Client,
    cfg: &GoogleDriveConnectorConfig,
) -> Result<Vec<(String, String)>, NexusError> {
    let mut files = Vec::new();
    let mut page_token: Option<String> = None;
    let query = format!("'{}' in parents and trashed=false", cfg.folder_id);

    loop {
        let url = format!("{}/drive/v3/files", cfg.api_base_url);
        let mut params: Vec<(String, String)> = vec![
            ("q".to_string(), query.clone()),
            (
                "fields".to_string(),
                "nextPageToken,files(id,name)".to_string(),
            ),
            ("pageSize".to_string(), "1000".to_string()),
        ];
        if let Some(token) = &page_token {
            params.push(("pageToken".to_string(), token.clone()));
        }

        let client = client.clone();
        let access_token = cfg.access_token.clone();
        let timeout_seconds = cfg.timeout_seconds;
        let url_c = url.clone();
        let params_c = params.clone();
        let response: FilesListResponse =
            retry_with_backoff(&cfg.retry, "google-drive files.list", || {
                let client = client.clone();
                let access_token = access_token.clone();
                let url = url_c.clone();
                let params = params_c.clone();
                async move {
                    let response = tokio::time::timeout(
                        std::time::Duration::from_secs(timeout_seconds),
                        client
                            .get(&url)
                            .bearer_auth(&access_token)
                            .query(&params)
                            .send(),
                    )
                    .await
                    .map_err(|e| {
                        NexusError::Connector(format!("google-drive files.list timed out: {e}"))
                    })?
                    .map_err(|e| {
                        NexusError::Connector(format!("google-drive files.list failed: {e}"))
                    })?;

                    if !response.status().is_success() {
                        let status = response.status();
                        let text = response.text().await.unwrap_or_default();
                        return Err(NexusError::Connector(format!(
                            "google-drive files.list failed ({status}): {text}"
                        )));
                    }
                    response.json().await.map_err(|e| {
                        NexusError::Connector(format!(
                            "google-drive files.list response parse failed: {e}"
                        ))
                    })
                }
            })
            .await?;

        for file in response.files {
            files.push((file.id, file.name));
        }

        page_token = response.next_page_token;
        if page_token.is_none() {
            break;
        }
    }

    files.sort_by(|a, b| a.1.cmp(&b.1));
    if files.is_empty() {
        return Err(NexusError::Connector(format!(
            "google-drive: folder '{}' has no files",
            cfg.folder_id
        )));
    }
    Ok(files)
}

/// Downloads one file's raw bytes via `GET /drive/v3/files/{id}?alt=media`
/// — real, documented Drive API v3 media-download shape (no JSON
/// envelope, raw content directly in the response body).
pub(crate) async fn download_file(
    client: &reqwest::Client,
    cfg: &GoogleDriveConnectorConfig,
    file_id: &str,
) -> Result<Vec<u8>, NexusError> {
    let url = format!("{}/drive/v3/files/{file_id}", cfg.api_base_url);

    let client = client.clone();
    let access_token = cfg.access_token.clone();
    let timeout_seconds = cfg.timeout_seconds;
    retry_with_backoff(&cfg.retry, "google-drive files.get", || {
        let client = client.clone();
        let access_token = access_token.clone();
        let url = url.clone();
        async move {
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(timeout_seconds),
                client
                    .get(&url)
                    .bearer_auth(&access_token)
                    .query(&[("alt", "media")])
                    .send(),
            )
            .await
            .map_err(|e| NexusError::Connector(format!("google-drive files.get timed out: {e}")))?
            .map_err(|e| NexusError::Connector(format!("google-drive files.get failed: {e}")))?;

            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                return Err(NexusError::Connector(format!(
                    "google-drive files.get failed ({status}): {text}"
                )));
            }
            response.bytes().await.map(|b| b.to_vec()).map_err(|e| {
                NexusError::Connector(format!("google-drive files.get body read failed: {e}"))
            })
        }
    })
    .await
}

#[derive(Deserialize)]
struct CreateFileResponse {
    #[allow(dead_code)]
    id: String,
}

/// Uploads `bytes` as a new file in `folder_id` — real Drive API v3
/// two-step create: `POST /drive/v3/files` first creates the metadata
/// (name + parent folder) and returns an id, then
/// `PATCH {upload_base_url}/upload/drive/v3/files/{id}?uploadType=media`
/// sets its content. Two real HTTP calls, not a single multipart one
/// — simpler to implement correctly, same trade-off
/// `nexus-connector-zendesk`'s per-record create/update (vs. an async
/// bulk job) makes elsewhere in this repo.
pub(crate) async fn upload_file(
    client: &reqwest::Client,
    cfg: &GoogleDriveConnectorConfig,
    name: &str,
    bytes: Vec<u8>,
) -> Result<(), NexusError> {
    let create_url = format!("{}/drive/v3/files", cfg.api_base_url);
    let metadata = json!({ "name": name, "parents": [cfg.folder_id] });

    let client_c = client.clone();
    let access_token = cfg.access_token.clone();
    let timeout_seconds = cfg.timeout_seconds;
    let created: CreateFileResponse =
        retry_with_backoff(&cfg.retry, "google-drive files.create", || {
            let client = client_c.clone();
            let access_token = access_token.clone();
            let url = create_url.clone();
            let metadata = metadata.clone();
            async move {
                let response = tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_seconds),
                    client
                        .post(&url)
                        .bearer_auth(&access_token)
                        .json(&metadata)
                        .send(),
                )
                .await
                .map_err(|e| {
                    NexusError::Connector(format!("google-drive files.create timed out: {e}"))
                })?
                .map_err(|e| {
                    NexusError::Connector(format!("google-drive files.create failed: {e}"))
                })?;

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    return Err(NexusError::Connector(format!(
                        "google-drive files.create failed ({status}): {text}"
                    )));
                }
                response.json().await.map_err(|e| {
                    NexusError::Connector(format!(
                        "google-drive files.create response parse failed: {e}"
                    ))
                })
            }
        })
        .await?;

    let upload_url = format!(
        "{}/upload/drive/v3/files/{}",
        cfg.upload_base_url, created.id
    );
    let access_token = cfg.access_token.clone();
    retry_with_backoff(&cfg.retry, "google-drive files.update media", || {
        let client = client.clone();
        let access_token = access_token.clone();
        let url = upload_url.clone();
        let bytes = bytes.clone();
        async move {
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(timeout_seconds),
                client
                    .patch(&url)
                    .bearer_auth(&access_token)
                    .query(&[("uploadType", "media")])
                    .header("Content-Type", "application/octet-stream")
                    .body(bytes)
                    .send(),
            )
            .await
            .map_err(|e| {
                NexusError::Connector(format!("google-drive files.update media timed out: {e}"))
            })?
            .map_err(|e| {
                NexusError::Connector(format!("google-drive files.update media failed: {e}"))
            })?;

            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                return Err(NexusError::Connector(format!(
                    "google-drive files.update media failed ({status}): {text}"
                )));
            }
            Ok(())
        }
    })
    .await
}
