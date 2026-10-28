use crate::auth::resolve_access_token;
use crate::config::LinkedinAdsConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{with_timeout, NexusError, RecordBatchBuilder, Source};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

/// Reads via `GET /rest/adAnalytics` — confirmed real endpoint and
/// query shape (dot-notation date range, `pivot`, `fields`, REST.li
/// `List(...)` array syntax for `accounts`). **No pagination** —
/// confirmed real hard cap of 15,000 elements per request, not a
/// simplification. Every column is `Utf8`, same fallback every other
/// tier-5 connector in this workspace uses.
///
/// Response envelope (`elements` array) follows LinkedIn's standard
/// Rest.li finder-response convention — a reasonable, well-grounded
/// assumption given every other Rest.li finder response uses this key,
/// but **not independently confirmed against a live call** this
/// session (no MDP-approved account available to test against — see
/// `docs/PLANNED_CONNECTORS.md`).
pub struct LinkedinAdsSource {
    client: reqwest::Client,
    cfg: LinkedinAdsConnectorConfig,
    schema: SchemaRef,
}

#[derive(Deserialize)]
struct AdAnalyticsResponse {
    #[serde(default)]
    elements: Vec<Value>,
}

impl LinkedinAdsSource {
    pub async fn connect(cfg: &LinkedinAdsConnectorConfig) -> Result<Self, NexusError> {
        if cfg.fields.is_empty() {
            return Err(NexusError::Schema(
                "linkedin-ads: fields must not be empty".into(),
            ));
        }
        if cfg.account_urns.is_empty() {
            return Err(NexusError::Schema(
                "linkedin-ads: account_urns must not be empty".into(),
            ));
        }
        let fields: Vec<Field> = cfg
            .fields
            .iter()
            .map(|f| Field::new(f, DataType::Utf8, true))
            .collect();
        Ok(Self {
            client: reqwest::Client::new(),
            cfg: cfg.clone(),
            schema: Arc::new(Schema::new(fields)),
        })
    }

    fn project(&self, object: &Value) -> Value {
        let mut out = serde_json::Map::with_capacity(self.cfg.fields.len());
        for name in &self.cfg.fields {
            let value = match object.get(name) {
                Some(Value::String(s)) => Value::String(s.clone()),
                Some(Value::Null) | None => Value::Null,
                Some(other) => Value::String(other.to_string()),
            };
            out.insert(name.clone(), value);
        }
        Value::Object(out)
    }
}

fn date_params(prefix: &str, date: (u16, u8, u8)) -> [(String, String); 3] {
    [
        (format!("dateRange.{prefix}.year"), date.0.to_string()),
        (format!("dateRange.{prefix}.month"), date.1.to_string()),
        (format!("dateRange.{prefix}.day"), date.2.to_string()),
    ]
}

#[async_trait]
impl Source for LinkedinAdsSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let access_token = with_timeout(
            self.cfg.timeout_seconds,
            "linkedin-ads resolve token",
            async { resolve_access_token(&self.client, &self.cfg).await },
        )
        .await?;

        let url = format!("{}/rest/adAnalytics", self.cfg.api_base_url);
        let accounts = format!("List({})", self.cfg.account_urns.join(","));

        let mut params: Vec<(String, String)> = vec![
            ("q".to_string(), self.cfg.finder.clone()),
            ("pivot".to_string(), self.cfg.pivot.clone()),
            ("fields".to_string(), self.cfg.fields.join(",")),
            ("accounts".to_string(), accounts),
        ];
        params.extend(date_params("start", self.cfg.start_date));
        params.extend(date_params("end", self.cfg.end_date));

        let response: AdAnalyticsResponse = with_timeout(
            self.cfg.timeout_seconds,
            "linkedin-ads adAnalytics",
            async {
                let response = self
                    .client
                    .get(&url)
                    .bearer_auth(&access_token)
                    .header("LinkedIn-Version", &self.cfg.linkedin_version)
                    .header("X-Restli-Protocol-Version", "2.0.0")
                    .query(&params)
                    .send()
                    .await
                    .map_err(|e| {
                        NexusError::Connector(format!(
                            "linkedin-ads adAnalytics request failed: {e}"
                        ))
                    })?;

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    return Err(NexusError::Connector(format!(
                        "linkedin-ads adAnalytics failed ({status}): {text}"
                    )));
                }

                response.json().await.map_err(|e| {
                    NexusError::Connector(format!(
                        "linkedin-ads adAnalytics response parse failed: {e}"
                    ))
                })
            },
        )
        .await?;

        let projected: Vec<Value> = response.elements.iter().map(|e| self.project(e)).collect();
        let batch = RecordBatchBuilder::from_json_rows(self.schema.clone(), &projected)?;
        Ok(Box::pin(stream::iter(vec![Ok(batch)])))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
