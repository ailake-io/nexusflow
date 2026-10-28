use crate::auth::SalesforceSession;
use crate::config::SalesforceConnectorConfig;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use nexus_core::NexusError;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
struct DescribeResponse {
    fields: Vec<DescribeField>,
}

#[derive(Deserialize)]
struct DescribeField {
    name: String,
    #[serde(rename = "type")]
    field_type: String,
}

/// `GET sobjects/{name}/describe` — real field list + types, needed
/// because SOQL has no `SELECT *`: the source builds its default query
/// from this, and every connector needs an Arrow schema up front.
///
/// Compound fields (`address`, `location`) are skipped — they don't
/// map to a single flat CSV column the way Bulk API 2.0 returns rows,
/// unlike every other Salesforce field type. Their component fields
/// (e.g. `BillingStreet`, `BillingCity`) are separate top-level fields
/// already included on their own.
pub(crate) async fn describe_sobject(
    client: &reqwest::Client,
    session: &SalesforceSession,
    cfg: &SalesforceConnectorConfig,
) -> Result<(SchemaRef, Vec<String>), NexusError> {
    let url = format!(
        "{}/services/data/{}/sobjects/{}/describe",
        session.instance_url, cfg.api_version, cfg.sobject
    );
    let response = client
        .get(&url)
        .bearer_auth(&session.access_token)
        .send()
        .await
        .map_err(|e| NexusError::Connector(format!("salesforce describe request failed: {e}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(NexusError::Connector(format!(
            "salesforce describe({}) failed ({status}): {body}",
            cfg.sobject
        )));
    }

    let parsed: DescribeResponse = response
        .json()
        .await
        .map_err(|e| NexusError::Connector(format!("salesforce describe parse failed: {e}")))?;

    let mut names = Vec::with_capacity(parsed.fields.len());
    let mut fields = Vec::with_capacity(parsed.fields.len());
    for f in parsed.fields {
        if matches!(f.field_type.as_str(), "address" | "location") {
            continue;
        }
        fields.push(Field::new(
            &f.name,
            salesforce_type_to_arrow(&f.field_type),
            true,
        ));
        names.push(f.name);
    }

    Ok((Arc::new(Schema::new(fields)), names))
}

/// Maps a Salesforce field `type` (from `describe()`) to an Arrow type.
/// Every numeric-ish type maps to a real numeric Arrow type; everything
/// else (including `date`/`datetime`, which have no dedicated Arrow
/// mapping in v1 — same simplification `nexus-connector-excel` already
/// documents for its own datetime cells) falls back to `Utf8`, which is
/// always a safe representation of whatever Bulk API's CSV puts there.
fn salesforce_type_to_arrow(sf_type: &str) -> DataType {
    match sf_type {
        "int" => DataType::Int64,
        "double" | "currency" | "percent" => DataType::Float64,
        "boolean" => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_numeric_types() {
        assert_eq!(salesforce_type_to_arrow("int"), DataType::Int64);
        assert_eq!(salesforce_type_to_arrow("double"), DataType::Float64);
        assert_eq!(salesforce_type_to_arrow("currency"), DataType::Float64);
        assert_eq!(salesforce_type_to_arrow("percent"), DataType::Float64);
        assert_eq!(salesforce_type_to_arrow("boolean"), DataType::Boolean);
    }

    #[test]
    fn falls_back_to_utf8_for_everything_else() {
        for t in [
            "string",
            "id",
            "reference",
            "picklist",
            "textarea",
            "email",
            "date",
            "datetime",
        ] {
            assert_eq!(salesforce_type_to_arrow(t), DataType::Utf8);
        }
    }
}
