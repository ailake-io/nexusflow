use crate::error::NexusError;
use arrow_array::builder::StringBuilder;
use arrow_array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashSet;
use std::sync::Arc;

type HmacSha256 = Hmac<Sha256>;

/// One column to tokenize (Fase 28) — deterministic, one-way: the same
/// input value always produces the same token (so `GROUP BY`/joins on the
/// token still work in a downstream Transform SQL node), but the original
/// value cannot be recovered from the token without the installation-wide
/// salt (`NEXUS_MASKING_SALT`, see `ColumnMasker::new`). This is
/// tokenization, not encryption — there is no "unmask" operation anywhere
/// in this crate, matching the decision made for this feature (deliberately
/// weaker than `crypto.rs::SecretCipher`'s reversible AES-256-GCM, on
/// purpose: a masked data value should never come back, a connector
/// credential sometimes legitimately needs to).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMaskingSpec {
    pub column: String,
}

/// Deterministically tokenizes configured columns of a `RecordBatch` via
/// HMAC-SHA256 keyed by an installation-wide salt. Every masked column
/// becomes `Utf8` in the output regardless of its original Arrow type — a
/// token is a token, and this project's connectors don't otherwise need a
/// masked numeric column (e.g. a CPF stored as an int) to stay numeric
/// downstream (see `mask_array`'s doc comment for exactly which types are
/// supported).
///
/// `Clone` is cheap — `salt`/`columns` are both `Arc`-wrapped so this can
/// be captured by a `nexus_core::pipeline::BatchTransform` closure and
/// cloned once per batch the same way `nexus-ai`'s embedding backend
/// already is (see `runner.rs::run_passthrough_pipeline`).
#[derive(Clone)]
pub struct ColumnMasker {
    salt: Arc<Vec<u8>>,
    columns: Arc<HashSet<String>>,
}

impl ColumnMasker {
    /// `salt` is the raw bytes of `NEXUS_MASKING_SALT` — any length works
    /// (HMAC itself hashes/pads a key of any size), but a short or
    /// well-known salt makes tokens for low-cardinality values (e.g. a
    /// 9-digit SSN) practically reversible via a rainbow table; this
    /// function doesn't enforce a minimum length itself (see the caller in
    /// `nexus-server::lib.rs` for where that's validated instead — this
    /// crate has no I/O and no env vars of its own, CLAUDE.md §8.3).
    pub fn new(specs: &[ColumnMaskingSpec], salt: &[u8]) -> Self {
        Self {
            salt: Arc::new(salt.to_vec()),
            columns: Arc::new(specs.iter().map(|s| s.column.clone()).collect()),
        }
    }

    fn token_for(&self, value: &str) -> String {
        let mut mac =
            HmacSha256::new_from_slice(&self.salt).expect("HMAC-SHA256 accepts a key of any length");
        mac.update(value.as_bytes());
        let digest = mac.finalize().into_bytes();
        // Truncated to 16 bytes (32 hex chars) of the 32-byte digest — a
        // token only needs to be stable and, for any realistic column
        // cardinality, collision-free; 128 bits of collision resistance is
        // far more than that needs, and a shorter token is friendlier to
        // store/index/join on downstream.
        digest.iter().take(16).map(|b| format!("{b:02x}")).collect()
    }

    /// Replaces every configured column with its token; every other column
    /// passes through unchanged (same `ArrayRef`, not copied). A configured
    /// column that doesn't exist in `batch` is silently skipped — masking
    /// is a safety net that must survive a pipeline's whole lifetime, and a
    /// column temporarily absent (e.g. mid schema-migration) must never
    /// turn into either a hard failure (which would just mean *no* masking
    /// happens to anything, worse than partial masking) or a false
    /// impression that something was masked when it wasn't; a real
    /// column rename should surface via `pipeline_schema_store`'s drift
    /// detection instead, not this function erroring.
    pub fn mask_batch(&self, batch: RecordBatch) -> Result<RecordBatch, NexusError> {
        if self.columns.is_empty() {
            return Ok(batch);
        }
        let schema = batch.schema();
        let new_schema = self.mask_schema(&schema);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for (i, field) in schema.fields().iter().enumerate() {
            let array = batch.column(i);
            if self.columns.contains(field.name()) {
                columns.push(self.mask_array(array)?);
            } else {
                columns.push(array.clone());
            }
        }
        RecordBatch::try_new(new_schema, columns)
            .map_err(|e| NexusError::Schema(format!("failed to rebuild batch after masking: {e}")))
    }

    /// The schema `mask_batch` would produce for a batch of this shape —
    /// every masked column becomes `Utf8`, everything else is untouched.
    /// Exposed separately so a caller that needs the post-masking schema
    /// without any actual data in hand yet (e.g. a source that produced
    /// zero batches this run, but whose schema still needs to be correct
    /// for a downstream Transform SQL node to resolve column references
    /// against) doesn't have to fabricate a batch just to get one.
    pub fn mask_schema(&self, schema: &Schema) -> Arc<Schema> {
        if self.columns.is_empty() {
            return Arc::new(schema.clone());
        }
        let fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|f| {
                if self.columns.contains(f.name()) {
                    Field::new(f.name(), DataType::Utf8, true)
                } else {
                    (**f).clone()
                }
            })
            .collect();
        Arc::new(Schema::new(fields))
    }

    /// Covers the primitive types this project's connectors actually
    /// produce (same coverage `nexus_core::quality`'s own `display_value`
    /// has, reimplemented here since that one is private and keyed off a
    /// `(RecordBatch, col_idx)` pair rather than a plain `ArrayRef`) — an
    /// unsupported array type is hashed via its own `Debug` rendering
    /// rather than erroring, same "never lose the value, worst case
    /// stringify it" posture the bridging connectors already apply.
    fn mask_array(&self, array: &ArrayRef) -> Result<ArrayRef, NexusError> {
        let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 32);
        for row in 0..array.len() {
            if array.is_null(row) {
                // A null stays null, never given a token — a "token for
                // null" would let a downstream GROUP BY/join treat every
                // null as the same real value, which is wrong.
                builder.append_null();
                continue;
            }
            let display = match array.data_type() {
                DataType::Utf8 => array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .map(|a| a.value(row).to_string()),
                DataType::Int64 => array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|a| a.value(row).to_string()),
                DataType::Float64 => array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .map(|a| a.value(row).to_string()),
                DataType::Boolean => array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .map(|a| a.value(row).to_string()),
                _ => None,
            }
            .unwrap_or_else(|| format!("{array:?}[{row}]"));
            builder.append_value(self.token_for(&display));
        }
        Ok(Arc::new(builder.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;

    fn batch(ids: &[i64], emails: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("email", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(emails.to_vec())),
            ],
        )
        .unwrap()
    }

    fn spec(column: &str) -> ColumnMaskingSpec {
        ColumnMaskingSpec {
            column: column.to_string(),
        }
    }

    #[test]
    fn masks_configured_column_leaving_others_untouched() {
        let masker = ColumnMasker::new(&[spec("email")], b"test-salt");
        let b = batch(&[1, 2], &["a@example.com", "b@example.com"]);
        let masked = masker.mask_batch(b).unwrap();

        assert_eq!(masked.schema().field(0).name(), "id");
        assert_eq!(masked.schema().field(1).name(), "email");
        assert_eq!(*masked.schema().field(1).data_type(), DataType::Utf8);

        let ids = masked.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.value(0), 1, "unmasked column stays exactly as-is");

        let emails = masked
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_ne!(emails.value(0), "a@example.com", "value must be tokenized");
        assert_eq!(emails.value(0).len(), 32, "16 bytes hex-encoded");
    }

    #[test]
    fn same_input_produces_the_same_token_every_time() {
        let masker = ColumnMasker::new(&[spec("email")], b"test-salt");
        let b1 = masker
            .mask_batch(batch(&[1], &["a@example.com"]))
            .unwrap();
        let b2 = masker
            .mask_batch(batch(&[99], &["a@example.com"]))
            .unwrap();
        let e1 = b1.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let e2 = b2.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(e1.value(0), e2.value(0), "deterministic: same value, same token");
    }

    #[test]
    fn different_inputs_produce_different_tokens() {
        let masker = ColumnMasker::new(&[spec("email")], b"test-salt");
        let masked = masker
            .mask_batch(batch(&[1, 2], &["a@example.com", "b@example.com"]))
            .unwrap();
        let emails = masked.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_ne!(emails.value(0), emails.value(1));
    }

    #[test]
    fn different_salt_produces_a_different_token_for_the_same_value() {
        let a = ColumnMasker::new(&[spec("email")], b"salt-a")
            .mask_batch(batch(&[1], &["a@example.com"]))
            .unwrap();
        let b = ColumnMasker::new(&[spec("email")], b"salt-b")
            .mask_batch(batch(&[1], &["a@example.com"]))
            .unwrap();
        let a_email = a.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let b_email = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_ne!(
            a_email.value(0),
            b_email.value(0),
            "the original value is not recoverable without the exact salt"
        );
    }

    #[test]
    fn null_values_stay_null_not_a_token() {
        let schema = Arc::new(Schema::new(vec![Field::new("email", DataType::Utf8, true)]));
        let b = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("a@example.com"),
                None,
            ]))],
        )
        .unwrap();
        let masker = ColumnMasker::new(&[spec("email")], b"test-salt");
        let masked = masker.mask_batch(b).unwrap();
        let emails = masked.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(emails.is_null(1));
    }

    #[test]
    fn missing_configured_column_is_skipped_not_an_error() {
        let masker = ColumnMasker::new(&[spec("does_not_exist")], b"test-salt");
        let b = batch(&[1], &["a@example.com"]);
        let masked = masker.mask_batch(b).unwrap();
        assert_eq!(masked.num_columns(), 2, "batch passes through unchanged");
    }

    #[test]
    fn empty_spec_list_is_a_no_op() {
        let masker = ColumnMasker::new(&[], b"test-salt");
        let b = batch(&[1], &["a@example.com"]);
        let masked = masker.mask_batch(b).unwrap();
        let emails = masked.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(emails.value(0), "a@example.com");
    }

    #[test]
    fn mask_schema_matches_mask_batchs_schema_with_zero_rows() {
        let masker = ColumnMasker::new(&[spec("email")], b"test-salt");
        let schema = batch(&[], &[]).schema();
        let via_schema = masker.mask_schema(&schema);
        let via_batch = masker.mask_batch(batch(&[], &[])).unwrap();
        assert_eq!(*via_schema, *via_batch.schema());
        assert_eq!(*via_schema.field(1).data_type(), DataType::Utf8);
    }

    #[test]
    fn masks_a_numeric_column_by_its_string_rendering() {
        let masker = ColumnMasker::new(&[spec("id")], b"test-salt");
        let b = batch(&[12345, 12345], &["a@example.com", "b@example.com"]);
        let masked = masker.mask_batch(b).unwrap();
        assert_eq!(*masked.schema().field(0).data_type(), DataType::Utf8);
        let ids = masked.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(ids.value(0), ids.value(1), "same int value -> same token");
    }
}
