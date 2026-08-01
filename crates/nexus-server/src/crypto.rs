use aes_gcm::aead::{Aead, AeadCore, OsRng};
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};

const NONCE_LEN: usize = 12;

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("NEXUS_ENCRYPTION_KEY must be a 64-character hex string (32 raw bytes): {0}")]
    InvalidKey(String),
    #[error("ciphertext is malformed or was not produced with this key")]
    Decrypt,
}

/// Encrypts connector secrets (URIs, passwords, tokens) before they're
/// persisted — CLAUDE.md §5 / IMPLEMENTATION_PLAN.md Marco 7: "Segredos:
/// AES-256-GCM antes de persistir credencial de conector; chave via env var
/// no MVP (débito de KMS documentado, não resolvido aqui)". The KMS debt is
/// real: this is a single static key from an env var, not per-tenant or
/// rotatable — acceptable for the MVP self-host scale, not for multi-tenant
/// SaaS.
#[derive(Clone)]
pub struct SecretCipher {
    cipher: Aes256Gcm,
}

impl SecretCipher {
    pub fn new(key_bytes: &[u8; 32]) -> Self {
        Self {
            cipher: Aes256Gcm::new(&Key::<Aes256Gcm>::from(*key_bytes)),
        }
    }

    /// `hex_key` must decode to exactly 32 bytes (AES-256). Generate one via
    /// `openssl rand -hex 32`.
    pub fn from_hex_key(hex_key: &str) -> Result<Self, CryptoError> {
        let bytes = hex::decode(hex_key).map_err(|e| CryptoError::InvalidKey(e.to_string()))?;
        let key_bytes: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
            CryptoError::InvalidKey(format!("expected 32 bytes, got {}", bytes.len()))
        })?;
        Ok(Self::new(&key_bytes))
    }

    /// Returns `hex(nonce || ciphertext)` — a fresh random nonce every call,
    /// so encrypting the same plaintext twice yields different output.
    pub fn encrypt(&self, plaintext: &str) -> String {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = self
            .cipher
            .encrypt(&nonce, plaintext.as_bytes())
            .expect("AES-256-GCM encryption cannot fail for a validly-constructed cipher");
        let mut out = nonce.to_vec();
        out.extend_from_slice(&ciphertext);
        hex::encode(out)
    }

    pub fn decrypt(&self, encoded: &str) -> Result<String, CryptoError> {
        let raw = hex::decode(encoded).map_err(|_| CryptoError::Decrypt)?;
        if raw.len() < NONCE_LEN {
            return Err(CryptoError::Decrypt);
        }
        let (nonce_bytes, ciphertext) = raw.split_at(NONCE_LEN);
        let nonce_bytes: [u8; NONCE_LEN] =
            nonce_bytes.try_into().map_err(|_| CryptoError::Decrypt)?;
        let plaintext = self
            .cipher
            .decrypt(&Nonce::from(nonce_bytes), ciphertext)
            .map_err(|_| CryptoError::Decrypt)?;
        String::from_utf8(plaintext).map_err(|_| CryptoError::Decrypt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cipher() -> SecretCipher {
        SecretCipher::from_hex_key(&"01".repeat(32)).unwrap()
    }

    #[test]
    fn round_trips_plaintext() {
        let c = cipher();
        let encoded = c.encrypt("postgres://user:hunter2@db.internal/prod");
        assert_eq!(
            c.decrypt(&encoded).unwrap(),
            "postgres://user:hunter2@db.internal/prod"
        );
    }

    #[test]
    fn same_plaintext_encrypts_differently_each_time() {
        let c = cipher();
        let a = c.encrypt("same-secret");
        let b = c.encrypt("same-secret");
        assert_ne!(a, b, "nonce must be fresh per call");
        assert_eq!(c.decrypt(&a).unwrap(), "same-secret");
        assert_eq!(c.decrypt(&b).unwrap(), "same-secret");
    }

    #[test]
    fn tampered_ciphertext_fails_to_decrypt() {
        let c = cipher();
        let mut encoded = c.encrypt("secret-value");
        // Flip one hex nibble past the nonce, inside the ciphertext/tag.
        let mutated_char = if &encoded[24..25] == "0" { '1' } else { '0' };
        encoded.replace_range(24..25, &mutated_char.to_string());
        assert!(matches!(c.decrypt(&encoded), Err(CryptoError::Decrypt)));
    }

    #[test]
    fn wrong_key_fails_to_decrypt() {
        let a = SecretCipher::from_hex_key(&"11".repeat(32)).unwrap();
        let b = SecretCipher::from_hex_key(&"22".repeat(32)).unwrap();
        let encoded = a.encrypt("secret-value");
        assert!(matches!(b.decrypt(&encoded), Err(CryptoError::Decrypt)));
    }

    #[test]
    fn rejects_key_of_wrong_length() {
        assert!(matches!(
            SecretCipher::from_hex_key(&"11".repeat(16)),
            Err(CryptoError::InvalidKey(_))
        ));
    }

    #[test]
    fn rejects_non_hex_key() {
        assert!(matches!(
            SecretCipher::from_hex_key("not hex at all"),
            Err(CryptoError::InvalidKey(_))
        ));
    }

    #[test]
    fn rejects_garbage_ciphertext() {
        let c = cipher();
        assert!(matches!(c.decrypt("not hex"), Err(CryptoError::Decrypt)));
        assert!(matches!(c.decrypt("deadbeef"), Err(CryptoError::Decrypt)));
    }
}
