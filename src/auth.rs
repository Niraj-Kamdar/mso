use dashmap::DashMap;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;

use crate::config::MAX_ACTIVE_KEYS;
use crate::db::queries::{self, ApiKeyRecord};
use crate::db::DbManager;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKeyScope {
    Read,
    Write,
}

impl ApiKeyScope {
    pub fn as_str(self) -> &'static str {
        match self {
            ApiKeyScope::Read => "read",
            ApiKeyScope::Write => "write",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "read" => Some(ApiKeyScope::Read),
            "write" => Some(ApiKeyScope::Write),
            _ => None,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ApiKeyMetadata {
    pub id: String,
    pub app_name: String,
    pub expires_at: Option<i64>,
    pub is_revoked: bool,
    pub scope: ApiKeyScope,
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("Missing x-api-key authentication header")]
    MissingKey,
    #[error("Invalid or unrecognized API key")]
    InvalidKey,
    #[error("API key has expired")]
    ExpiredKey,
    #[error("API key has been revoked")]
    RevokedKey,
    #[error("API key requires write scope for this operation")]
    InsufficientScope,
    #[error("Maximum active API key quota reached (limit: {0})")]
    QuotaExceeded(usize),
    #[error("Database error: {0}")]
    DatabaseError(String),
}

#[derive(Clone)]
pub struct AuthManager {
    db: DbManager,
    // Key: SHA256 hex string -> ApiKeyMetadata
    cache: Arc<DashMap<String, ApiKeyMetadata>>,
}

impl AuthManager {
    pub fn new(db: DbManager) -> Result<Self, AuthError> {
        let auth = Self {
            db,
            cache: Arc::new(DashMap::new()),
        };
        auth.reload_cache()?;
        Ok(auth)
    }

    pub fn reload_cache(&self) -> Result<(), AuthError> {
        let keys = self
            .db
            .with_conn(|conn| queries::list_all_keys(conn))
            .map_err(|e| AuthError::DatabaseError(e.to_string()))?;

        self.cache.clear();
        for k in keys {
            let scope = ApiKeyScope::from_str(&k.scope).unwrap_or(ApiKeyScope::Read);
            self.cache.insert(
                k.key_hash,
                ApiKeyMetadata {
                    id: k.id,
                    app_name: k.app_name,
                    expires_at: k.expires_at,
                    is_revoked: k.is_revoked,
                    scope,
                },
            );
        }
        Ok(())
    }

    pub fn authenticate(&self, raw_key: &str) -> Result<ApiKeyMetadata, AuthError> {
        if raw_key.trim().is_empty() {
            return Err(AuthError::MissingKey);
        }

        let hash = hash_key(raw_key.trim());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        if let Some(entry) = self.cache.get(&hash) {
            let meta = entry.value();
            if meta.is_revoked {
                return Err(AuthError::RevokedKey);
            }
            if let Some(exp) = meta.expires_at {
                if now > exp {
                    return Err(AuthError::ExpiredKey);
                }
            }
            return Ok(meta.clone());
        }

        Err(AuthError::InvalidKey)
    }

    pub fn create_key(
        &self,
        app_name: &str,
        ttl_ms: Option<i64>,
        scope: ApiKeyScope,
    ) -> Result<(String, String), AuthError> {
        let active_count = self
            .db
            .with_conn(|conn| queries::count_active_keys(conn))
            .map_err(|e| AuthError::DatabaseError(e.to_string()))?;

        if active_count >= MAX_ACTIVE_KEYS {
            return Err(AuthError::QuotaExceeded(MAX_ACTIVE_KEYS));
        }

        let id = format!("key_{}", Uuid::new_v4().simple());
        let raw_secret = format!("pnl_live_{}", Uuid::new_v4().simple());
        let key_hash = hash_key(&raw_secret);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let expires_at = ttl_ms.map(|ttl| now + ttl);

        self.db
            .with_conn(|conn| {
                queries::insert_api_key(conn, &id, app_name, &key_hash, now, expires_at, scope.as_str())
            })
            .map_err(|e| AuthError::DatabaseError(e.to_string()))?;

        self.cache.insert(
            key_hash,
            ApiKeyMetadata {
                id: id.clone(),
                app_name: app_name.to_string(),
                expires_at,
                is_revoked: false,
                scope,
            },
        );

        Ok((id, raw_secret))
    }

    pub fn authenticate_with_scope(&self, raw_key: &str, required_scope: ApiKeyScope) -> Result<ApiKeyMetadata, AuthError> {
        let meta = self.authenticate(raw_key)?;
        // Strict scope enforcement: read keys can only read, write keys can only write
        if meta.scope != required_scope {
            return Err(AuthError::InsufficientScope);
        }
        Ok(meta)
    }

    pub fn revoke_key(&self, id: &str) -> Result<bool, AuthError> {
        let revoked = self
            .db
            .with_conn(|conn| queries::revoke_api_key(conn, id))
            .map_err(|e| AuthError::DatabaseError(e.to_string()))?;

        if revoked {
            self.reload_cache()?;
        }
        Ok(revoked)
    }

    pub fn list_keys(&self) -> Result<Vec<ApiKeyRecord>, AuthError> {
        self.db
            .with_conn(|conn| queries::list_all_keys(conn))
            .map_err(|e| AuthError::DatabaseError(e.to_string()))
    }
}

pub fn hash_key(raw_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_key.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_workflow() {
        let db = DbManager::in_memory().unwrap();
        let auth = AuthManager::new(db.clone()).unwrap();

        // 1. Create read key
        let (id, secret) = auth.create_key("test-app", Some(60_000), ApiKeyScope::Read).unwrap();
        assert!(id.starts_with("key_"));
        assert!(secret.starts_with("pnl_live_"));

        // 2. Authenticate valid key
        let meta = auth.authenticate(&secret).unwrap();
        assert_eq!(meta.id, id);
        assert_eq!(meta.app_name, "test-app");
        assert_eq!(meta.scope, ApiKeyScope::Read);

        // 3. Authenticate invalid key
        assert!(matches!(auth.authenticate("invalid_key"), Err(AuthError::InvalidKey)));

        // 4. Revoke key
        let revoked = auth.revoke_key(&id).unwrap();
        assert!(revoked);

        // 5. Authenticate revoked key
        assert!(matches!(auth.authenticate(&secret), Err(AuthError::RevokedKey)));

        // 6. Test write scope with fresh AuthManager (Issue #15)
        let (wid, wsecret) = auth.create_key("writer-app", None, ApiKeyScope::Write).unwrap();

        // Reload cache from DB to ensure consistency
        let auth2 = AuthManager::new(db).unwrap();
        let wmeta = auth2.authenticate_with_scope(&wsecret, ApiKeyScope::Write).unwrap();
        assert_eq!(wmeta.scope, ApiKeyScope::Write);
        assert_eq!(wmeta.id, wid);

        // 7. Test insufficient scope
        let (_rid, rsecret) = auth2.create_key("reader-app", None, ApiKeyScope::Read).unwrap();
        let auth3 = AuthManager::new(auth2.db.clone()).unwrap();
        assert!(matches!(
            auth3.authenticate_with_scope(&rsecret, ApiKeyScope::Write),
            Err(AuthError::InsufficientScope)
        ));
    }

    #[test]
    fn test_quota_limit() {
        let db = DbManager::in_memory().unwrap();
        let auth = AuthManager::new(db).unwrap();

        for i in 0..MAX_ACTIVE_KEYS {
            let res = auth.create_key(&format!("app_{}", i), None, ApiKeyScope::Read);
            assert!(res.is_ok());
        }

        // 101th key should fail quota
        let overflow = auth.create_key("overflow", None, ApiKeyScope::Read);
        assert!(matches!(overflow, Err(AuthError::QuotaExceeded(_))));
    }
}
