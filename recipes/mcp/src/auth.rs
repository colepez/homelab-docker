//! OAuth 2.1 resource-server side of the MCP auth spec: the MCP endpoint only accepts Keycloak-issued access tokens.
//!
//! Unauthenticated requests get `401` with `WWW-Authenticate: Bearer resource_metadata=...`, which points the client
//! (claude.ai) at `/.well-known/oauth-protected-resource`; that document names Keycloak as the authorization server.
//! Tokens are verified locally: RS256 signature against Keycloak's JWKS (fetched over the internal network and cached),
//! issuer, audience (set by the client's audience mapper) and expiry, then the user must hold one of the allowed realm roles.

use std::{collections::HashMap, sync::Arc, time::{Duration, Instant}};

use axum::{
    Json,
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header, jwk::JwkSet};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::RwLock;

pub struct Auth {
    pub issuer: String,
    pub jwks_url: String,
    pub audience: String,
    /// Any one of these realm roles grants access
    pub allowed_roles: Vec<String>,
    /// Public URL of the MCP endpoint (the OAuth "resource")
    pub resource: String,
    pub http: reqwest::Client,
    keys: RwLock<Keys>,
}

#[derive(Default)]
struct Keys {
    by_kid: HashMap<String, DecodingKey>,
    fetched: Option<Instant>,
}

#[derive(Deserialize)]
struct Claims {
    preferred_username: Option<String>,
    #[serde(default)]
    realm_access: RealmAccess,
}

#[derive(Deserialize, Default)]
struct RealmAccess {
    #[serde(default)]
    roles: Vec<String>,
}

type Denied = (StatusCode, &'static str);

impl Auth {
    pub fn new(issuer: String, jwks_url: String, audience: String, allowed_roles: Vec<String>, resource: String, http: reqwest::Client) -> Self {
        Self { issuer, jwks_url, audience, allowed_roles, resource, http, keys: RwLock::new(Keys::default()) }
    }

    pub fn metadata_url(&self) -> String {
        // RFC 9728: well-known path inserted before the resource's path component
        let (origin, path) = split_origin(&self.resource);
        format!("{origin}/.well-known/oauth-protected-resource{path}")
    }

    /// Refetch the JWKS (at most every 30s, so bogus `kid`s can't hammer Keycloak).
    async fn refresh(&self) {
        if self.keys.read().await.fetched.is_some_and(|t| t.elapsed() < Duration::from_secs(30)) {
            return;
        }
        let set: Result<JwkSet, String> = async {
            let resp = self.http.get(&self.jwks_url).send().await.map_err(|e| e.to_string())?;
            resp.json().await.map_err(|e| e.to_string())
        }
        .await;
        let mut keys = self.keys.write().await;
        keys.fetched = Some(Instant::now());
        match set {
            Ok(set) => {
                keys.by_kid = set
                    .keys
                    .iter()
                    .filter_map(|jwk| Some((jwk.common.key_id.clone()?, DecodingKey::from_jwk(jwk).ok()?)))
                    .collect();
                tracing::info!(keys = keys.by_kid.len(), "loaded JWKS");
            }
            Err(e) => tracing::warn!("JWKS fetch failed: {e}"),
        }
    }

    async fn key(&self, kid: &str) -> Option<DecodingKey> {
        if let Some(k) = self.keys.read().await.by_kid.get(kid) {
            return Some(k.clone());
        }
        self.refresh().await;
        self.keys.read().await.by_kid.get(kid).cloned()
    }

    pub async fn check(&self, token: &str) -> Result<String, Denied> {
        let invalid = (StatusCode::UNAUTHORIZED, "invalid_token");
        let header = decode_header(token).map_err(|_| invalid)?;
        let kid = header.kid.ok_or(invalid)?;
        let key = self.key(&kid).await.ok_or(invalid)?;
        let mut v = Validation::new(Algorithm::RS256);
        v.set_issuer(&[&self.issuer]);
        v.set_audience(&[&self.audience]);
        v.leeway = 30;
        let claims = decode::<Claims>(token, &key, &v).map_err(|e| {
            tracing::info!("rejected token: {e}");
            invalid
        })?;
        let claims = claims.claims;
        let user = claims.preferred_username.unwrap_or_else(|| "?".into());
        if !claims.realm_access.roles.iter().any(|r| self.allowed_roles.contains(r)) {
            tracing::warn!(user, "token lacks an allowed role");
            return Err((StatusCode::FORBIDDEN, "insufficient_scope"));
        }
        Ok(user)
    }

    fn challenge(&self, (status, error): (StatusCode, Option<&str>)) -> Response {
        let mut value = format!("Bearer resource_metadata=\"{}\"", self.metadata_url());
        if let Some(e) = error {
            value.push_str(&format!(", error=\"{e}\""));
        }
        (status, [(header::WWW_AUTHENTICATE, value)]).into_response()
    }
}

fn split_origin(url: &str) -> (&str, &str) {
    let after_scheme = url.find("://").map(|i| i + 3).unwrap_or(0);
    match url[after_scheme..].find('/') {
        Some(i) => url.split_at(after_scheme + i),
        None => (url, ""),
    }
}

/// axum middleware guarding the MCP endpoint
pub async fn require_auth(State(auth): State<Arc<Auth>>, req: Request, next: Next) -> Response {
    let token = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")))
        .map(str::trim);
    match token {
        None => auth.challenge((StatusCode::UNAUTHORIZED, None)),
        Some(t) => match auth.check(t).await {
            Ok(user) => {
                tracing::debug!(user, "authorized");
                next.run(req).await
            }
            Err((status, err)) => auth.challenge((status, Some(err))),
        },
    }
}

/// RFC 9728 protected resource metadata
pub async fn protected_resource_metadata(State(auth): State<Arc<Auth>>) -> impl IntoResponse {
    Json(json!({
        "resource": auth.resource,
        "authorization_servers": [auth.issuer],
        "bearer_methods_supported": ["header"],
        "scopes_supported": ["openid", "offline_access"],
        "resource_name": "Recipes (Mealie)",
    }))
}
