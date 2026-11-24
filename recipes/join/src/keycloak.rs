//! Keycloak admin calls with the group-scoped service account (client credentials). Its only power is the
//! fine-grained permission on group Mealie (view, manage-members, manage-membership): it can create users *into* that
//! group and manage those members, nothing else (see scripts/kc-setup-join.py and kc-join-perms-selftest.py).

use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::sync::Mutex;

pub struct Keycloak {
    http: reqwest::Client,
    base: String,
    realm: String,
    client_id: String,
    secret: String,
    group: String,
    login_client: String,
    login_redirect: String,
    token: Mutex<Option<(String, Instant)>>,
}

pub enum Created {
    Ok(String),
    Exists,
}

fn enc(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

impl Keycloak {
    #[allow(clippy::too_many_arguments)]
    pub fn new(http: reqwest::Client, base: String, realm: String, client_id: String, secret: String, group: String,
               login_client: String, login_redirect: String) -> Self {
        Self { http, base: base.trim_end_matches('/').to_owned(), realm, client_id, secret, group, login_client, login_redirect, token: Mutex::new(None) }
    }

    async fn token(&self) -> anyhow::Result<String> {
        let mut t = self.token.lock().await;
        if let Some((tok, until)) = t.as_ref() {
            if *until > Instant::now() {
                return Ok(tok.clone());
            }
        }
        let body = format!("grant_type=client_credentials&client_id={}&client_secret={}", enc(&self.client_id), enc(&self.secret));
        let resp = self
            .http
            .post(format!("{}/realms/{}/protocol/openid-connect/token", self.base, self.realm))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await?;
        anyhow::ensure!(resp.status().is_success(), "service account login failed: {}", resp.status());
        let v: Value = resp.json().await?;
        let tok = v["access_token"].as_str().ok_or_else(|| anyhow::anyhow!("no access_token"))?.to_owned();
        let ttl = v["expires_in"].as_u64().unwrap_or(60).saturating_sub(30).max(10);
        *t = Some((tok.clone(), Instant::now() + Duration::from_secs(ttl)));
        Ok(tok)
    }

    fn admin(&self, path: &str) -> String {
        format!("{}/admin/realms/{}{}", self.base, self.realm, path)
    }

    /// Create an enabled user directly in the group. Username = email (unique; also the login).
    pub async fn create_user(&self, email: &str, first: &str, last: &str) -> anyhow::Result<Created> {
        let tok = self.token().await?;
        let resp = self
            .http
            .post(self.admin("/users"))
            .bearer_auth(&tok)
            .json(&json!({
                "username": email, "email": email, "firstName": first, "lastName": last,
                "enabled": true, "emailVerified": true, "groups": [self.group],
            }))
            .send()
            .await?;
        match resp.status().as_u16() {
            201 => {
                let loc = resp.headers().get("location").and_then(|v| v.to_str().ok()).unwrap_or_default();
                let id = loc.rsplit('/').next().unwrap_or_default().to_owned();
                anyhow::ensure!(!id.is_empty(), "no user id in Location");
                Ok(Created::Ok(id))
            }
            409 => Ok(Created::Exists),
            s => anyhow::bail!("create user: {s} {}", resp.text().await.unwrap_or_default().chars().take(200).collect::<String>()),
        }
    }

    /// Email the user a link to set their password (valid 3 days), landing on the recipes site afterwards.
    pub async fn send_set_password(&self, user_id: &str) -> anyhow::Result<()> {
        let tok = self.token().await?;
        let url = format!("{}?client_id={}&redirect_uri={}&lifespan=259200", self.admin(&format!("/users/{user_id}/execute-actions-email")),
                          enc(&self.login_client), enc(&self.login_redirect));
        let resp = self.http.put(url).bearer_auth(&tok).json(&json!(["UPDATE_PASSWORD"])).send().await?;
        anyhow::ensure!(resp.status().is_success(), "execute-actions-email: {}", resp.status());
        Ok(())
    }

    /// Undo a half-finished sign-up (account created but the email could not be sent).
    pub async fn delete_user(&self, user_id: &str) -> anyhow::Result<()> {
        let tok = self.token().await?;
        let resp = self.http.delete(self.admin(&format!("/users/{user_id}"))).bearer_auth(&tok).send().await?;
        anyhow::ensure!(resp.status().is_success(), "delete user: {}", resp.status());
        Ok(())
    }
}
