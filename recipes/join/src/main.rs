//! recipes-join: invite-link sign-up for the recipes site.
//!
//! `GET /join/<code>` shows a short form; `POST` creates a Keycloak user directly in group Mealie (through a service
//! account whose only power is a fine-grained admin permission on that group), asks Keycloak to email a set-password
//! link, and pings a Home Assistant webhook so the owner gets a push.
//!
//! Invite codes are made with the CLI (`recipes-join new --days 7 --uses 10 --note ...`), never over HTTP, and stored only
//! as SHA-256 hashes in DATA_DIR/invites.json. A use is reserved before the Keycloak call and released if it fails.

mod invites;
mod keycloak;
mod web;

use std::{env, sync::Arc, time::Duration};

use anyhow::Context;

pub struct Config {
    pub public_url: String,
    pub data_dir: String,
    /// Emails on this domain skip the set-password email (tests only; the domain can never receive mail)
    pub test_email_domain: String,
    pub notify_url: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    let data_dir = env::var("DATA_DIR").unwrap_or_else(|_| "/data".into());
    let public_url = env::var("PUBLIC_URL").context("PUBLIC_URL is not set")?.trim_end_matches('/').to_owned();

    match args.first().map(String::as_str) {
        Some("new") => return invites::cli_new(&data_dir, &public_url, &args[1..]),
        Some("list") => return invites::cli_list(&data_dir),
        Some("revoke") => return invites::cli_revoke(&data_dir, args.get(1).map(String::as_str).unwrap_or("")),
        Some("--healthcheck") => return web::healthcheck(),
        Some(other) => anyhow::bail!("unknown command {other:?}; use: new [--days N] [--uses N] [--note TEXT] | list | revoke <id>"),
        None => {}
    }

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_ansi(false) // plain text for journald/Wazuh
        .init();
    let http = reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?;
    let kc = keycloak::Keycloak::new(
        http.clone(),
        env::var("KC_URL").unwrap_or_else(|_| "http://keycloak:8080".into()),
        env::var("KC_REALM").unwrap_or_else(|_| "homelab".into()),
        env::var("KC_CLIENT_ID").unwrap_or_else(|_| "mealie-join".into()),
        env::var("KC_CLIENT_SECRET").context("KC_CLIENT_SECRET is not set")?,
        env::var("KC_GROUP").unwrap_or_else(|_| "/Mealie".into()),
        env::var("KC_LOGIN_CLIENT").unwrap_or_else(|_| "mealie".into()),
        format!("{public_url}/login"),
    );
    let cfg = Config {
        public_url,
        data_dir,
        test_email_domain: env::var("TEST_EMAIL_DOMAIN").unwrap_or_else(|_| "invalid.example".into()),
        notify_url: env::var("NOTIFY_URL").ok().filter(|s| !s.is_empty()),
    };
    web::serve(Arc::new(web::State::new(cfg, kc, http))).await
}
