//! HTTP side: the invite form and sign-up handler. Public through the Cloudflare tunnel, so: rate limits keyed on
//! CF-Connecting-IP, strict input validation, a honeypot field, no JavaScript and a locked-down CSP.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Form, Router,
    extract::{DefaultBodyLimit, Path, State as AxState},
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use serde_json::json;

use crate::{
    Config,
    invites::{self, Reserve},
    keycloak::{Created, Keycloak},
};

const LISTEN: &str = "0.0.0.0:8080";
const PER_IP: (usize, Duration) = (5, Duration::from_secs(600));
const GLOBAL: (usize, Duration) = (30, Duration::from_secs(3600));

pub struct State {
    cfg: Config,
    kc: Keycloak,
    http: reqwest::Client,
    hits: Mutex<HashMap<String, Vec<Instant>>>,
}

impl State {
    pub fn new(cfg: Config, kc: Keycloak, http: reqwest::Client) -> Self {
        Self { cfg, kc, http, hits: Mutex::new(HashMap::new()) }
    }

    /// Sliding-window limits on sign-up attempts: per client and overall.
    fn allow(&self, ip: &str) -> bool {
        let now = Instant::now();
        let mut hits = self.hits.lock().unwrap();
        hits.retain(|_, v| {
            v.retain(|t| now.duration_since(*t) < GLOBAL.1);
            !v.is_empty()
        });
        let total: usize = hits.values().map(|v| v.iter().filter(|t| now.duration_since(**t) < GLOBAL.1).count()).sum();
        let mine = hits.get(ip).map(|v| v.iter().filter(|t| now.duration_since(**t) < PER_IP.1).count()).unwrap_or(0);
        if mine >= PER_IP.0 || total >= GLOBAL.0 {
            return false;
        }
        hits.entry(ip.to_owned()).or_default().push(now);
        true
    }
}

pub async fn serve(state: Arc<State>) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/join/{code}", get(form).post(submit))
        .route("/healthz", get(|| async { "ok" }))
        .fallback(|| async { (StatusCode::NOT_FOUND, "not found") })
        .layer(DefaultBodyLimit::max(8 * 1024))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(LISTEN).await?;
    tracing::info!("recipes-join listening on {LISTEN}");
    axum::serve(listener, app).with_graceful_shutdown(async { let _ = tokio::signal::ctrl_c().await; }).await?;
    Ok(())
}

pub fn healthcheck() -> anyhow::Result<()> {
    use std::io::{Read, Write};
    let mut s = std::net::TcpStream::connect_timeout(&"127.0.0.1:8080".parse()?, Duration::from_secs(3))?;
    s.set_read_timeout(Some(Duration::from_secs(3)))?;
    s.write_all(b"GET /healthz HTTP/1.0\r\nHost: localhost\r\n\r\n")?;
    let mut buf = String::new();
    s.read_to_string(&mut buf)?;
    anyhow::ensure!(buf.starts_with("HTTP/1.1 200") || buf.starts_with("HTTP/1.0 200"), "unhealthy");
    Ok(())
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;").replace('\'', "&#39;")
}

fn page(status: StatusCode, title: &str, body: &str) -> Response {
    let html = format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex"><meta name="referrer" content="no-referrer"><title>{title}</title>
<style>body{{font-family:system-ui,sans-serif;max-width:30rem;margin:2.5rem auto;padding:0 1rem;line-height:1.5;color:#222;background:#fff}}
@media (prefers-color-scheme:dark){{body{{color:#eee;background:#111}}a{{color:#8ab4f8}}input{{background:#222;color:#eee;border-color:#555}}}}
h1{{font-size:1.35rem}}label{{display:block;margin-top:.9rem;font-weight:600}}input{{width:100%;box-sizing:border-box;font-size:1rem;padding:.5rem;margin-top:.25rem;border:1px solid #bbb;border-radius:6px}}
button{{font-size:1rem;padding:.6rem 1.2rem;margin-top:1.2rem}}.hp{{position:absolute;left:-10000px}}.err{{color:#c0392b}}.muted{{opacity:.7;font-size:.9rem}}</style></head>
<body>{body}</body></html>"#
    );
    (
        status,
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (header::HeaderName::from_static("x-frame-options"), "DENY"),
            (header::CONTENT_SECURITY_POLICY,
             "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"),
        ],
        Html(html),
    )
        .into_response()
}

fn invalid_link() -> Response {
    page(StatusCode::NOT_FOUND, "Invite not valid",
         "<h1>This invite link isn't valid anymore</h1><p>It may have expired or already been used. Ask for a new one.</p>")
}

fn valid_code(code: &str) -> bool {
    code.len() == 48 && code.bytes().all(|b| b.is_ascii_hexdigit())
}

fn form_html(error: &str, first: &str, last: &str, email: &str) -> String {
    let err = if error.is_empty() { String::new() } else { format!(r#"<p class="err">{}</p>"#, esc(error)) };
    format!(
        r#"<h1>Join the recipe book</h1>
<p>You've been invited to the shared recipe collection. Enter your details and we'll email you a link to set your password.</p>
{err}<form method="post">
<label>First name<input name="first" required maxlength="50" autocomplete="given-name" value="{}"></label>
<label>Last name<input name="last" required maxlength="50" autocomplete="family-name" value="{}"></label>
<label>Email<input name="email" type="email" required maxlength="254" autocomplete="email" value="{}"></label>
<label class="hp" aria-hidden="true">Website<input name="website" tabindex="-1" autocomplete="off"></label>
<button type="submit">Create my account</button></form>
<p class="muted">Already have an account? Just sign in on the recipes site.</p>"#,
        esc(first), esc(last), esc(email)
    )
}

async fn form(AxState(st): AxState<Arc<State>>, Path(code): Path<String>) -> Response {
    if !valid_code(&code) {
        return invalid_link();
    }
    match invites::peek(&st.cfg.data_dir, &code) {
        Ok(Some(_)) => page(StatusCode::OK, "Join the recipe book", &form_html("", "", "", "")),
        Ok(None) => invalid_link(),
        Err(e) => {
            tracing::error!("invite store: {e:#}");
            page(StatusCode::SERVICE_UNAVAILABLE, "Try again later", "<h1>Something went wrong</h1><p>Please try again in a few minutes.</p>")
        }
    }
}

#[derive(Deserialize)]
struct SignUp {
    #[serde(default)]
    first: String,
    #[serde(default)]
    last: String,
    #[serde(default)]
    email: String,
    #[serde(default)]
    website: String,
}

fn clean_name(s: &str) -> Option<String> {
    let s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    let n = s.chars().count();
    ((1..=50).contains(&n) && s.chars().all(|c| c.is_alphabetic() || matches!(c, ' ' | '-' | '\'' | '.'))).then_some(s)
}

fn clean_email(s: &str) -> Option<String> {
    let s = s.trim().to_lowercase();
    let (local, domain) = s.split_once('@')?;
    let ok_local = (1..=64).contains(&local.len())
        && local.bytes().all(|b| b.is_ascii_alphanumeric() || b"!#$%&*+-/=?^_{|}~.".contains(&b))
        && !local.starts_with('.') && !local.ends_with('.') && !local.contains("..");
    let labels: Vec<&str> = domain.split('.').collect();
    let ok_domain = s.len() <= 254 && labels.len() >= 2
        && labels.iter().all(|l| (1..=63).contains(&l.len()) && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            && !l.starts_with('-') && !l.ends_with('-'));
    (ok_local && ok_domain).then_some(s)
}

fn client_ip(h: &HeaderMap) -> String {
    h.get("cf-connecting-ip").and_then(|v| v.to_str().ok()).unwrap_or("direct").chars().take(64).collect()
}

async fn submit(AxState(st): AxState<Arc<State>>, Path(code): Path<String>, headers: HeaderMap, Form(f): Form<SignUp>) -> Response {
    if !valid_code(&code) {
        return invalid_link();
    }
    let ip = client_ip(&headers);
    if !st.allow(&ip) {
        tracing::warn!(ip, "sign-up rate limited");
        return page(StatusCode::TOO_MANY_REQUESTS, "Slow down", "<h1>Too many attempts</h1><p>Please wait a few minutes and try again.</p>");
    }
    let done = |email: &str| {
        page(StatusCode::OK, "Check your email", &format!(
            "<h1>Almost done</h1><p>We've emailed <b>{}</b> a link to set your password (from \"Homelab SSO\"; check spam if it isn't there in a few minutes).</p>\
             <p>Then sign in at <a href=\"{}\">{}</a>.</p>", esc(email), esc(&st.cfg.public_url), esc(&st.cfg.public_url)))
    };
    if !f.website.is_empty() {
        tracing::info!(ip, "honeypot filled; ignored");
        return done(&f.email);
    }
    let (Some(first), Some(last), Some(email)) = (clean_name(&f.first), clean_name(&f.last), clean_email(&f.email)) else {
        return page(StatusCode::BAD_REQUEST, "Join the recipe book",
                    &form_html("Please check your name (letters, spaces, - ' .) and email address.", &f.first, &f.last, &f.email));
    };
    let domain = email.rsplit('@').next().unwrap_or_default().to_owned();
    let (inv_id, uses, max_uses) = match invites::reserve(&st.cfg.data_dir, &code) {
        Ok(Reserve::Ok { id, uses, max_uses }) => (id, uses, max_uses),
        Ok(Reserve::Invalid) => return invalid_link(),
        Err(e) => {
            tracing::error!("invite store: {e:#}");
            return page(StatusCode::SERVICE_UNAVAILABLE, "Try again later", "<h1>Something went wrong</h1><p>Please try again in a few minutes.</p>");
        }
    };
    let fail = |msg: &str| page(StatusCode::BAD_GATEWAY, "Try again later", &format!("<h1>Something went wrong</h1><p>{}</p>", esc(msg)));
    let user_id = match st.kc.create_user(&email, &first, &last).await {
        Ok(Created::Ok(id)) => id,
        Ok(Created::Exists) => {
            let _ = invites::release(&st.cfg.data_dir, &inv_id);
            tracing::info!(invite = inv_id, domain, "sign-up for an existing account");
            return page(StatusCode::CONFLICT, "Already registered", &format!(
                "<h1>You already have an account</h1><p>That email is already registered. Sign in at <a href=\"{0}\">{0}</a>, \
                 or use \"Forgot password?\" on the sign-in page.</p>", esc(&st.cfg.public_url)));
        }
        Err(e) => {
            let _ = invites::release(&st.cfg.data_dir, &inv_id);
            tracing::error!(invite = inv_id, "create user failed: {e:#}");
            return fail("We couldn't create your account. Please try again later.");
        }
    };
    if domain != st.cfg.test_email_domain {
        if let Err(e) = st.kc.send_set_password(&user_id).await {
            tracing::error!(invite = inv_id, "set-password email failed: {e:#}");
            let _ = st.kc.delete_user(&user_id).await;
            let _ = invites::release(&st.cfg.data_dir, &inv_id);
            return fail("We couldn't send the email to that address. Please check it and try again.");
        }
    } else {
        tracing::info!("test address: set-password email skipped");
    }
    tracing::info!(invite = inv_id, domain, uses, max_uses, "account created");
    if let Some(url) = st.cfg.notify_url.clone() {
        let http = st.http.clone();
        let payload = json!({ "name": format!("{first} {last}"), "email": email, "invite": inv_id, "uses": uses, "max_uses": max_uses });
        tokio::spawn(async move {
            if let Err(e) = http.post(url).timeout(Duration::from_secs(5)).json(&payload).send().await {
                tracing::warn!("notify webhook failed: {e}");
            }
        });
    }
    done(&email)
}
