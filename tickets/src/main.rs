//! homelab-tickets: a minimal ticket queue for homelab alerts.
//!
//! - Web UI (server-rendered, no JavaScript) behind a single-password login.
//! - JSON API (Bearer token) used by the Wazuh -> Claude alert responder.
//! - SQLite storage. Tickets can carry a dedupe `key`: posting with the key of an open ticket appends an event
//!   to it instead of opening a new one.
//!
//! Report text may contain attacker-controlled log content, so markdown is rendered with raw HTML neutralised,
//! dangerous link schemes dropped, images removed, and a CSP that forbids scripts entirely.
//!
//! Auth: with TICKETS_PROXY_SECRET set, the UI trusts the user nginx passes in X-Tickets-User (from oauth2-proxy /
//! Keycloak), but only when X-Tickets-Proxy-Secret matches; password login is then disabled. Without it, the
//! single-password login is used. The /api/* routes always use the Bearer token.
//!
//! Env: TICKETS_DB (default /data/tickets.db), TICKETS_BIND (default 0.0.0.0:8080),
//!      TICKETS_API_TOKEN, TICKETS_UI_PASSWORD, TICKETS_SESSION_SECRET (all required),
//!      TICKETS_BASE_URL (for links returned by the API).

use axum::{
    extract::{Form, Path, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
    Json, Router,
};
use hmac::{Hmac, Mac};
use pulldown_cmark::{html, CowStr, Event, Options, Parser, Tag};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::sync::{Arc, Mutex};

type HmacSha256 = Hmac<Sha256>;

const SEVERITIES: [&str; 5] = ["info", "low", "medium", "high", "critical"];
const STATUSES: [&str; 4] = ["new", "acknowledged", "resolved", "closed"];
const SESSION_DAYS: i64 = 30;

struct AppState {
    db: Mutex<Connection>,
    api_token: String,
    ui_password: String,
    secret: Vec<u8>,
    base_url: String,
    proxy_secret: Option<String>,
}
type S = State<Arc<AppState>>;

// ---------- helpers ----------

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn sev_rank(s: &str) -> usize {
    SEVERITIES.iter().position(|x| *x == s).unwrap_or(0)
}

fn norm_sev(s: &str) -> String {
    let s = s.trim().to_lowercase();
    if SEVERITIES.contains(&s.as_str()) { s } else { "info".into() }
}

fn esc(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => o.push_str("&amp;"),
            '<' => o.push_str("&lt;"),
            '>' => o.push_str("&gt;"),
            '"' => o.push_str("&quot;"),
            '\'' => o.push_str("&#39;"),
            _ => o.push(c),
        }
    }
    o
}

fn fmt_time(ts: i64) -> String {
    let dt = chrono::DateTime::from_timestamp(ts, 0).unwrap_or_default();
    let ago = now() - ts;
    let rel = if ago < 60 { "just now".to_string() }
        else if ago < 3600 { format!("{}m ago", ago / 60) }
        else if ago < 86400 { format!("{}h ago", ago / 3600) }
        else { format!("{}d ago", ago / 86400) };
    format!("{} UTC ({})", dt.format("%Y-%m-%d %H:%M"), rel)
}

/// Render untrusted markdown safely: raw HTML becomes text, images become their alt text,
/// and only http(s)/mailto/relative links survive.
fn render_md(src: &str) -> String {
    let mut opts = Options::empty();
    opts.insert(Options::ENABLE_TABLES);
    opts.insert(Options::ENABLE_STRIKETHROUGH);
    let mut in_image = 0usize;
    let events = Parser::new_ext(src, opts).filter_map(|ev| match ev {
        Event::Html(h) | Event::InlineHtml(h) => Some(Event::Text(h)),
        Event::Start(Tag::Image { .. }) => { in_image += 1; None }
        Event::End(pulldown_cmark::TagEnd::Image) => { in_image = in_image.saturating_sub(1); None }
        Event::Start(Tag::Link { link_type, dest_url, title, id }) => {
            let d = dest_url.trim().to_lowercase();
            let ok = d.starts_with("http://") || d.starts_with("https://") || d.starts_with("mailto:")
                || d.starts_with('/') || d.starts_with('#');
            let dest = if ok { dest_url } else { CowStr::Borrowed("#") };
            Some(Event::Start(Tag::Link { link_type, dest_url: dest, title, id }))
        }
        other => Some(other),
    });
    let mut out = String::new();
    html::push_html(&mut out, events);
    out
}

fn ct_eq(a: &str, b: &str) -> bool {
    // constant-time compare via HMAC of both sides with a fixed key
    let mut ma = HmacSha256::new_from_slice(b"ct-eq").unwrap();
    ma.update(a.as_bytes());
    let mut mb = HmacSha256::new_from_slice(b"ct-eq").unwrap();
    mb.update(b.as_bytes());
    ma.verify_slice(&mb.finalize().into_bytes()).is_ok()
}

fn sign(secret: &[u8], msg: &str) -> String {
    let mut m = HmacSha256::new_from_slice(secret).unwrap();
    m.update(msg.as_bytes());
    hex::encode(m.finalize().into_bytes())
}

fn session_ok(st: &AppState, headers: &HeaderMap) -> bool {
    let Some(cookies) = headers.get(header::COOKIE).and_then(|v| v.to_str().ok()) else { return false };
    for c in cookies.split(';') {
        if let Some(v) = c.trim().strip_prefix("tk=") {
            if let Some((exp, sig)) = v.split_once('.') {
                if let Ok(e) = exp.parse::<i64>() {
                    if e > now() && ct_eq(&sign(&st.secret, exp), sig) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// Who is using the UI, if anyone. Proxy mode: trusted header from nginx (secret-gated). Otherwise: password session.
fn ui_user(st: &AppState, headers: &HeaderMap) -> Option<String> {
    if let Some(secret) = &st.proxy_secret {
        let h = |k: &str| headers.get(k).and_then(|v| v.to_str().ok()).unwrap_or("").trim().to_string();
        let user = h("x-tickets-user");
        return (ct_eq(&h("x-tickets-proxy-secret"), secret) && !user.is_empty())
            .then(|| user.chars().take(120).collect());
    }
    session_ok(st, headers).then(|| "cole".to_string())
}

fn api_ok(st: &AppState, headers: &HeaderMap) -> bool {
    headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|t| ct_eq(t, &st.api_token)).unwrap_or(false)
}

fn page(title: &str, body: &str) -> Response {
    let html = format!(r#"<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>{t}</title><style>
body{{font:15px/1.45 -apple-system,system-ui,sans-serif;margin:0;background:#111418;color:#e6e6e6}}
a{{color:#7cb7ff;text-decoration:none}} header{{padding:12px 16px;background:#1b2027;display:flex;gap:14px;align-items:center;flex-wrap:wrap}}
header b{{font-size:17px}} main{{padding:12px 16px;max-width:920px;margin:auto}}
.t{{display:block;padding:10px 12px;margin:8px 0;background:#1b2027;border-radius:8px;border-left:4px solid #555}}
.t .m{{color:#9aa4ae;font-size:13px}} .sev-critical{{border-color:#ff4d4f}} .sev-high{{border-color:#ff8c1a}}
.sev-medium{{border-color:#f5c518}} .sev-low{{border-color:#4caf50}} .sev-info{{border-color:#5b8def}}
.badge{{display:inline-block;padding:1px 7px;border-radius:9px;font-size:12px;background:#2c333c;margin-right:4px}}
.cole{{background:#6b2fbf}} .filters a{{margin-right:10px}} .filters a.on{{font-weight:bold;text-decoration:underline}}
.report{{background:#1b2027;padding:4px 14px;border-radius:8px;overflow-x:auto}} pre,code{{white-space:pre-wrap;word-break:break-word}}
.ev{{border-left:2px solid #333;padding:4px 10px;margin:6px 0}} .ev .m{{color:#9aa4ae;font-size:12px}}
button,input,textarea,select{{font:inherit;background:#2c333c;color:#e6e6e6;border:1px solid #444;border-radius:6px;padding:6px 10px}}
textarea{{width:100%;box-sizing:border-box;min-height:70px}} form.inline{{display:inline}} table{{border-collapse:collapse}} td,th{{border:1px solid #333;padding:3px 6px}}
</style></head><body><header><b><a href="/">Homelab Tickets</a></b><span class="filters">
<a href="/?view=attention">Needs you</a><a href="/?view=open">Open</a><a href="/?view=all">All</a></span>
<span style="flex:1"></span><form class="inline" method="post" action="/logout"><button>Log out</button></form></header>
<main>{b}</main></body></html>"#, t = esc(title), b = body);
    let mut r = Html(html).into_response();
    r.headers_mut().insert(header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("default-src 'self'; script-src 'none'; style-src 'unsafe-inline'; img-src 'self'; form-action 'self'; frame-ancestors 'self'"));
    r.headers_mut().insert(header::REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
    r
}

fn login_redirect() -> Response {
    // In proxy mode nginx/oauth2-proxy normally intercepts first; /login forwards to the SSO sign-in.
    Redirect::to("/login").into_response()
}

// ---------- db ----------

fn init_db(c: &Connection) -> rusqlite::Result<()> {
    c.execute_batch(r#"
        PRAGMA journal_mode=WAL;
        PRAGMA foreign_keys=ON;
        CREATE TABLE IF NOT EXISTS tickets(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT, title TEXT NOT NULL, severity TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'new',
            awaiting_cole INTEGER NOT NULL DEFAULT 0, source TEXT, host TEXT,
            summary TEXT NOT NULL DEFAULT '', body TEXT NOT NULL DEFAULT '',
            occurrences INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
        CREATE INDEX IF NOT EXISTS tickets_key ON tickets(key, status);
        CREATE INDEX IF NOT EXISTS tickets_updated ON tickets(updated_at);
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticket_id INTEGER NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
            at INTEGER NOT NULL, kind TEXT NOT NULL, author TEXT NOT NULL, text TEXT NOT NULL DEFAULT '');
        CREATE INDEX IF NOT EXISTS events_ticket ON events(ticket_id, at);
    "#)
}

#[derive(Serialize, Clone)]
struct Ticket {
    id: i64, key: Option<String>, title: String, severity: String, status: String, awaiting_cole: bool,
    source: Option<String>, host: Option<String>, summary: String, body: String, occurrences: i64,
    created_at: i64, updated_at: i64,
}

#[derive(Serialize)]
struct EventRow { at: i64, kind: String, author: String, text: String }

const TCOLS: &str = "id,key,title,severity,status,awaiting_cole,source,host,summary,body,occurrences,created_at,updated_at";

fn row_ticket(r: &rusqlite::Row) -> rusqlite::Result<Ticket> {
    Ok(Ticket {
        id: r.get(0)?, key: r.get(1)?, title: r.get(2)?, severity: r.get(3)?, status: r.get(4)?,
        awaiting_cole: r.get::<_, i64>(5)? != 0, source: r.get(6)?, host: r.get(7)?, summary: r.get(8)?,
        body: r.get(9)?, occurrences: r.get(10)?, created_at: r.get(11)?, updated_at: r.get(12)?,
    })
}

fn get_ticket(c: &Connection, id: i64) -> rusqlite::Result<Option<Ticket>> {
    c.query_row(&format!("SELECT {TCOLS} FROM tickets WHERE id=?1"), [id], row_ticket).optional()
}

fn get_events(c: &Connection, id: i64) -> rusqlite::Result<Vec<EventRow>> {
    let mut s = c.prepare("SELECT at,kind,author,text FROM events WHERE ticket_id=?1 ORDER BY at, id")?;
    let rows = s.query_map([id], |r| Ok(EventRow { at: r.get(0)?, kind: r.get(1)?, author: r.get(2)?, text: r.get(3)? }))?;
    rows.collect()
}

fn add_event(c: &Connection, id: i64, kind: &str, author: &str, text: &str) -> rusqlite::Result<()> {
    c.execute("INSERT INTO events(ticket_id,at,kind,author,text) VALUES(?1,?2,?3,?4,?5)",
              params![id, now(), kind, author, text])?;
    c.execute("UPDATE tickets SET updated_at=?2 WHERE id=?1", params![id, now()])?;
    Ok(())
}

fn list_tickets(c: &Connection, view: &str) -> rusqlite::Result<Vec<Ticket>> {
    let filter = match view {
        "attention" => "WHERE awaiting_cole=1 AND status IN ('new','acknowledged')",
        "all" => "",
        _ => "WHERE status IN ('new','acknowledged')",
    };
    let mut s = c.prepare(&format!("SELECT {TCOLS} FROM tickets {filter} ORDER BY awaiting_cole DESC, updated_at DESC LIMIT 300"))?;
    let rows = s.query_map([], row_ticket)?;
    rows.collect()
}

// ---------- UI handlers ----------

#[derive(Deserialize)]
struct ListQ { view: Option<String> }

async fn ui_list(State(st): S, headers: HeaderMap, Query(q): Query<ListQ>) -> Response {
    let Some(_user) = ui_user(&st, &headers) else { return login_redirect(); };
    let view = q.view.unwrap_or_else(|| "open".into());
    let tickets = match list_tickets(&st.db.lock().unwrap(), &view) {
        Ok(t) => t,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    let mut b = format!("<p class=\"m\">{} ticket(s) &middot; view: {}</p>", tickets.len(), esc(&view));
    for t in &tickets {
        b.push_str(&format!(
            r#"<a class="t sev-{sev}" href="/t/{id}"><div><span class="badge">#{id}</span><span class="badge">{sev}</span><span class="badge">{st}</span>{cole}{occ}<b>{title}</b></div><div class="m">{host}{summary}</div><div class="m">updated {upd}</div></a>"#,
            id = t.id, sev = esc(&t.severity), st = esc(&t.status), title = esc(&t.title),
            cole = if t.awaiting_cole { "<span class=\"badge cole\">needs you</span>" } else { "" },
            occ = if t.occurrences > 1 { format!("<span class=\"badge\">x{}</span>", t.occurrences) } else { String::new() },
            host = t.host.as_deref().map(|h| format!("{} &middot; ", esc(h))).unwrap_or_default(),
            summary = esc(&t.summary.chars().take(220).collect::<String>()), upd = fmt_time(t.updated_at)));
    }
    if tickets.is_empty() { b.push_str("<p>Nothing here.</p>"); }
    page("Homelab Tickets", &b)
}

async fn ui_ticket(State(st): S, headers: HeaderMap, Path(id): Path<i64>) -> Response {
    let Some(_user) = ui_user(&st, &headers) else { return login_redirect(); };
    let (t, evs) = {
        let c = st.db.lock().unwrap();
        match get_ticket(&c, id) {
            Ok(Some(t)) => (t, get_events(&c, id).unwrap_or_default()),
            _ => return (StatusCode::NOT_FOUND, "no such ticket").into_response(),
        }
    };
    let mut actions = String::new();
    let btn = |label: &str, status: &str| format!(
        r#"<form class="inline" method="post" action="/t/{id}/status"><input type="hidden" name="status" value="{status}"><button>{label}</button></form> "#);
    match t.status.as_str() {
        "new" => { actions += &btn("Acknowledge", "acknowledged"); actions += &btn("Resolve", "resolved"); actions += &btn("Close (noise)", "closed"); }
        "acknowledged" => { actions += &btn("Resolve", "resolved"); actions += &btn("Close (noise)", "closed"); }
        _ => { actions += &btn("Reopen", "new"); }
    }
    if t.awaiting_cole {
        actions += &format!(r#"<form class="inline" method="post" action="/t/{id}/confirmed"><button>Mark decided</button></form>"#);
    }
    let mut ev_html = String::new();
    for e in &evs {
        ev_html.push_str(&format!(r#"<div class="ev"><div class="m">{} &middot; {} &middot; {}</div>{}</div>"#,
            fmt_time(e.at), esc(&e.kind), esc(&e.author), render_md(&e.text)));
    }
    let b = format!(r#"<h2>#{id} {title}</h2>
<p><span class="badge">{sev}</span><span class="badge">{status}</span>{cole}<span class="badge">seen x{occ}</span></p>
<p class="m">host: {host} &middot; source: {source} &middot; key: {key}<br>opened {created} &middot; updated {updated}</p>
<p><b>{summary}</b></p><p>{actions}</p>
<div class="report">{body}</div>
<h3>Timeline</h3>{ev_html}
<form method="post" action="/t/{id}/comment"><textarea name="text" placeholder="Add a note"></textarea><p><button>Add note</button></p></form>"#,
        title = esc(&t.title), sev = esc(&t.severity), status = esc(&t.status),
        cole = if t.awaiting_cole { "<span class=\"badge cole\">needs you</span>" } else { "" },
        occ = t.occurrences, host = esc(t.host.as_deref().unwrap_or("-")), source = esc(t.source.as_deref().unwrap_or("-")),
        key = esc(t.key.as_deref().unwrap_or("-")), created = fmt_time(t.created_at), updated = fmt_time(t.updated_at),
        summary = esc(&t.summary), body = render_md(&t.body));
    page(&format!("#{id} {}", t.title), &b)
}

#[derive(Deserialize)]
struct StatusForm { status: String }

async fn ui_status(State(st): S, headers: HeaderMap, Path(id): Path<i64>, Form(f): Form<StatusForm>) -> Response {
    let Some(user) = ui_user(&st, &headers) else { return login_redirect(); };
    if !STATUSES.contains(&f.status.as_str()) { return (StatusCode::BAD_REQUEST, "bad status").into_response(); }
    let c = st.db.lock().unwrap();
    let _ = c.execute("UPDATE tickets SET status=?2 WHERE id=?1", params![id, f.status]);
    if f.status == "resolved" || f.status == "closed" {
        let _ = c.execute("UPDATE tickets SET awaiting_cole=0 WHERE id=?1", [id]);
    }
    let _ = add_event(&c, id, "status", &user, &format!("Status set to **{}**", f.status));
    Redirect::to(&format!("/t/{id}")).into_response()
}

async fn ui_confirmed(State(st): S, headers: HeaderMap, Path(id): Path<i64>) -> Response {
    let Some(user) = ui_user(&st, &headers) else { return login_redirect(); };
    let c = st.db.lock().unwrap();
    let _ = c.execute("UPDATE tickets SET awaiting_cole=0 WHERE id=?1", [id]);
    let _ = add_event(&c, id, "decision", &user, "Marked as decided");
    Redirect::to(&format!("/t/{id}")).into_response()
}

#[derive(Deserialize)]
struct CommentForm { text: String }

async fn ui_comment(State(st): S, headers: HeaderMap, Path(id): Path<i64>, Form(f): Form<CommentForm>) -> Response {
    let Some(user) = ui_user(&st, &headers) else { return login_redirect(); };
    let text = f.text.trim();
    if !text.is_empty() {
        let _ = add_event(&st.db.lock().unwrap(), id, "note", &user, &text.chars().take(20000).collect::<String>());
    }
    Redirect::to(&format!("/t/{id}")).into_response()
}

async fn ui_login_form(State(st): S) -> Response {
    if st.proxy_secret.is_some() { return Redirect::to("/oauth2/sign_in?rd=%2F%3Fview%3Dattention").into_response(); }
    let mut r = Html(r#"<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Login</title><style>body{font:16px system-ui;background:#111418;color:#e6e6e6;display:flex;justify-content:center;padding-top:15vh}
input,button{font:inherit;padding:8px 12px;border-radius:6px;border:1px solid #444;background:#2c333c;color:#e6e6e6}</style></head>
<body><form method="post" action="/login"><h2>Homelab Tickets</h2><p><input type="password" name="password" placeholder="Password" autofocus></p><p><button>Log in</button></p></form></body></html>"#).into_response();
    r.headers_mut().insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static("default-src 'none'; style-src 'unsafe-inline'; form-action 'self'"));
    r
}

#[derive(Deserialize)]
struct LoginForm { password: String }

async fn ui_login(State(st): S, Form(f): Form<LoginForm>) -> Response {
    if st.proxy_secret.is_some() { return StatusCode::NOT_FOUND.into_response(); }
    if !ct_eq(&f.password, &st.ui_password) {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        return Redirect::to("/login").into_response();
    }
    let exp = (now() + SESSION_DAYS * 86400).to_string();
    let cookie = format!("tk={}.{}; Path=/; Max-Age={}; HttpOnly; Secure; SameSite=Strict", exp, sign(&st.secret, &exp), SESSION_DAYS * 86400);
    let mut r = Redirect::to("/?view=attention").into_response();
    r.headers_mut().insert(header::SET_COOKIE, HeaderValue::from_str(&cookie).unwrap());
    r
}

async fn ui_logout(State(st): S) -> Response {
    if st.proxy_secret.is_some() {
        // Clear the oauth2-proxy cookie, then end the Keycloak session so the next visit asks for a login again.
        return Redirect::to("/oauth2/sign_out?rd=https%3A%2F%2Fsso.pezzitools.org%2Frealms%2Fhomelab%2Fprotocol%2Fopenid-connect%2Flogout%3Fclient_id%3Dhomelab-tickets%26post_logout_redirect_uri%3Dhttps%253A%252F%252Ftickets.pezzitools.org%252F").into_response();
    }
    let mut r = Redirect::to("/login").into_response();
    r.headers_mut().insert(header::SET_COOKIE, HeaderValue::from_static("tk=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Strict"));
    r
}

// ---------- API ----------

#[derive(Deserialize)]
struct NewTicket {
    title: String,
    severity: Option<String>,
    summary: Option<String>,
    body: Option<String>,
    key: Option<String>,
    source: Option<String>,
    host: Option<String>,
    awaiting_cole: Option<bool>,
}

#[derive(Serialize)]
struct NewTicketResp { id: i64, url: String, created: bool, awaiting_cole: bool }

async fn api_create(State(st): S, headers: HeaderMap, Json(n): Json<NewTicket>) -> Response {
    if !api_ok(&st, &headers) { return StatusCode::UNAUTHORIZED.into_response(); }
    let sev = norm_sev(n.severity.as_deref().unwrap_or("info"));
    let summary = n.summary.unwrap_or_default();
    let body = n.body.unwrap_or_default();
    let cole = n.awaiting_cole.unwrap_or(false);
    let title: String = n.title.chars().take(300).collect();
    let c = st.db.lock().unwrap();
    // Dedupe: an open ticket with the same key absorbs this occurrence.
    if let Some(key) = n.key.as_deref().filter(|k| !k.is_empty()) {
        let existing: Option<Ticket> = c.query_row(
            &format!("SELECT {TCOLS} FROM tickets WHERE key=?1 AND status IN ('new','acknowledged') ORDER BY id DESC LIMIT 1"),
            [key], row_ticket).optional().unwrap_or(None);
        if let Some(t) = existing {
            let new_sev = if sev_rank(&sev) > sev_rank(&t.severity) { sev.clone() } else { t.severity.clone() };
            let new_cole = t.awaiting_cole || cole;
            let _ = c.execute("UPDATE tickets SET occurrences=occurrences+1, severity=?2, awaiting_cole=?3, summary=?4 WHERE id=?1",
                              params![t.id, new_sev, new_cole as i64, summary]);
            let _ = add_event(&c, t.id, "occurrence", "responder", &format!("**{}**: {}\n\n{}", sev, summary, body));
            return Json(NewTicketResp { id: t.id, url: format!("{}/t/{}", st.base_url, t.id), created: false, awaiting_cole: new_cole }).into_response();
        }
    }
    let ts = now();
    let r = c.execute(
        "INSERT INTO tickets(key,title,severity,status,awaiting_cole,source,host,summary,body,created_at,updated_at) VALUES(?1,?2,?3,'new',?4,?5,?6,?7,?8,?9,?9)",
        params![n.key, title, sev, cole as i64, n.source, n.host, summary, body, ts]);
    if let Err(e) = r { return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(); }
    let id = c.last_insert_rowid();
    let _ = add_event(&c, id, "opened", n.source.as_deref().unwrap_or("api"), "Ticket opened");
    Json(NewTicketResp { id, url: format!("{}/t/{}", st.base_url, id), created: true, awaiting_cole: cole }).into_response()
}

#[derive(Deserialize)]
struct ApiListQ { status: Option<String> }

async fn api_list(State(st): S, headers: HeaderMap, Query(q): Query<ApiListQ>) -> Response {
    if !api_ok(&st, &headers) { return StatusCode::UNAUTHORIZED.into_response(); }
    let view = match q.status.as_deref() { Some("all") => "all", Some("attention") => "attention", _ => "open" };
    match list_tickets(&st.db.lock().unwrap(), view) {
        Ok(t) => Json(t).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn api_get(State(st): S, headers: HeaderMap, Path(id): Path<i64>) -> Response {
    if !api_ok(&st, &headers) { return StatusCode::UNAUTHORIZED.into_response(); }
    let c = st.db.lock().unwrap();
    match get_ticket(&c, id) {
        Ok(Some(t)) => Json(serde_json::json!({"ticket": t, "events": get_events(&c, id).unwrap_or_default()})).into_response(),
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

#[derive(Deserialize)]
struct ApiEvent { kind: Option<String>, text: String, status: Option<String> }

async fn api_event(State(st): S, headers: HeaderMap, Path(id): Path<i64>, Json(e): Json<ApiEvent>) -> Response {
    if !api_ok(&st, &headers) { return StatusCode::UNAUTHORIZED.into_response(); }
    let c = st.db.lock().unwrap();
    if !matches!(get_ticket(&c, id), Ok(Some(_))) { return StatusCode::NOT_FOUND.into_response(); }
    if let Some(s) = e.status.as_deref() {
        if !STATUSES.contains(&s) { return (StatusCode::BAD_REQUEST, "bad status").into_response(); }
        let _ = c.execute("UPDATE tickets SET status=?2 WHERE id=?1", params![id, s]);
    }
    let _ = add_event(&c, id, e.kind.as_deref().unwrap_or("note"), "responder", &e.text);
    StatusCode::NO_CONTENT.into_response()
}

async fn healthz() -> &'static str { "ok" }

/// `homelab-tickets --healthcheck`: used by the Docker HEALTHCHECK (the slim image has no curl).
fn healthcheck() -> ! {
    use std::io::{Read, Write};
    let port = std::env::var("TICKETS_BIND").ok().and_then(|b| b.rsplit(':').next().map(str::to_string))
        .unwrap_or_else(|| "8080".into());
    let ok = (|| -> std::io::Result<bool> {
        let mut s = std::net::TcpStream::connect(format!("127.0.0.1:{port}"))?;
        s.set_read_timeout(Some(std::time::Duration::from_secs(3)))?;
        s.write_all(b"GET /healthz HTTP/1.0\r\nHost: localhost\r\n\r\n")?;
        let mut buf = String::new();
        s.read_to_string(&mut buf)?;
        Ok(buf.starts_with("HTTP/1.0 200") || buf.starts_with("HTTP/1.1 200"))
    })().unwrap_or(false);
    std::process::exit(if ok { 0 } else { 1 })
}

#[tokio::main]
async fn main() {
    if std::env::args().any(|a| a == "--healthcheck") { healthcheck(); }
    let env = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("{k} must be set"));
    let db_path = std::env::var("TICKETS_DB").unwrap_or_else(|_| "/data/tickets.db".into());
    let bind = std::env::var("TICKETS_BIND").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let conn = Connection::open(&db_path).expect("open db");
    init_db(&conn).expect("init db");
    let st = Arc::new(AppState {
        db: Mutex::new(conn),
        api_token: env("TICKETS_API_TOKEN"),
        ui_password: env("TICKETS_UI_PASSWORD"),
        secret: env("TICKETS_SESSION_SECRET").into_bytes(),
        base_url: std::env::var("TICKETS_BASE_URL").unwrap_or_default().trim_end_matches('/').to_string(),
        proxy_secret: std::env::var("TICKETS_PROXY_SECRET").ok().filter(|v| v.len() >= 16),
    });
    for (k, v) in [("TICKETS_API_TOKEN", &st.api_token), ("TICKETS_UI_PASSWORD", &st.ui_password)] {
        assert!(v.len() >= 16, "{k} must be at least 16 characters");
    }
    let app = Router::new()
        .route("/", get(ui_list))
        .route("/t/{id}", get(ui_ticket))
        .route("/t/{id}/status", post(ui_status))
        .route("/t/{id}/confirmed", post(ui_confirmed))
        .route("/t/{id}/comment", post(ui_comment))
        .route("/login", get(ui_login_form).post(ui_login))
        .route("/logout", post(ui_logout))
        .route("/api/tickets", get(api_list).post(api_create))
        .route("/api/tickets/{id}", get(api_get))
        .route("/api/tickets/{id}/events", post(api_event))
        .route("/healthz", get(healthz))
        .with_state(st);
    let listener = tokio::net::TcpListener::bind(&bind).await.expect("bind");
    println!("homelab-tickets listening on {bind}, db {db_path}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async { let _ = tokio::signal::ctrl_c().await; })
        .await
        .expect("server");
}
