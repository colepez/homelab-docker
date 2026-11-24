//! One-time photo upload pages. The create_image_upload_link tool issues a random token bound to one recipe; the user
//! opens <base>/mcp/upload/<token> in a browser, picks a photo, and it is stored in Mealie as that recipe's image.
//! These routes sit outside the OAuth layer (a phone browser has no MCP token), so the token is the only credential:
//! 256 random bits, valid for TTL, consumed by the first successful upload, never logged. Tokens live in memory only,
//! so a restart invalidates them. GET never consumes a token, so chat link previews can't burn it.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Router,
    extract::{DefaultBodyLimit, Multipart, Path, State},
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::get,
};

use crate::{As, Mealie, sniff_image};

pub const TTL: Duration = Duration::from_secs(30 * 60);
const MAX_PENDING: usize = 20;
const MAX_BYTES: usize = 15 * 1024 * 1024;

struct Pending {
    slug: String,
    name: String,
    /// Keycloak username of whoever asked for the link; the photo is uploaded to Mealie as them
    user: String,
    expires: Instant,
}

#[derive(Default)]
pub struct Uploads {
    pending: Mutex<HashMap<String, Pending>>,
}

impl Uploads {
    pub fn issue(&self, slug: String, name: String, user: String) -> Result<String, String> {
        let mut buf = [0u8; 32];
        getrandom::fill(&mut buf).map_err(|e| format!("no randomness: {e}"))?;
        let token: String = buf.iter().map(|b| format!("{b:02x}")).collect();
        let mut map = self.pending.lock().unwrap();
        let now = Instant::now();
        map.retain(|_, p| p.expires > now);
        if map.len() >= MAX_PENDING {
            return Err("too many open upload links; wait for some to expire (30 minutes)".into());
        }
        map.insert(token.clone(), Pending { slug, name, user, expires: now + TTL });
        Ok(token)
    }

    fn lookup(&self, token: &str) -> Option<(String, String, String)> {
        let map = self.pending.lock().unwrap();
        map.get(token).filter(|p| p.expires > Instant::now()).map(|p| (p.slug.clone(), p.name.clone(), p.user.clone()))
    }

    fn consume(&self, token: &str) {
        self.pending.lock().unwrap().remove(token);
    }
}

#[derive(Clone)]
pub struct UploadState {
    pub mealie: Arc<Mealie>,
    pub uploads: Arc<Uploads>,
}

pub fn router(state: UploadState) -> Router {
    Router::new()
        .route("/mcp/upload/{token}", get(page).post(receive))
        .layer(DefaultBodyLimit::max(MAX_BYTES + 64 * 1024))
        .with_state(state)
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;").replace('\'', "&#39;")
}

fn shell(status: StatusCode, title: &str, body: &str) -> Response {
    let html = format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex"><meta name="referrer" content="no-referrer"><title>{title}</title>
<style>body{{font-family:system-ui,sans-serif;max-width:32rem;margin:2.5rem auto;padding:0 1rem;line-height:1.5;color:#222;background:#fff}}
@media (prefers-color-scheme:dark){{body{{color:#eee;background:#111}}a{{color:#8ab4f8}}}}
h1{{font-size:1.3rem}}button{{font-size:1rem;padding:.6rem 1.2rem;margin-top:1rem}}input{{font-size:1rem}}
img{{max-width:100%;border-radius:8px;margin-top:1rem}}.muted{{opacity:.7;font-size:.9rem}}</style></head><body>{body}</body></html>"#
    );
    (
        status,
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (header::HeaderName::from_static("x-frame-options"), "DENY"),
        ],
        Html(html),
    )
        .into_response()
}

fn expired() -> Response {
    shell(
        StatusCode::NOT_FOUND,
        "Link expired",
        "<h1>This upload link has expired or was already used</h1><p>Ask Claude for a new one.</p>",
    )
}

async fn page(State(st): State<UploadState>, Path(token): Path<String>) -> Response {
    let Some((_, name, _)) = st.uploads.lookup(&token) else { return expired() };
    // The script downsizes to <= 2400px JPEG in the browser (also converts iPhone HEIC); without JS the form posts as is.
    let body = format!(
        r#"<h1>Add a photo to “{name}”</h1>
<form id="f" method="post" enctype="multipart/form-data">
<input id="i" type="file" name="image" accept="image/*" required><br>
<button id="b" type="submit">Upload photo</button></form>
<p id="s" class="muted">Replaces the recipe's current photo. This link works once.</p>
<script>
const f=document.getElementById('f'),i=document.getElementById('i'),s=document.getElementById('s'),b=document.getElementById('b');
f.addEventListener('submit',async e=>{{
  e.preventDefault(); const file=i.files[0]; if(!file) return;
  b.disabled=true; s.textContent='Uploading…';
  let blob=file;
  try {{
    const bmp=await createImageBitmap(file);
    const k=Math.min(1,2400/Math.max(bmp.width,bmp.height));
    const c=document.createElement('canvas'); c.width=Math.round(bmp.width*k); c.height=Math.round(bmp.height*k);
    c.getContext('2d').drawImage(bmp,0,0,c.width,c.height);
    blob=await new Promise(r=>c.toBlob(r,'image/jpeg',0.88))||file;
  }} catch(_) {{}}
  const fd=new FormData(); fd.append('image',blob,'photo.jpg');
  try {{
    const r=await fetch(location.pathname,{{method:'POST',body:fd}});
    document.open(); document.write(await r.text()); document.close();
  }} catch(err) {{ s.textContent='Upload failed: '+err; b.disabled=false; }}
}});
</script>"#,
        name = esc(&name)
    );
    shell(StatusCode::OK, "Upload recipe photo", &body)
}

async fn receive(State(st): State<UploadState>, Path(token): Path<String>, mut form: Multipart) -> Response {
    let Some((slug, name, user)) = st.uploads.lookup(&token) else {
        // Read (and drop) the upload before answering: closing while a proxy is still sending the body turns the
        // 404 into a 502 at Cloudflare. The body-size limit still applies.
        while let Ok(Some(field)) = form.next_field().await {
            let _ = field.bytes().await;
        }
        return expired();
    };
    let me = As::new(st.mealie.clone(), user);
    let fail = |status, msg: &str| {
        shell(status, "Upload failed", &format!("<h1>Upload failed</h1><p>{}</p><p><a href=\"\">Try again</a></p>", esc(msg)))
    };
    let mut bytes = None;
    loop {
        match form.next_field().await {
            Ok(Some(field)) if field.name() == Some("image") => match field.bytes().await {
                Ok(b) => {
                    bytes = Some(b);
                    break;
                }
                Err(_) => return fail(StatusCode::PAYLOAD_TOO_LARGE, "The photo is too large (15 MB max)."),
            },
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(_) => return fail(StatusCode::BAD_REQUEST, "The upload didn't arrive intact."),
        }
    }
    let Some(bytes) = bytes.filter(|b| !b.is_empty()) else { return fail(StatusCode::BAD_REQUEST, "No photo was selected.") };
    if bytes.len() > MAX_BYTES {
        return fail(StatusCode::PAYLOAD_TOO_LARGE, "The photo is too large (15 MB max).");
    }
    if sniff_image(&bytes).is_none() {
        return fail(StatusCode::UNSUPPORTED_MEDIA_TYPE, "That file isn't a JPEG, PNG, WebP or GIF image.");
    }
    if let Err(e) = me.upload_image(&slug, bytes.to_vec()).await {
        tracing::warn!(slug, error = e, "photo upload failed");
        if e.contains("isn't allowed") || e.contains("no Mealie account") {
            return fail(StatusCode::FORBIDDEN, &e);
        }
        return fail(StatusCode::BAD_GATEWAY, "Mealie couldn't save the photo. Try again, or try a different image.");
    }
    st.uploads.consume(&token);
    tracing::info!(user = me.user, slug, "recipe photo uploaded via upload link");
    let link = st.mealie.link(&slug);
    let preview = match me.fetch(&slug).await {
        Ok(r) => st.mealie.image_url(&r).as_str().map(|u| format!(r#"<img src="{}" alt="">"#, esc(u))).unwrap_or_default(),
        Err(_) => String::new(),
    };
    shell(
        StatusCode::OK,
        "Photo saved",
        &format!(
            r#"<h1>Photo saved</h1><p>“{}” has its new photo. <a href="{}">Open the recipe</a></p>{preview}"#,
            esc(&name),
            esc(&link)
        ),
    )
}
