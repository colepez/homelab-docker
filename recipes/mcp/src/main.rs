//! recipes-mcp: a Streamable-HTTP MCP server that lets Claude search, read and write recipes in Mealie.
//!
//! Auth is a capability URL: the MCP endpoint is mounted only at `/mcp/<MCP_PATH_SECRET>`, and every other path
//! is a 404. Claude.ai custom connectors can't send a static bearer header, so the secret lives in the URL.
//! Mealie is reached over the internal docker network with a long-lived API token.

use std::{
    net::IpAddr,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use axum::{http::StatusCode, routing::get};
use reqwest::{Method, Url};
use rmcp::{
    ErrorData as McpError, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::*,
    schemars::{self, JsonSchema},
    tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio_util::sync::CancellationToken;

const LISTEN: &str = "0.0.0.0:8080";

// ---------------------------------------------------------------------------------------------------------------
// Mealie client
// ---------------------------------------------------------------------------------------------------------------

struct Mealie {
    http: reqwest::Client,
    base: Url,
    token: String,
    public_url: String,
    group_slug: String,
}

type ToolResult = Result<CallToolResult, McpError>;

impl Mealie {
    async fn call(&self, method: Method, path: &str, query: &[(&str, String)], body: Option<Value>) -> Result<Value, String> {
        let mut url = self.base.join(path).map_err(|e| e.to_string())?;
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query.iter().map(|(k, v)| (*k, v.as_str())));
        }
        let mut req = self.http.request(method.clone(), url).bearer_auth(&self.token);
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req.send().await.map_err(|e| format!("Mealie is unreachable: {e}"))?;
        let status = resp.status();
        let text = resp.text().await.map_err(|e| format!("reading Mealie response: {e}"))?;
        if !status.is_success() {
            tracing::warn!(%method, path, %status, "mealie error");
            return Err(format!("Mealie returned {status}: {}", truncate(&text, 600)));
        }
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&text).map_err(|e| format!("Mealie sent invalid JSON: {e}"))
    }

    fn link(&self, slug: &str) -> String {
        format!("{}/g/{}/r/{}", self.public_url, self.group_slug, slug)
    }

    /// Map tag/category names to Mealie organizer objects, creating any that don't exist yet.
    /// Matching is case-insensitive so "Dinner" and "dinner" don't become two tags.
    async fn organizers(&self, kind: &str, names: &[String]) -> Result<Value, String> {
        let path = format!("/api/organizers/{kind}");
        let existing = self.call(Method::GET, &path, &[("perPage", "-1".into())], None).await?;
        let items = existing["items"].as_array().cloned().unwrap_or_default();
        let mut out = Vec::new();
        for name in names.iter().map(|n| n.trim()).filter(|n| !n.is_empty()) {
            let found = items.iter().find(|i| i["name"].as_str().is_some_and(|s| s.eq_ignore_ascii_case(name)));
            match found {
                Some(item) => out.push(item.clone()),
                None => out.push(self.call(Method::POST, &path, &[], Some(json!({ "name": name }))).await?),
            }
        }
        Ok(Value::Array(out))
    }

    /// Build a PATCH body from the optional recipe fields; only fields that were given are touched.
    async fn patch_body(&self, f: &RecipeFields) -> Result<Map<String, Value>, String> {
        let mut p = Map::new();
        let mut set = |k: &str, v: Value| {
            p.insert(k.to_owned(), v);
        };
        if let Some(v) = &f.description { set("description", json!(v)); }
        if let Some(v) = &f.recipe_yield { set("recipeYield", json!(v)); }
        if let Some(v) = f.servings { set("recipeServings", json!(v)); }
        if let Some(v) = &f.prep_time { set("prepTime", json!(v)); }
        if let Some(v) = &f.cook_time { set("performTime", json!(v)); }
        if let Some(v) = &f.total_time { set("totalTime", json!(v)); }
        if let Some(v) = &f.source_url { set("orgURL", json!(v)); }
        if let Some(v) = &f.ingredients {
            set("recipeIngredient", v.iter().map(|line| ingredient(line)).collect());
        }
        if let Some(v) = &f.instructions {
            set("recipeInstructions", v.iter().map(|s| json!({ "text": s })).collect());
        }
        if let Some(v) = &f.notes {
            set("notes", v.iter().map(|n| json!({ "title": n.title.clone().unwrap_or_default(), "text": n.text })).collect());
        }
        if let Some(v) = &f.tags {
            let tags = self.organizers("tags", v).await?;
            p.insert("tags".into(), tags);
        }
        if let Some(v) = &f.categories {
            let cats = self.organizers("categories", v).await?;
            p.insert("recipeCategory".into(), cats);
        }
        Ok(p)
    }
}

/// An ingredient line. A line starting with "## " becomes a section header ("## For the sauce").
fn ingredient(line: &str) -> Value {
    match line.trim().strip_prefix("## ") {
        Some(title) => json!({ "title": title.trim(), "note": "" }),
        None => json!({ "note": line.trim() }),
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_owned();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &s[..end])
}

/// Slugs go straight into URL paths, so only accept Mealie's slug alphabet (no "/", "..", "?").
fn check_slug(slug: &str) -> Result<&str, String> {
    let s = slug.trim();
    if !s.is_empty() && s.len() <= 200 && s.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
        Ok(s)
    } else {
        Err(format!("invalid recipe slug {slug:?}: use the slug from search_recipes, e.g. \"chicken-tikka-masala\""))
    }
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            !(v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_documentation()
                || o[0] == 0
                || (o[0] == 100 && (o[1] & 0xc0) == 64) // 100.64.0.0/10 (CGNAT / Tailscale)
                || o[0] >= 224)
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_public_ip(IpAddr::V4(v4));
            }
            let seg = v6.segments()[0];
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (seg & 0xfe00) == 0xfc00 // unique local
                || (seg & 0xffc0) == 0xfe80) // link local
        }
    }
}

/// Mealie fetches import URLs from inside the homelab. Refuse anything that resolves to a private address,
/// so a prompt-injected URL can't make it probe LAN services.
async fn check_import_url(raw: &str) -> Result<Url, String> {
    let url = Url::parse(raw.trim()).map_err(|e| format!("invalid URL: {e}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("only http(s) URLs can be imported".into());
    }
    let host = url.host_str().ok_or("URL has no host")?.to_owned();
    let port = url.port_or_known_default().unwrap_or(443);
    let addrs: Vec<_> = tokio::net::lookup_host((host.trim_matches(['[', ']']), port))
        .await
        .map_err(|e| format!("can't resolve {host}: {e}"))?
        .collect();
    if addrs.is_empty() || addrs.iter().any(|a| !is_public_ip(a.ip())) {
        return Err(format!("{host} resolves to a private or local address; only public recipe sites can be imported"));
    }
    Ok(url)
}

// ---------------------------------------------------------------------------------------------------------------
// Tool inputs
// ---------------------------------------------------------------------------------------------------------------

#[derive(Debug, Deserialize, JsonSchema)]
struct NoteIn {
    /// Optional heading for the note, e.g. "Make ahead"
    title: Option<String>,
    /// Note body
    text: String,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct RecipeFields {
    /// One or two sentence summary shown under the title
    description: Option<String>,
    /// Free-text yield, e.g. "4 servings", "2 loaves", "24 cookies"
    recipe_yield: Option<String>,
    /// Number of servings, if known
    servings: Option<f64>,
    /// Human-readable duration, e.g. "15 minutes"
    prep_time: Option<String>,
    /// Human-readable active cooking time, e.g. "1 hour 10 minutes"
    cook_time: Option<String>,
    /// Human-readable total time, e.g. "1 hour 25 minutes"
    total_time: Option<String>,
    /// Ingredient lines in order, written as a person would read them: "2 cups all-purpose flour, sifted".
    /// A line starting with "## " is a section header, e.g. "## For the dressing". Replaces all existing ingredients.
    ingredients: Option<Vec<String>>,
    /// Instruction steps in order, one step per entry (no leading numbers). Replaces all existing steps.
    instructions: Option<Vec<String>>,
    /// Extra notes/tips shown below the recipe. Replaces all existing notes.
    notes: Option<Vec<NoteIn>>,
    /// Tag names, e.g. ["vegetarian", "quick"]. Missing tags are created. Replaces existing tags.
    tags: Option<Vec<String>>,
    /// Category names, e.g. ["Dinner"]. Missing categories are created. Replaces existing categories.
    categories: Option<Vec<String>>,
    /// Original source URL, if the recipe came from somewhere
    source_url: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SearchArgs {
    /// Free-text search over names, descriptions and ingredients. Omit to list recent recipes.
    query: Option<String>,
    /// Only recipes with this tag name
    tag: Option<String>,
    /// Only recipes in this category name
    category: Option<String>,
    /// Max results (default 20, max 100)
    limit: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SlugArgs {
    /// Recipe slug, as returned by search_recipes (e.g. "chicken-tikka-masala")
    slug: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct CreateArgs {
    /// Recipe title
    name: String,
    #[serde(flatten)]
    fields: RecipeFields,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct UpdateArgs {
    /// Slug of the recipe to change
    slug: String,
    /// New title (this can change the slug; the new slug is returned)
    name: Option<String>,
    #[serde(flatten)]
    fields: RecipeFields,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ImportArgs {
    /// Public web page containing a recipe (most recipe sites with schema.org markup work)
    url: String,
    /// Also import the site's keywords as tags (default false)
    include_tags: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct DeleteArgs {
    /// Slug of the recipe to delete
    slug: String,
    /// Must be true. Ask the user before deleting.
    confirm: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MealPlanAddArgs {
    /// Date in YYYY-MM-DD
    date: String,
    /// Recipe slug to plan. Omit to add a free-text entry using `title`.
    slug: Option<String>,
    /// breakfast, lunch, dinner, side, snack, drink or dessert (default dinner)
    meal: Option<String>,
    /// Title for a free-text entry (when no slug)
    title: Option<String>,
    /// Optional note
    note: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MealPlanGetArgs {
    /// First day, YYYY-MM-DD
    start_date: String,
    /// Last day, YYYY-MM-DD
    end_date: String,
}

// ---------------------------------------------------------------------------------------------------------------
// MCP server
// ---------------------------------------------------------------------------------------------------------------

#[derive(Clone)]
struct Recipes {
    mealie: Arc<Mealie>,
    tool_router: ToolRouter<Recipes>,
}

fn ok_json(v: Value) -> ToolResult {
    Ok(CallToolResult::success(vec![ContentBlock::text(
        serde_json::to_string_pretty(&v).unwrap_or_default(),
    )]))
}

fn tool_err(msg: impl Into<String>) -> ToolResult {
    Ok(CallToolResult::error(vec![ContentBlock::text(msg.into())]))
}

/// Unwrap a Result<_, String>, turning the error into a tool-level error the model can read.
macro_rules! try_tool {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => return tool_err(e),
        }
    };
}

fn names(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| a.iter().filter_map(|i| i["name"].as_str().map(str::to_owned)).collect())
        .unwrap_or_default()
}

fn str_or_null(v: &Value) -> Value {
    match v.as_str() {
        Some(s) if !s.is_empty() => json!(s),
        _ => Value::Null,
    }
}

#[tool_router]
impl Recipes {
    fn new(mealie: Arc<Mealie>) -> Self {
        Self { mealie, tool_router: Self::tool_router() }
    }

    fn summary(&self, r: &Value) -> Value {
        let slug = r["slug"].as_str().unwrap_or_default();
        json!({
            "name": r["name"],
            "slug": slug,
            "description": str_or_null(&r["description"]),
            "total_time": str_or_null(&r["totalTime"]),
            "tags": names(&r["tags"]),
            "categories": names(&r["recipeCategory"]),
            "url": self.mealie.link(slug),
        })
    }

    fn full(&self, r: &Value) -> Value {
        let slug = r["slug"].as_str().unwrap_or_default();
        let ingredients: Vec<String> = r["recipeIngredient"]
            .as_array()
            .map(|a| {
                a.iter()
                    .flat_map(|i| {
                        let mut lines = Vec::new();
                        if let Some(t) = i["title"].as_str().filter(|t| !t.is_empty()) {
                            lines.push(format!("## {t}"));
                        }
                        let text = i["display"].as_str().filter(|s| !s.is_empty()).or(i["note"].as_str()).unwrap_or("");
                        if !text.is_empty() {
                            lines.push(text.to_owned());
                        }
                        lines
                    })
                    .collect()
            })
            .unwrap_or_default();
        let instructions: Vec<String> = r["recipeInstructions"]
            .as_array()
            .map(|a| a.iter().filter_map(|s| s["text"].as_str().map(str::to_owned)).collect())
            .unwrap_or_default();
        let notes: Vec<Value> = r["notes"]
            .as_array()
            .map(|a| a.iter().map(|n| json!({ "title": n["title"], "text": n["text"] })).collect())
            .unwrap_or_default();
        json!({
            "id": r["id"],
            "name": r["name"],
            "slug": slug,
            "url": self.mealie.link(slug),
            "description": str_or_null(&r["description"]),
            "recipe_yield": str_or_null(&r["recipeYield"]),
            "servings": r["recipeServings"],
            "prep_time": str_or_null(&r["prepTime"]),
            "cook_time": str_or_null(&r["performTime"]),
            "total_time": str_or_null(&r["totalTime"]),
            "ingredients": ingredients,
            "instructions": instructions,
            "notes": notes,
            "tags": names(&r["tags"]),
            "categories": names(&r["recipeCategory"]),
            "source_url": str_or_null(&r["orgURL"]),
            "rating": r["rating"],
            "updated_at": r["updatedAt"],
        })
    }

    async fn fetch(&self, slug: &str) -> Result<Value, String> {
        self.mealie.call(Method::GET, &format!("/api/recipes/{slug}"), &[], None).await
    }

    #[tool(
        description = "Search the recipe library. Returns name, slug, short description, tags and a link for each match. \
                       With no query, lists the most recently updated recipes.",
        annotations(title = "Search recipes", read_only_hint = true)
    )]
    async fn search_recipes(&self, Parameters(a): Parameters<SearchArgs>) -> ToolResult {
        let mut q = vec![
            ("perPage", a.limit.unwrap_or(20).clamp(1, 100).to_string()),
            ("orderBy", "updatedAt".to_string()),
            ("orderDirection", "desc".to_string()),
        ];
        if let Some(s) = a.query.filter(|s| !s.trim().is_empty()) {
            q.push(("search", s));
        }
        for (kind, param, wanted) in [("tags", "tags", a.tag), ("categories", "categories", a.category)] {
            if let Some(name) = wanted {
                let all = try_tool!(self.mealie.call(Method::GET, &format!("/api/organizers/{kind}"), &[("perPage", "-1".into())], None).await);
                let hit = all["items"].as_array().and_then(|items| {
                    items.iter().find(|i| i["name"].as_str().is_some_and(|n| n.eq_ignore_ascii_case(name.trim())))
                });
                match hit {
                    Some(item) => q.push((param, item["id"].as_str().unwrap_or_default().to_owned())),
                    None => return ok_json(json!({ "total": 0, "recipes": [], "note": format!("no {kind} named {name:?}") })),
                }
            }
        }
        let res = try_tool!(self.mealie.call(Method::GET, "/api/recipes", &q, None).await);
        let recipes: Vec<Value> = res["items"].as_array().map(|a| a.iter().map(|r| self.summary(r)).collect()).unwrap_or_default();
        ok_json(json!({ "total": res["total"], "recipes": recipes }))
    }

    #[tool(
        description = "Get a full recipe by slug: ingredients, steps, times, yield, notes, tags and categories.",
        annotations(title = "Get recipe", read_only_hint = true)
    )]
    async fn get_recipe(&self, Parameters(a): Parameters<SlugArgs>) -> ToolResult {
        let slug = try_tool!(check_slug(&a.slug));
        let r = try_tool!(self.fetch(slug).await);
        ok_json(self.full(&r))
    }

    #[tool(
        description = "Create a new recipe. Give ingredients and instructions as plain lines. Returns the saved recipe and its link.",
        annotations(title = "Create recipe", read_only_hint = false, destructive_hint = false)
    )]
    async fn create_recipe(&self, Parameters(a): Parameters<CreateArgs>) -> ToolResult {
        let name = a.name.trim();
        if name.is_empty() {
            return tool_err("name is required");
        }
        let slug = try_tool!(self.mealie.call(Method::POST, "/api/recipes", &[], Some(json!({ "name": name }))).await);
        let slug = slug.as_str().unwrap_or_default().to_owned();
        let patch = match self.mealie.patch_body(&a.fields).await {
            Ok(p) => p,
            Err(e) => {
                let _ = self.mealie.call(Method::DELETE, &format!("/api/recipes/{slug}"), &[], None).await;
                return tool_err(e);
            }
        };
        if !patch.is_empty() {
            if let Err(e) = self.mealie.call(Method::PATCH, &format!("/api/recipes/{slug}"), &[], Some(Value::Object(patch))).await {
                // Don't leave an empty stub behind
                let _ = self.mealie.call(Method::DELETE, &format!("/api/recipes/{slug}"), &[], None).await;
                return tool_err(format!("recipe not saved: {e}"));
            }
        }
        let r = try_tool!(self.fetch(&slug).await);
        tracing::info!(slug, "created recipe");
        ok_json(self.full(&r))
    }

    #[tool(
        description = "Update an existing recipe. Only the fields you pass change; list fields (ingredients, instructions, \
                       notes, tags, categories) replace the old list entirely, so fetch the recipe first and send the full edited list.",
        annotations(title = "Update recipe", read_only_hint = false, destructive_hint = true, idempotent_hint = true)
    )]
    async fn update_recipe(&self, Parameters(a): Parameters<UpdateArgs>) -> ToolResult {
        let slug = try_tool!(check_slug(&a.slug));
        let mut patch = try_tool!(self.mealie.patch_body(&a.fields).await);
        if let Some(n) = a.name.as_deref().map(str::trim).filter(|n| !n.is_empty()) {
            patch.insert("name".into(), json!(n));
        }
        if patch.is_empty() {
            return tool_err("nothing to update: pass at least one field");
        }
        let r = try_tool!(self.mealie.call(Method::PATCH, &format!("/api/recipes/{slug}"), &[], Some(Value::Object(patch))).await);
        // PATCH returns the updated recipe; re-fetch by its (possibly new) slug for a consistent view
        let new_slug = r["slug"].as_str().unwrap_or(slug).to_owned();
        let r = try_tool!(self.fetch(&new_slug).await);
        tracing::info!(slug = new_slug, "updated recipe");
        ok_json(self.full(&r))
    }

    #[tool(
        description = "Import a recipe from a public web page URL (Mealie scrapes it). Returns the imported recipe; \
                       review it and use update_recipe to fix anything the scraper got wrong.",
        annotations(title = "Import recipe from URL", read_only_hint = false, destructive_hint = false, open_world_hint = true)
    )]
    async fn import_recipe_from_url(&self, Parameters(a): Parameters<ImportArgs>) -> ToolResult {
        let url = try_tool!(check_import_url(&a.url).await);
        let body = json!({ "url": url.as_str(), "includeTags": a.include_tags.unwrap_or(false) });
        let slug = try_tool!(self.mealie.call(Method::POST, "/api/recipes/create/url", &[], Some(body)).await);
        let slug = slug.as_str().unwrap_or_default().to_owned();
        let r = try_tool!(self.fetch(&slug).await);
        tracing::info!(slug, "imported recipe");
        ok_json(self.full(&r))
    }

    #[tool(
        description = "Permanently delete a recipe. Only do this when the user explicitly asks; set confirm=true.",
        annotations(title = "Delete recipe", read_only_hint = false, destructive_hint = true)
    )]
    async fn delete_recipe(&self, Parameters(a): Parameters<DeleteArgs>) -> ToolResult {
        if !a.confirm {
            return tool_err("not deleted: confirm must be true (ask the user first)");
        }
        let slug = try_tool!(check_slug(&a.slug));
        try_tool!(self.mealie.call(Method::DELETE, &format!("/api/recipes/{slug}"), &[], None).await);
        tracing::info!(slug, "deleted recipe");
        ok_json(json!({ "deleted": slug }))
    }

    #[tool(
        description = "List all tags and categories with recipe counts, to reuse existing names instead of inventing near-duplicates.",
        annotations(title = "List tags and categories", read_only_hint = true)
    )]
    async fn list_tags_and_categories(&self) -> ToolResult {
        let mut out = Map::new();
        for kind in ["tags", "categories"] {
            let res = try_tool!(self.mealie.call(Method::GET, &format!("/api/organizers/{kind}"), &[("perPage", "-1".into())], None).await);
            let list: Vec<Value> = res["items"]
                .as_array()
                .map(|a| a.iter().map(|i| json!({ "name": i["name"], "recipes": i["recipeCount"] })).collect())
                .unwrap_or_default();
            out.insert(kind.into(), Value::Array(list));
        }
        ok_json(Value::Object(out))
    }

    #[tool(
        description = "Add a recipe (or a free-text entry) to the meal plan on a given date.",
        annotations(title = "Add to meal plan", read_only_hint = false, destructive_hint = false)
    )]
    async fn add_to_meal_plan(&self, Parameters(a): Parameters<MealPlanAddArgs>) -> ToolResult {
        let meal = a.meal.unwrap_or_else(|| "dinner".into()).to_lowercase();
        if !["breakfast", "lunch", "dinner", "side", "snack", "drink", "dessert"].contains(&meal.as_str()) {
            return tool_err(format!("unknown meal {meal:?}"));
        }
        let mut body = json!({
            "date": a.date.trim(),
            "entryType": meal,
            "title": a.title.unwrap_or_default(),
            "text": a.note.unwrap_or_default(),
        });
        if let Some(slug) = a.slug {
            let slug = try_tool!(check_slug(&slug));
            let r = try_tool!(self.fetch(slug).await);
            body["recipeId"] = r["id"].clone();
        } else if body["title"].as_str().unwrap_or_default().is_empty() {
            return tool_err("pass a recipe slug or a title");
        }
        let res = try_tool!(self.mealie.call(Method::POST, "/api/households/mealplans", &[], Some(body)).await);
        ok_json(json!({ "added": { "date": res["date"], "meal": res["entryType"], "title": res["title"],
                                    "recipe": res["recipe"]["name"], "id": res["id"] } }))
    }

    #[tool(
        description = "Show the meal plan between two dates (inclusive).",
        annotations(title = "Get meal plan", read_only_hint = true)
    )]
    async fn get_meal_plan(&self, Parameters(a): Parameters<MealPlanGetArgs>) -> ToolResult {
        let q = [
            ("start_date", a.start_date.trim().to_owned()),
            ("end_date", a.end_date.trim().to_owned()),
            ("perPage", "-1".to_owned()),
            ("orderBy", "date".to_owned()),
            ("orderDirection", "asc".to_owned()),
        ];
        let res = try_tool!(self.mealie.call(Method::GET, "/api/households/mealplans", &q, None).await);
        let entries: Vec<Value> = res["items"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|e| {
                        json!({
                            "date": e["date"], "meal": e["entryType"],
                            "recipe": e["recipe"]["name"], "slug": e["recipe"]["slug"],
                            "title": str_or_null(&e["title"]), "note": str_or_null(&e["text"]),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        ok_json(json!({ "entries": entries }))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for Recipes {
    fn get_info(&self) -> ServerInfo {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")).with_title("Recipes (Mealie)"))
            .with_instructions(
                "Cole's recipe library (Mealie). Search before creating to avoid duplicates, reuse existing tag and \
                 category names (list_tags_and_categories), and always share the recipe link after saving. \
                 Never delete without the user's explicit request.",
            )
    }
}

// ---------------------------------------------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------------------------------------------

fn healthcheck() -> anyhow::Result<()> {
    use std::io::{Read, Write};
    let mut s = std::net::TcpStream::connect_timeout(&"127.0.0.1:8080".parse()?, Duration::from_secs(3))?;
    s.set_read_timeout(Some(Duration::from_secs(3)))?;
    s.write_all(b"GET /healthz HTTP/1.0\r\nHost: localhost\r\n\r\n")?;
    let mut buf = String::new();
    s.read_to_string(&mut buf)?;
    anyhow::ensure!(buf.starts_with("HTTP/1.1 200") || buf.starts_with("HTTP/1.0 200"), "unhealthy");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::args().any(|a| a == "--healthcheck") {
        return healthcheck();
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,rmcp=warn".into()),
        )
        .init();

    let env = |k: &str| std::env::var(k).with_context(|| format!("{k} is not set"));
    let secret = env("MCP_PATH_SECRET")?;
    anyhow::ensure!(
        secret.len() >= 32 && secret.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
        "MCP_PATH_SECRET must be at least 32 URL-safe characters"
    );
    let public_url = env("RECIPES_BASE_URL")?.trim_end_matches('/').to_owned();
    let allowed_hosts: Vec<String> = env("MCP_ALLOWED_HOSTS")?
        .split(',')
        .map(|h| h.trim().to_owned())
        .filter(|h| !h.is_empty())
        .collect();

    let mealie = Arc::new(Mealie {
        http: reqwest::Client::builder().timeout(Duration::from_secs(60)).build()?,
        base: Url::parse(&std::env::var("MEALIE_URL").unwrap_or_else(|_| "http://mealie:9000".into()))?,
        token: env("MEALIE_API_TOKEN")?,
        public_url,
        group_slug: std::env::var("MEALIE_GROUP_SLUG").unwrap_or_else(|_| "home".into()),
    });

    let ct = CancellationToken::new();
    let config = StreamableHttpServerConfig::default()
        .with_allowed_hosts(allowed_hosts)
        .with_legacy_session_mode(false)
        .with_json_response(true)
        .with_cancellation_token(ct.child_token());
    let m = mealie.clone();
    let service = StreamableHttpService::new(move || Ok(Recipes::new(m.clone())), LocalSessionManager::default().into(), config);

    let app = axum::Router::new()
        .nest_service(&format!("/mcp/{secret}"), service)
        .route("/healthz", get(|| async { "ok" }))
        .fallback(|| async { StatusCode::NOT_FOUND });

    let listener = tokio::net::TcpListener::bind(LISTEN).await?;
    tracing::info!("recipes-mcp listening on {LISTEN}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            ct.cancel();
        })
        .await?;
    Ok(())
}
