use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::{Html, IntoResponse, Json},
    routing::get,
    Router,
};
use serde::Serialize;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio_rusqlite::Connection;

const FRONTEND_INDEX: &str = include_str!("frontend_index.html");
const FRONTEND_HUB: &str = include_str!("frontend_hub.html");
const FRONTEND_CATEGORY: &str = include_str!("frontend_category.html");
const FRONTEND_MOVERS: &str = include_str!("frontend_movers.html");
const STYLE_CSS: &str = include_str!("style.css");
const APP_JS: &str = include_str!("app.js");

// (symbol, name, category_slug, category_label, leverage, inverse)
// Every 2x/3x leveraged bull fund paired with its inverse, grouped by
// underlying index/sector. Some 2x bear products (semiconductors,
// financials, technology, biotech) trade extremely thin volume -- real and
// valid, just illiquid; included anyway for full coverage.
const TICKERS: &[(&str, &str, &str, &str, i32, bool)] = &[
    ("TQQQ", "ProShares UltraPro QQQ", "nasdaq-100", "Nasdaq-100", 3, false),
    ("SQQQ", "ProShares UltraPro Short QQQ", "nasdaq-100", "Nasdaq-100", 3, true),
    ("QLD", "ProShares Ultra QQQ", "nasdaq-100", "Nasdaq-100", 2, false),
    ("QID", "ProShares UltraShort QQQ", "nasdaq-100", "Nasdaq-100", 2, true),
    ("UPRO", "ProShares UltraPro S&P 500", "sp500", "S&P 500", 3, false),
    ("SPXU", "ProShares UltraPro Short S&P 500", "sp500", "S&P 500", 3, true),
    ("SSO", "ProShares Ultra S&P 500", "sp500", "S&P 500", 2, false),
    ("SDS", "ProShares UltraShort S&P 500", "sp500", "S&P 500", 2, true),
    ("SPXL", "Direxion Daily S&P 500 Bull", "sp500", "S&P 500", 3, false),
    ("SPXS", "Direxion Daily S&P 500 Bear", "sp500", "S&P 500", 3, true),
    ("TNA", "Direxion Daily Small Cap Bull", "small-cap", "Small Cap \u{b7} Russell 2000", 3, false),
    ("TZA", "Direxion Daily Small Cap Bear", "small-cap", "Small Cap \u{b7} Russell 2000", 3, true),
    ("UWM", "ProShares Ultra Russell2000", "small-cap", "Small Cap \u{b7} Russell 2000", 2, false),
    ("TWM", "ProShares UltraShort Russell2000", "small-cap", "Small Cap \u{b7} Russell 2000", 2, true),
    ("SOXL", "Direxion Daily Semiconductor Bull", "semiconductors", "Semiconductors", 3, false),
    ("SOXS", "Direxion Daily Semiconductor Bear", "semiconductors", "Semiconductors", 3, true),
    ("USD", "Direxion Daily Semiconductor Bull", "semiconductors", "Semiconductors", 2, false),
    ("SSG", "ProShares UltraShort Semiconductors", "semiconductors", "Semiconductors", 2, true),
    ("FAS", "Direxion Daily Financial Bull", "financials", "Financials", 3, false),
    ("FAZ", "Direxion Daily Financial Bear", "financials", "Financials", 3, true),
    ("UYG", "ProShares Ultra Financials", "financials", "Financials", 2, false),
    ("SKF", "ProShares UltraShort Financials", "financials", "Financials", 2, true),
    ("TECL", "Direxion Daily Technology Bull", "technology", "Technology", 3, false),
    ("TECS", "Direxion Daily Technology Bear", "technology", "Technology", 3, true),
    ("ROM", "ProShares Ultra Technology", "technology", "Technology", 2, false),
    ("REW", "ProShares UltraShort Technology", "technology", "Technology", 2, true),
    ("LABU", "Direxion Daily S&P Biotech Bull", "biotech", "Biotech", 3, false),
    ("LABD", "Direxion Daily S&P Biotech Bear", "biotech", "Biotech", 3, true),
    ("BIB", "ProShares Ultra Nasdaq Biotechnology", "biotech", "Biotech", 2, false),
    ("BIS", "ProShares UltraShort Nasdaq Biotechnology", "biotech", "Biotech", 2, true),
];

const ALPACA_DATA_BASE: &str = "https://data.alpaca.markets/v2";
const POLL_INTERVAL: Duration = Duration::from_secs(120);
// 1w/1m/6m returns only meaningfully shift once per trading day, so a daily
// refresh of the historical-bars snapshot is plenty.
const RETURNS_REFRESH: Duration = Duration::from_secs(24 * 3600);
// Sparkline/24h-return/since-tracked all only ever look back a few days at
// most, so older rows are pure dead weight -- pruned daily to keep the DB
// file size bounded regardless of how long this runs.
const RETENTION_DAYS: i64 = 30;
const RETENTION_SWEEP: Duration = Duration::from_secs(24 * 3600);

struct AppState {
    db: Connection,
    client: reqwest::Client,
    key_id: String,
    secret_key: String,
    returns_cache: tokio::sync::RwLock<ReturnsCache>,
}

#[derive(Default)]
struct ReturnsCache {
    as_of: i64,
    rows: Vec<ReturnsOut>,
}

#[derive(Serialize, Clone)]
struct ReturnsOut {
    symbol: String,
    week_pct: Option<f64>,
    month_pct: Option<f64>,
    six_month_pct: Option<f64>,
}

#[derive(Serialize)]
struct QuoteOut {
    symbol: String,
    price: f64,
    change: f64,
    percent_change: f64,
    volume: Option<i64>,
    return_24h_pct: Option<f64>,
    return_since_tracked_pct: Option<f64>,
    updated_at: i64,
}

#[derive(Serialize)]
struct HistoryPoint {
    ts: i64,
    price: f64,
}

#[derive(Serialize)]
struct TickerMeta {
    symbol: String,
    name: String,
    leverage: i32,
    inverse: bool,
}

#[derive(Serialize)]
struct CategoryOut {
    slug: String,
    label: String,
    tickers: Vec<TickerMeta>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let key_id = std::env::var("APCA_API_KEY_ID").expect("APCA_API_KEY_ID env var must be set");
    let secret_key =
        std::env::var("APCA_API_SECRET_KEY").expect("APCA_API_SECRET_KEY env var must be set");
    let db_path = std::env::var("DB_PATH").unwrap_or_else(|_| "/data/stocks.db".to_string());

    let db = Connection::open(&db_path).await?;
    db.call(|conn| {
        conn.execute_batch(
            "PRAGMA auto_vacuum = INCREMENTAL;
            CREATE TABLE IF NOT EXISTS quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                ts INTEGER NOT NULL,
                price REAL NOT NULL,
                change REAL NOT NULL,
                percent_change REAL NOT NULL,
                volume INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_quotes_symbol_ts ON quotes(symbol, ts);",
        )?;
        Ok(())
    })
    .await?;

    let state = Arc::new(AppState {
        db,
        client: reqwest::Client::new(),
        key_id,
        secret_key,
        returns_cache: tokio::sync::RwLock::new(ReturnsCache::default()),
    });

    {
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = poll_quotes(&state).await {
                    tracing::error!("poll_quotes failed: {e:#}");
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        });
    }

    {
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = refresh_returns(&state).await {
                    tracing::error!("refresh_returns failed: {e:#}");
                }
                tokio::time::sleep(RETURNS_REFRESH).await;
            }
        });
    }

    {
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = prune_old_quotes(&state).await {
                    tracing::error!("prune_old_quotes failed: {e:#}");
                }
                tokio::time::sleep(RETENTION_SWEEP).await;
            }
        });
    }

    let app = Router::new()
        .route("/stocks", get(index))
        .route("/stocks/movers", get(movers_page))
        .route("/stocks/style.css", get(stylesheet))
        .route("/stocks/app.js", get(app_js))
        .route("/stocks/api/categories", get(api_categories))
        .route("/stocks/api/quotes", get(api_quotes))
        .route("/stocks/api/history/:symbol", get(api_history))
        .route("/stocks/api/returns", get(api_returns))
        .route("/stocks/:category", get(category_page))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    tracing::info!("listening on 0.0.0.0:8080");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(FRONTEND_HUB)
}

async fn movers_page() -> Html<&'static str> {
    Html(FRONTEND_MOVERS)
}

async fn stylesheet() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], STYLE_CSS)
}

async fn app_js() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "application/javascript; charset=utf-8")], APP_JS)
}

async fn category_page(Path(category): Path<String>) -> impl IntoResponse {
    if TICKERS.iter().any(|t| t.2 == category) {
        (StatusCode::OK, Html(FRONTEND_CATEGORY)).into_response()
    } else {
        (StatusCode::NOT_FOUND, Html(FRONTEND_INDEX)).into_response()
    }
}

async fn api_categories() -> impl IntoResponse {
    let mut categories: Vec<CategoryOut> = Vec::new();
    for (symbol, name, slug, label, leverage, inverse) in TICKERS {
        let entry = categories.iter_mut().find(|c| c.slug == *slug);
        let meta = TickerMeta {
            symbol: symbol.to_string(),
            name: name.to_string(),
            leverage: *leverage,
            inverse: *inverse,
        };
        match entry {
            Some(c) => c.tickers.push(meta),
            None => categories.push(CategoryOut {
                slug: slug.to_string(),
                label: label.to_string(),
                tickers: vec![meta],
            }),
        }
    }
    Json(categories)
}

fn parse_num(v: &Value) -> Option<f64> {
    if let Some(s) = v.as_str() {
        s.parse::<f64>().ok()
    } else {
        v.as_f64()
    }
}

// Single batched snapshot call for every tracked symbol, every 2 minutes.
// Each snapshot carries its own previous-close, so change/% is computed
// fresh each poll with no separate cached baseline needed.
async fn poll_quotes(state: &AppState) -> anyhow::Result<()> {
    let symbols = TICKERS.iter().map(|(s, ..)| *s).collect::<Vec<_>>().join(",");
    let url = format!("{ALPACA_DATA_BASE}/stocks/snapshots?symbols={symbols}&feed=iex");
    let resp: Value = state
        .client
        .get(&url)
        .header("APCA-API-KEY-ID", &state.key_id)
        .header("APCA-API-SECRET-KEY", &state.secret_key)
        .send()
        .await?
        .json()
        .await?;

    let Some(obj) = resp.as_object() else {
        anyhow::bail!("unexpected snapshots response: {resp}");
    };

    let mut polled = 0;
    for (symbol, snapshot) in obj {
        let close = snapshot
            .get("prevDailyBar")
            .and_then(|b| b.get("c"))
            .and_then(parse_num);
        let price = snapshot
            .get("latestTrade")
            .and_then(|t| t.get("p"))
            .and_then(parse_num);
        let volume = snapshot
            .get("dailyBar")
            .and_then(|b| b.get("v"))
            .and_then(parse_num)
            .map(|v| v as i64);

        let (Some(close), Some(price)) = (close, price) else {
            continue;
        };
        let change = price - close;
        let percent_change = if close > 0.0 { (change / close) * 100.0 } else { 0.0 };

        if let Err(e) =
            insert_quote_with_volume(state, symbol, price, change, percent_change, volume).await
        {
            tracing::warn!("failed to record quote for {symbol}: {e:#}");
        } else {
            polled += 1;
        }
    }

    tracing::info!("polled {polled} symbols");
    Ok(())
}

async fn prune_old_quotes(state: &AppState) -> anyhow::Result<()> {
    let cutoff = chrono::Utc::now().timestamp() - RETENTION_DAYS * 86_400;
    let deleted = state
        .db
        .call(move |conn| {
            let deleted = conn.execute("DELETE FROM quotes WHERE ts < ?1", rusqlite::params![cutoff])?;
            // Reclaims freed pages incrementally rather than a full VACUUM,
            // which would need an exclusive lock over the whole file.
            conn.execute_batch("PRAGMA incremental_vacuum;")?;
            Ok::<_, tokio_rusqlite::Error>(deleted)
        })
        .await?;
    tracing::info!("pruned {deleted} quote rows older than {RETENTION_DAYS}d");
    Ok(())
}

// Fetches ~200 calendar days of split-adjusted daily bars (comfortably
// covers 6 months of trading days) and computes 1-week/1-month/6-month %
// change per symbol. The multi-symbol bars endpoint paginates via
// next_page_token and its `limit` caps bars across ALL symbols combined,
// not per-symbol -- skipping pagination silently truncates later symbols.
// Prices must be split-adjusted (adjustment=split) or a leveraged inverse
// fund's reverse split reads as an enormous fake gain.
async fn refresh_returns(state: &AppState) -> anyhow::Result<()> {
    let symbols = TICKERS.iter().map(|(s, ..)| *s).collect::<Vec<_>>().join(",");
    let end = chrono::Utc::now().date_naive();
    let start = end - chrono::Duration::days(200);

    let mut closes_by_symbol: std::collections::HashMap<String, Vec<f64>> = std::collections::HashMap::new();
    let mut page_token: Option<String> = None;

    loop {
        let mut url = format!(
            "{ALPACA_DATA_BASE}/stocks/bars?symbols={symbols}&timeframe=1Day&start={start}&end={end}&feed=iex&limit=10000&adjustment=split"
        );
        if let Some(token) = &page_token {
            url.push_str(&format!("&page_token={token}"));
        }
        let resp: Value = state
            .client
            .get(&url)
            .header("APCA-API-KEY-ID", &state.key_id)
            .header("APCA-API-SECRET-KEY", &state.secret_key)
            .send()
            .await?
            .json()
            .await?;

        if let Some(bars) = resp.get("bars").and_then(|b| b.as_object()) {
            for (symbol, arr) in bars {
                let entry = closes_by_symbol.entry(symbol.clone()).or_default();
                if let Some(arr) = arr.as_array() {
                    for bar in arr {
                        if let Some(c) = bar.get("c").and_then(|c| c.as_f64()) {
                            entry.push(c);
                        }
                    }
                }
            }
        }

        page_token = resp.get("next_page_token").and_then(|t| t.as_str()).map(String::from);
        if page_token.is_none() {
            break;
        }
    }

    fn pct_change(closes: &[f64], trading_days_ago: usize) -> Option<f64> {
        if closes.len() <= trading_days_ago {
            return None;
        }
        let latest = closes[closes.len() - 1];
        let past = closes[closes.len() - 1 - trading_days_ago];
        if past == 0.0 {
            return None;
        }
        Some((latest - past) / past * 100.0)
    }

    let rows: Vec<ReturnsOut> = TICKERS
        .iter()
        .map(|(symbol, ..)| {
            let closes = closes_by_symbol.get(*symbol).map(Vec::as_slice).unwrap_or(&[]);
            let six_month_days = closes.len().saturating_sub(1).min(126);
            ReturnsOut {
                symbol: symbol.to_string(),
                week_pct: pct_change(closes, 5),
                month_pct: pct_change(closes, 21),
                six_month_pct: pct_change(closes, six_month_days),
            }
        })
        .collect();

    let mut cache = state.returns_cache.write().await;
    cache.as_of = chrono::Utc::now().timestamp();
    cache.rows = rows;
    tracing::info!("refreshed 1w/1m/6m returns for {} symbols", cache.rows.len());
    Ok(())
}

async fn insert_quote_with_volume(
    state: &AppState,
    symbol: &str,
    price: f64,
    change: f64,
    percent_change: f64,
    volume: Option<i64>,
) -> anyhow::Result<()> {
    let ts = chrono::Utc::now().timestamp();
    let symbol = symbol.to_string();
    state
        .db
        .call(move |conn| {
            conn.execute(
                "INSERT INTO quotes (symbol, ts, price, change, percent_change, volume) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![symbol, ts, price, change, percent_change, volume],
            )?;
            Ok(())
        })
        .await?;
    Ok(())
}

async fn api_quotes(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut out = Vec::new();

    for (symbol, ..) in TICKERS {
        let symbol_owned = symbol.to_string();
        let latest = state
            .db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT ts, price, change, percent_change, volume FROM quotes WHERE symbol = ?1 ORDER BY ts DESC LIMIT 1",
                )?;
                let row = stmt
                    .query_row(rusqlite::params![symbol_owned], |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, f64>(1)?,
                            r.get::<_, f64>(2)?,
                            r.get::<_, f64>(3)?,
                            r.get::<_, Option<i64>>(4)?,
                        ))
                    })
                    .ok();
                Ok::<_, tokio_rusqlite::Error>(row)
            })
            .await
            .ok()
            .flatten();

        let Some((ts, price, change, percent_change, volume)) = latest else {
            continue;
        };

        let symbol_owned = symbol.to_string();
        let return_24h_pct = state
            .db
            .call(move |conn| {
                let cutoff = chrono::Utc::now().timestamp() - 86_400;
                let mut stmt = conn.prepare(
                    "SELECT price FROM quotes WHERE symbol = ?1 AND ts <= ?2 ORDER BY ts DESC LIMIT 1",
                )?;
                let row = stmt
                    .query_row(rusqlite::params![symbol_owned, cutoff], |r| r.get::<_, f64>(0))
                    .ok();
                Ok::<_, tokio_rusqlite::Error>(row)
            })
            .await
            .ok()
            .flatten()
            .map(|past_price| ((price - past_price) / past_price) * 100.0);

        let symbol_owned = symbol.to_string();
        let return_since_tracked_pct = state
            .db
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT price FROM quotes WHERE symbol = ?1 ORDER BY ts ASC LIMIT 1",
                )?;
                let row = stmt
                    .query_row(rusqlite::params![symbol_owned], |r| r.get::<_, f64>(0))
                    .ok();
                Ok::<_, tokio_rusqlite::Error>(row)
            })
            .await
            .ok()
            .flatten()
            .map(|first_price| ((price - first_price) / first_price) * 100.0);

        out.push(QuoteOut {
            symbol: symbol.to_string(),
            price,
            change,
            percent_change,
            volume,
            return_24h_pct,
            return_since_tracked_pct,
            updated_at: ts,
        });
    }

    Json(out)
}

async fn api_history(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> impl IntoResponse {
    if !TICKERS.iter().any(|(s, ..)| *s == symbol) {
        return (StatusCode::NOT_FOUND, Json(Vec::<HistoryPoint>::new()));
    }

    let points = state
        .db
        .call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT ts, price FROM quotes WHERE symbol = ?1 ORDER BY ts DESC LIMIT 100",
            )?;
            let rows = stmt
                .query_map(rusqlite::params![symbol], |r| {
                    Ok(HistoryPoint {
                        ts: r.get(0)?,
                        price: r.get(1)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok::<_, tokio_rusqlite::Error>(rows)
        })
        .await
        .unwrap_or_default();

    let mut points = points;
    points.reverse();
    (StatusCode::OK, Json(points))
}

#[derive(Serialize)]
struct ReturnsRow {
    symbol: String,
    day_pct: Option<f64>,
    week_pct: Option<f64>,
    month_pct: Option<f64>,
    six_month_pct: Option<f64>,
}

#[derive(Serialize)]
struct ReturnsResponse {
    as_of: i64,
    rows: Vec<ReturnsRow>,
}

async fn api_returns(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let cache = state.returns_cache.read().await;
    let mut rows = Vec::with_capacity(cache.rows.len());

    for r in &cache.rows {
        let symbol_owned = r.symbol.clone();
        let day_pct = state
            .db
            .call(move |conn| {
                let val = conn
                    .query_row(
                        "SELECT percent_change FROM quotes WHERE symbol = ?1 ORDER BY ts DESC LIMIT 1",
                        rusqlite::params![symbol_owned],
                        |row| row.get::<_, f64>(0),
                    )
                    .ok();
                Ok::<_, tokio_rusqlite::Error>(val)
            })
            .await
            .ok()
            .flatten();

        rows.push(ReturnsRow {
            symbol: r.symbol.clone(),
            day_pct,
            week_pct: r.week_pct,
            month_pct: r.month_pct,
            six_month_pct: r.six_month_pct,
        });
    }

    Json(ReturnsResponse { as_of: cache.as_of, rows })
}
