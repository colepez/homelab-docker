use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse, Json},
    routing::get,
    Router,
};
use serde::Serialize;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio_rusqlite::Connection;

const FRONTEND_INDEX: &str = include_str!("frontend_index.html");
const FRONTEND_3X: &str = include_str!("frontend_3x.html");

const TICKERS: &[(&str, &str)] = &[
    ("TQQQ", "ProShares UltraPro QQQ (3x Nasdaq-100)"),
    ("SOXL", "Direxion Daily Semiconductor Bull 3x"),
    ("UPRO", "ProShares UltraPro S&P 500"),
    ("SPXL", "Direxion Daily S&P 500 Bull 3x"),
    ("TNA", "Direxion Daily Small Cap Bull 3x"),
    ("FAS", "Direxion Daily Financial Bull 3x"),
    ("TECL", "Direxion Daily Technology Bull 3x"),
    ("LABU", "Direxion Daily S&P Biotech Bull 3x"),
    ("SQQQ", "ProShares UltraPro Short QQQ (3x inverse Nasdaq)"),
];

const POLL_INTERVAL: Duration = Duration::from_secs(300);

struct AppState {
    db: Connection,
    client: reqwest::Client,
    api_key: String,
}

#[derive(Serialize)]
struct QuoteOut {
    symbol: String,
    name: String,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let api_key =
        std::env::var("TWELVEDATA_API_KEY").expect("TWELVEDATA_API_KEY env var must be set");
    let db_path = std::env::var("DB_PATH").unwrap_or_else(|_| "/data/stocks.db".to_string());

    let db = Connection::open(&db_path).await?;
    db.call(|conn| {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS quotes (
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
        api_key,
    });

    // background poller
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

    let app = Router::new()
        .route("/stocks", get(index))
        .route("/stocks/3x", get(dashboard_3x))
        .route("/stocks/3x/api/quotes", get(api_quotes))
        .route("/stocks/3x/api/history/:symbol", get(api_history))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    tracing::info!("listening on 0.0.0.0:8080");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(FRONTEND_INDEX)
}

async fn dashboard_3x() -> Html<&'static str> {
    Html(FRONTEND_3X)
}

fn parse_num(v: &Value) -> Option<f64> {
    if let Some(s) = v.as_str() {
        s.parse::<f64>().ok()
    } else {
        v.as_f64()
    }
}

async fn poll_quotes(state: &AppState) -> anyhow::Result<()> {
    let symbols = TICKERS.iter().map(|(s, _)| *s).collect::<Vec<_>>().join(",");
    let url = format!(
        "https://api.twelvedata.com/quote?symbol={}&apikey={}",
        symbols, state.api_key
    );
    let resp: Value = state.client.get(&url).send().await?.json().await?;

    let ts = chrono::Utc::now().timestamp();

    // Twelve Data returns a flat object for a single symbol, or an object
    // keyed by symbol for multiple. We always request multiple.
    let entries: Vec<(&str, &Value)> = if TICKERS.len() == 1 {
        vec![(TICKERS[0].0, &resp)]
    } else {
        TICKERS
            .iter()
            .filter_map(|(sym, _)| resp.get(*sym).map(|v| (*sym, v)))
            .collect()
    };

    for (symbol, quote) in entries {
        if quote.get("status").and_then(|s| s.as_str()) == Some("error") {
            tracing::warn!("twelvedata error for {symbol}: {quote}");
            continue;
        }
        let price = quote.get("close").and_then(parse_num);
        let change = quote.get("change").and_then(parse_num).unwrap_or(0.0);
        let percent_change = quote.get("percent_change").and_then(parse_num).unwrap_or(0.0);
        let volume = quote
            .get("volume")
            .and_then(parse_num)
            .map(|v| v as i64);

        let Some(price) = price else {
            tracing::warn!("no close price for {symbol}, skipping: {quote}");
            continue;
        };

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
    }

    tracing::info!("polled quotes at ts={ts}");
    Ok(())
}

async fn api_quotes(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut out = Vec::new();

    for (symbol, name) in TICKERS {
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
            name: name.to_string(),
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
    if !TICKERS.iter().any(|(s, _)| *s == symbol) {
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
