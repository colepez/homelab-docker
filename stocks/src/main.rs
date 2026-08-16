use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse, Json},
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::Value;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tokio_rusqlite::Connection;
use tokio_tungstenite::tungstenite::Message;

const FRONTEND_INDEX: &str = include_str!("frontend_index.html");
const FRONTEND_3X: &str = include_str!("frontend_3x.html");

// All 9 tickers stream live over Alpaca's free IEX WebSocket feed, which
// allows up to 30 symbol subscriptions on the free tier -- comfortably
// covers this list with no need to round-robin or ration requests.
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

const ALPACA_WS_URL: &str = "wss://stream.data.alpaca.markets/v2/iex";
const ALPACA_DATA_BASE: &str = "https://data.alpaca.markets/v2";
// The trade stream gives price ticks but not daily change, so previous
// close is fetched separately and refreshed on this interval to track the
// new trading day's baseline.
const PREV_CLOSE_REFRESH: Duration = Duration::from_secs(6 * 3600);

struct AppState {
    db: Connection,
    client: reqwest::Client,
    key_id: String,
    secret_key: String,
    prev_close: RwLock<HashMap<String, f64>>,
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

    let key_id = std::env::var("APCA_API_KEY_ID").expect("APCA_API_KEY_ID env var must be set");
    let secret_key =
        std::env::var("APCA_API_SECRET_KEY").expect("APCA_API_SECRET_KEY env var must be set");
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
        key_id,
        secret_key,
        prev_close: RwLock::new(HashMap::new()),
    });

    {
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = refresh_prev_closes(&state).await {
                    tracing::error!("refresh_prev_closes failed: {e:#}");
                }
                tokio::time::sleep(PREV_CLOSE_REFRESH).await;
            }
        });
    }

    {
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = run_websocket(&state).await {
                    tracing::error!("websocket session ended: {e:#}");
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
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

async fn refresh_prev_closes(state: &AppState) -> anyhow::Result<()> {
    let symbols = TICKERS.iter().map(|(s, _)| *s).collect::<Vec<_>>().join(",");
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

    // Collect everything we need from the response first, then update the
    // shared map and touch the DB without holding the write lock across
    // those awaits (which would otherwise block the WS handler's reads for
    // the whole loop).
    struct Seed<'a> {
        symbol: &'a str,
        close: f64,
        price: Option<f64>,
        volume: Option<i64>,
    }
    let seeds: Vec<Seed> = obj
        .iter()
        .filter_map(|(symbol, snapshot)| {
            let close = snapshot
                .get("prevDailyBar")
                .and_then(|b| b.get("c"))
                .and_then(|c| c.as_f64())?;
            let price = snapshot
                .get("latestTrade")
                .and_then(|t| t.get("p"))
                .and_then(|p| p.as_f64());
            let volume = snapshot
                .get("dailyBar")
                .and_then(|b| b.get("v"))
                .and_then(|v| v.as_i64());
            Some(Seed { symbol, close, price, volume })
        })
        .collect();

    {
        let mut map = state.prev_close.write().await;
        for seed in &seeds {
            map.insert(seed.symbol.to_string(), seed.close);
        }
    }

    let mut seeded = 0;
    for seed in &seeds {
        let already_tracked = state
            .db
            .call({
                let symbol = seed.symbol.to_string();
                move |conn| {
                    let exists: i64 = conn.query_row(
                        "SELECT COUNT(*) FROM quotes WHERE symbol = ?1",
                        rusqlite::params![symbol],
                        |r| r.get(0),
                    )?;
                    Ok::<_, tokio_rusqlite::Error>(exists > 0)
                }
            })
            .await
            .unwrap_or(false);
        if already_tracked {
            continue;
        }

        // Seed a quote row from the last trade so the dashboard has real
        // data immediately, rather than sitting empty until the next live
        // tick (which only arrives during market hours).
        let Some(price) = seed.price else { continue };
        let change = price - seed.close;
        let percent_change = if seed.close > 0.0 { (change / seed.close) * 100.0 } else { 0.0 };
        if let Err(e) =
            insert_quote_with_volume(state, seed.symbol, price, change, percent_change, seed.volume).await
        {
            tracing::warn!("failed to seed quote for {}: {e:#}", seed.symbol);
        } else {
            seeded += 1;
        }
    }

    if seeded > 0 {
        tracing::info!("seeded {seeded} initial quotes from last-trade snapshot");
    }
    tracing::info!("refreshed previous-close baseline for {} symbols", seeds.len());
    Ok(())
}

async fn insert_quote(
    state: &AppState,
    symbol: &str,
    price: f64,
    change: f64,
    percent_change: f64,
) -> anyhow::Result<()> {
    insert_quote_with_volume(state, symbol, price, change, percent_change, None).await
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

async fn run_websocket(state: &AppState) -> anyhow::Result<()> {
    // Make sure we have a previous-close baseline before ticks start
    // arriving, so the very first prints can compute a real change/%.
    if state.prev_close.read().await.is_empty() {
        if let Err(e) = refresh_prev_closes(state).await {
            tracing::warn!("initial prev-close fetch failed, will retry on schedule: {e:#}");
        }
    }

    let (ws_stream, _) = tokio_tungstenite::connect_async(ALPACA_WS_URL).await?;
    let (mut write, mut read) = ws_stream.split();

    let auth = serde_json::json!({
        "action": "auth",
        "key": state.key_id,
        "secret": state.secret_key,
    });
    write.send(Message::Text(auth.to_string())).await?;

    let symbols: Vec<&str> = TICKERS.iter().map(|(s, _)| *s).collect();
    let subscribe = serde_json::json!({
        "action": "subscribe",
        "trades": symbols,
    });
    write.send(Message::Text(subscribe.to_string())).await?;
    tracing::info!("alpaca websocket connected, subscribed to {} symbols", symbols.len());

    let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
    ping_interval.tick().await;

    loop {
        tokio::select! {
            msg = read.next() => {
                let Some(msg) = msg else {
                    anyhow::bail!("websocket stream closed");
                };
                match msg? {
                    Message::Text(text) => {
                        if let Err(e) = handle_ws_payload(state, &text).await {
                            tracing::warn!("failed to handle ws payload: {e:#}");
                        }
                    }
                    Message::Close(_) => anyhow::bail!("websocket closed by server"),
                    _ => {}
                }
            }
            _ = ping_interval.tick() => {
                write.send(Message::Ping(vec![])).await?;
            }
        }
    }
}

async fn handle_ws_payload(state: &AppState, text: &str) -> anyhow::Result<()> {
    let events: Vec<Value> = serde_json::from_str(text)?;
    for event in events {
        let msg_type = event.get("T").and_then(|t| t.as_str());
        match msg_type {
            Some("t") => {
                let Some(symbol) = event.get("S").and_then(|s| s.as_str()) else { continue };
                let Some(price) = event.get("p").and_then(|p| p.as_f64()) else { continue };

                let prev_close = state.prev_close.read().await.get(symbol).copied();
                let (change, percent_change) = match prev_close {
                    Some(prev) if prev > 0.0 => (price - prev, ((price - prev) / prev) * 100.0),
                    _ => (0.0, 0.0),
                };

                if let Err(e) = insert_quote(state, symbol, price, change, percent_change).await {
                    tracing::warn!("failed to record trade tick for {symbol}: {e:#}");
                }
            }
            Some("error") => {
                tracing::warn!("alpaca ws error event: {event}");
            }
            _ => {}
        }
    }
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
