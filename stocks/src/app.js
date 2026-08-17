// Shared across every /stocks page: formatters, sparkline drawing, and the
// market open/close clock in the topbar.

function fmtPct(v) {
  if (v === null || v === undefined) return "—";
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(2)}%`;
}
function fmtPrice(v) {
  return v.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}
function fmtVol(v) {
  if (v === null || v === undefined) return "—";
  if (v >= 1e9) return (v / 1e9).toFixed(2) + "B";
  if (v >= 1e6) return (v / 1e6).toFixed(2) + "M";
  if (v >= 1e3) return (v / 1e3).toFixed(1) + "K";
  return String(v);
}
function upDown(v) { return v > 0 ? "up" : v < 0 ? "down" : ""; }
function fmtAge(unixTs) {
  if (!unixTs) return "--";
  const secs = Math.max(0, Math.round(Date.now() / 1000 - unixTs));
  if (secs < 60) return "just now";
  const mins = Math.round(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  return `${Math.round(mins / 60)}h ago`;
}
function fmtAsOf(unixTs) {
  if (!unixTs) return "--";
  return new Date(unixTs * 1000).toLocaleString(undefined, {
    month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
  });
}
function leverageTag(ticker) {
  const mag = `${ticker.leverage}x`;
  return ticker.inverse ? `&minus;${mag}` : mag;
}

async function fetchCategories() {
  const res = await fetch("/stocks/api/categories");
  if (!res.ok) throw new Error("failed to load categories");
  return res.json();
}

function drawSpark(canvas, points, positive) {
  const ctx = canvas.getContext("2d");
  const w = canvas.width, h = canvas.height;
  ctx.clearRect(0, 0, w, h);
  if (!points || points.length < 2) return;
  const prices = points.map(p => p.price);
  const min = Math.min(...prices), max = Math.max(...prices);
  const range = max - min || 1;
  const stepX = w / (points.length - 1);
  const color = positive ? "#39d98a" : "#ff5c72";

  ctx.beginPath();
  points.forEach((p, i) => {
    const x = i * stepX;
    const y = h - ((p.price - min) / range) * (h - 6) - 3;
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  });
  const lastY = h - ((prices[prices.length - 1] - min) / range) * (h - 6) - 3;
  ctx.lineTo(w, lastY);
  ctx.lineTo(w, h);
  ctx.lineTo(0, h);
  ctx.closePath();
  ctx.fillStyle = color + "22";
  ctx.fill();

  ctx.beginPath();
  points.forEach((p, i) => {
    const x = i * stepX;
    const y = h - ((p.price - min) / range) * (h - 6) - 3;
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  });
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.5;
  ctx.stroke();

  const lastX = (points.length - 1) * stepX;
  ctx.beginPath();
  ctx.arc(lastX, lastY, 2.5, 0, Math.PI * 2);
  ctx.fillStyle = color;
  ctx.fill();
}

// Regular NYSE/Nasdaq hours only (9:30am-4pm ET, Mon-Fri) -- does not
// account for market holidays, so it can read "open" on e.g. Thanksgiving.
const ET_FMT = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York", hour12: false,
  hour: "2-digit", minute: "2-digit", second: "2-digit", weekday: "short",
});
const WEEKDAY_INDEX = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
const OPEN_MIN = 9 * 60 + 30;
const CLOSE_MIN = 16 * 60;

function fmtDuration(totalMinutes) {
  const h = Math.floor(totalMinutes / 60);
  const m = totalMinutes % 60;
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
}

function tickClock() {
  const el = document.getElementById("clock");
  if (el) el.textContent = new Date().toLocaleTimeString(undefined, { hour12: false });
}

function tickMarketStatus() {
  const pill = document.getElementById("market-pill");
  const text = document.getElementById("market-text");
  const countdown = document.getElementById("market-countdown");
  if (!pill || !text || !countdown) return;

  const parts = Object.fromEntries(ET_FMT.formatToParts(new Date()).map(p => [p.type, p.value]));
  const dayIdx = WEEKDAY_INDEX[parts.weekday];
  const mins = parseInt(parts.hour, 10) * 60 + parseInt(parts.minute, 10);
  const secs = parseInt(parts.second, 10);
  const isWeekday = dayIdx >= 1 && dayIdx <= 5;
  const isOpen = isWeekday && mins >= OPEN_MIN && mins < CLOSE_MIN;

  if (isOpen) {
    pill.classList.remove("closed");
    text.textContent = "market open";
    const minsLeft = CLOSE_MIN - mins - (secs > 0 ? 1 : 0);
    countdown.textContent = `closes in ${fmtDuration(Math.max(minsLeft, 0))}`;
  } else {
    pill.classList.add("closed");
    text.textContent = "market closed";
    let daysAhead;
    if (isWeekday && mins < OPEN_MIN) daysAhead = 0;
    else if (dayIdx === 5) daysAhead = 3;
    else if (dayIdx === 6) daysAhead = 2;
    else if (dayIdx === 0) daysAhead = 1;
    else daysAhead = 1;
    const minsUntil = daysAhead * 1440 + OPEN_MIN - mins - (secs > 0 ? 1 : 0);
    countdown.textContent = `opens in ${fmtDuration(Math.max(minsUntil, 0))}`;
  }
}

function initTopbar() {
  tickClock();
  tickMarketStatus();
  setInterval(tickClock, 1000);
  setInterval(tickMarketStatus, 1000);
}

document.addEventListener("DOMContentLoaded", initTopbar);
