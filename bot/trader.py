"""
TastyTrade Multi-Strategy Auto-Trader v1.3
==========================================
New in v1.3:
  - StatusWriter: writes status.json every 3s with full bot telemetry
  - StatusServer: lightweight HTTP server on port 8080 serving status.json
    with CORS headers so the dashboard can read it from any origin
  - Signal log: rolling 100-entry feed of every scan result
  - Last-tick tracking per symbol with timestamp
  - Uptime, connection state, equity, mode all exposed

Access status at: http://YOUR_VPS_IP:8080/status.json
Dashboard reads it automatically on the Status tab.
"""

import asyncio
import json
import logging
import os
import signal
import time
from collections import defaultdict, deque
from datetime import date, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import Optional
from zoneinfo import ZoneInfo

import requests
import websockets
import pandas as pd

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("trader.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)

# ── Paths / constants ──────────────────────────────────────────────────────────
ROOT          = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SETTINGS_PATH = os.path.join(ROOT, "config", "settings.json")
STATE_PATH    = os.path.join(ROOT, "config", "state.json")
STATUS_PATH   = os.path.join(ROOT, "config", "status.json")

TT_BASE_LIVE  = "https://api.tastytrade.com"
TT_BASE_PAPER = "https://api.cert.tastyworks.com"

FILL_POLL_INTERVAL = 2
FILL_POLL_TIMEOUT  = 30
STATUS_PORT        = 8080
SIGNAL_LOG_MAX     = 100


# ══════════════════════════════════════════════════════════════════════════════
# Status Writer + HTTP Server
# ══════════════════════════════════════════════════════════════════════════════

class StatusTracker:
    """
    Collects live telemetry from the engine and writes status.json every 3s.
    Also serves it over HTTP so the dashboard can poll it.
    """

    def __init__(self):
        self.start_time   = time.time()
        self.connected    = False
        self.mode         = "PAPER"
        self.algo_enabled = False
        self.account      = ""
        self.equity       = 0.0

        # {symbol: {price, ts}}
        self.last_ticks: dict[str, dict] = {}

        # Rolling signal log
        self.signal_log: deque = deque(maxlen=SIGNAL_LOG_MAX)

        # Today's fills list
        self.fills_today: list[dict] = []
        self._fills_date = str(date.today())

        # Open positions snapshot (written by engine)
        self.open_positions: list[dict] = []

        # Strategy counters
        self.s15_trades_today = 0
        self.s15_daily_limit  = 2
        self.sd_trades_today  = 0
        self.sd_daily_limit   = 1
        self.s15_enabled      = True
        self.sd_enabled       = False

    # ── Called by engine ──────────────────────────────────────────────────────

    def record_tick(self, symbol: str, price: float, ts: float):
        self.last_ticks[symbol] = {"price": round(price, 4), "ts": ts}

    def record_signal(self, symbol: str, strategy: str, signal: str,
                      price: float, ema_val: float, dist_pct: float, fired: bool):
        self.signal_log.appendleft({
            "ts":       datetime.now().strftime("%H:%M:%S"),
            "symbol":   symbol,
            "strategy": strategy,
            "signal":   signal,
            "price":    round(price, 4),
            "ema":      round(ema_val, 4),
            "dist_pct": round(dist_pct, 4),
            "fired":    fired,
        })

    def record_fill(self, symbol: str, strategy: str, signal: str,
                    intended: float, filled: float, shares: int,
                    stop: float, tp_tiers: list):
        today = str(date.today())
        if self._fills_date != today:
            self.fills_today = []
            self._fills_date = today
        self.fills_today.append({
            "ts":        datetime.now().strftime("%H:%M:%S"),
            "symbol":    symbol,
            "strategy":  strategy,
            "signal":    signal,
            "intended":  round(intended, 4),
            "filled":    round(filled, 4),
            "slippage":  round(filled - intended, 4),
            "shares":    shares,
            "stop":      round(stop, 4),
            "tp_tiers":  tp_tiers,
        })

    def update_positions(self, s15_pos: dict, sd_pos: dict, live_prices: dict):
        positions = []
        for sym, p in s15_pos.items():
            curr = live_prices.get(sym, {}).get("price", p["entry_price"])
            pnl  = round((curr - p["entry_price"]) * p["shares"], 2)
            pnl_pct = round((curr - p["entry_price"]) / p["entry_price"] * 100, 2)
            positions.append({
                "symbol":      sym,
                "strategy":    "15-Min EMA Cloud",
                "entry_price": p["entry_price"],
                "current":     curr,
                "stop":        p["stop_price"],
                "shares":      p["shares"],
                "pnl":         pnl,
                "pnl_pct":     pnl_pct,
                "entry_time":  p.get("entry_time", ""),
                "signal":      p.get("signal", ""),
            })
        for sym, p in sd_pos.items():
            curr = live_prices.get(sym, {}).get("price", p["entry_price"])
            pnl  = round((curr - p["entry_price"]) * p["shares"], 2)
            pnl_pct = round((curr - p["entry_price"]) / p["entry_price"] * 100, 2)
            positions.append({
                "symbol":      sym,
                "strategy":    "Daily EMA Pullback",
                "entry_price": p["entry_price"],
                "current":     curr,
                "stop":        p["stop_price"],
                "shares":      p["shares"],
                "pnl":         pnl,
                "pnl_pct":     pnl_pct,
                "entry_time":  p.get("entry_time", ""),
                "signal":      p.get("signal", ""),
            })
        self.open_positions = positions

    # ── Serialise ─────────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        now = time.time()
        uptime_s = int(now - self.start_time)
        h, rem   = divmod(uptime_s, 3600)
        m, s     = divmod(rem, 60)

        # Mark symbols stale if no tick in 60s
        ticks = {}
        for sym, t in self.last_ticks.items():
            age = round(now - t["ts"], 1)
            ticks[sym] = {
                "price": t["price"],
                "age_s": age,
                "stale": age > 60,
            }

        return {
            "generated_at":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "uptime":          f"{h:02d}:{m:02d}:{s:02d}",
            "connected":       self.connected,
            "mode":            self.mode,
            "algo_enabled":    self.algo_enabled,
            "account":         self.account,
            "equity":          self.equity,
            "strategies": {
                "s15": {
                    "enabled":      self.s15_enabled,
                    "trades_today": self.s15_trades_today,
                    "daily_limit":  self.s15_daily_limit,
                },
                "sd": {
                    "enabled":      self.sd_enabled,
                    "trades_today": self.sd_trades_today,
                    "daily_limit":  self.sd_daily_limit,
                },
            },
            "last_ticks":      ticks,
            "open_positions":  self.open_positions,
            "fills_today":     self.fills_today,
            "signal_log":      list(self.signal_log),
        }

    def write(self):
        try:
            with open(STATUS_PATH, "w") as f:
                json.dump(self.to_dict(), f)
        except Exception as e:
            log.warning("Could not write status.json: %s", e)


# ── HTTP Server (runs in background thread) ────────────────────────────────────

class _StatusHandler(BaseHTTPRequestHandler):
    tracker:        "StatusTracker" = None
    dashboard_secret: str           = ""   # injected before server starts

    def _send_json(self, code: int, payload: dict):
        data = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type",                "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers","Content-Type, X-Dashboard-Secret")
        self.send_header("Access-Control-Allow-Methods","GET, POST, OPTIONS")
        self.send_header("Content-Length",              str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self):
        # CORS preflight — required for browser fetch from dashboard
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Dashboard-Secret")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()

    def do_GET(self):
        if self.path in ("/status.json", "/"):
            self._send_json(200, self.tracker.to_dict())
        elif self.path == "/settings.json":
            try:
                with open(SETTINGS_PATH) as f:
                    settings = json.load(f)
                self._send_json(200, settings)
            except Exception as e:
                self._send_json(500, {"error": str(e)})
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/settings":
            self._send_json(404, {"error": "not found"})
            return

        # Auth check — require X-Dashboard-Secret header if secret is configured
        secret = self.dashboard_secret
        if secret:
            provided = self.headers.get("X-Dashboard-Secret", "")
            if provided != secret:
                log.warning("⚠️  Settings update rejected — invalid secret from %s",
                            self.client_address[0])
                self._send_json(403, {"error": "invalid secret"})
                return

        # Read body
        length = int(self.headers.get("Content-Length", 0))
        body   = self.rfile.read(length)
        try:
            new_settings = json.loads(body)
        except json.JSONDecodeError as e:
            self._send_json(400, {"error": f"invalid JSON: {e}"})
            return

        # Write to settings.json
        try:
            with open(SETTINGS_PATH, "w") as f:
                json.dump(new_settings, f, indent=2)
            log.info("⚙️  Settings updated via dashboard.")
            self._send_json(200, {"ok": True})
        except Exception as e:
            log.error("❌ Failed to write settings: %s", e)
            self._send_json(500, {"error": str(e)})

    def log_message(self, *args):
        pass  # suppress access log spam


def start_status_server(tracker: StatusTracker):
    secret = os.environ.get("DASHBOARD_SECRET", "")
    _StatusHandler.tracker          = tracker
    _StatusHandler.dashboard_secret = secret
    server = HTTPServer(("0.0.0.0", STATUS_PORT), _StatusHandler)
    t = Thread(target=server.serve_forever, daemon=True)
    t.start()
    log.info("🌐 Status server on http://0.0.0.0:%d  (secret: %s)",
             STATUS_PORT, "set ✅" if secret else "not set ⚠️")
    return server


# ══════════════════════════════════════════════════════════════════════════════
# Config & State
# ══════════════════════════════════════════════════════════════════════════════

def load_settings() -> dict:
    with open(SETTINGS_PATH) as f:
        return json.load(f)


def load_state() -> dict:
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH) as f:
            return json.load(f)
    return {
        "trade_date":       str(date.today()),
        "s15_trades_today": 0,
        "sd_trades_today":  0,
        "s15_positions":    {},
        "sd_positions":     {},
    }


def save_state(state: dict):
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)


# ══════════════════════════════════════════════════════════════════════════════
# TastyTrade REST Client
# ══════════════════════════════════════════════════════════════════════════════

class TastyTradeClient:
    def __init__(self, client_secret: str, refresh_token: str, paper: bool = False):
        self.base          = TT_BASE_PAPER if paper else TT_BASE_LIVE
        self.oauth_base    = "https://api.cert.tastyworks.com" if paper else "https://api.tastytrade.com"
        self.paper         = paper
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.access_token: Optional[str] = None
        self.token_expiry: float         = 0
        self.account_number: Optional[str] = None
        self._authenticate()

    def _authenticate(self):
        """
        Exchange refresh_token + client_secret for a short-lived access token.
        TastyTrade OAuth access tokens expire every 15 minutes.
        Refresh tokens never expire — we re-authenticate automatically before expiry.
        """
        log.info("🔑 Authenticating via OAuth...")
        resp = requests.post(
            f"{self.oauth_base}/oauth/token",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent":   "tastytrade-algo/1.3",
            },
            data={
                "grant_type":    "refresh_token",
                "refresh_token": self.refresh_token,
                "client_secret": self.client_secret,
            },
            timeout=15,
        )
        if not resp.ok:
            log.error("❌ OAuth failed (%s): %s", resp.status_code, resp.text)
            resp.raise_for_status()
        data = resp.json()
        self.access_token = data["access_token"]
        # Refresh 60s before actual expiry to be safe
        self.token_expiry = time.time() + data.get("expires_in", 900) - 60
        log.info("✅ TastyTrade OAuth authenticated (%s)", "PAPER" if self.paper else "LIVE")

    def _ensure_token(self):
        """Re-authenticate if the access token is about to expire."""
        if time.time() >= self.token_expiry:
            log.info("🔄 Access token expiring — refreshing...")
            self._authenticate()

    def _h(self):
        self._ensure_token()
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type":  "application/json",
            "User-Agent":    "tastytrade-algo/1.3",
        }

    def get_account(self) -> str:
        resp = requests.get(f"{self.base}/customers/me/accounts", headers=self._h(), timeout=10)
        resp.raise_for_status()
        self.account_number = resp.json()["data"]["items"][0]["account"]["account-number"]
        return self.account_number

    def get_equity(self) -> float:
        resp = requests.get(
            f"{self.base}/accounts/{self.account_number}/balances",
            headers=self._h(), timeout=10)
        resp.raise_for_status()
        return float(resp.json()["data"]["net-liquidating-value"])

    def get_positions(self) -> list:
        resp = requests.get(
            f"{self.base}/accounts/{self.account_number}/positions",
            headers=self._h(), timeout=10)
        resp.raise_for_status()
        return resp.json()["data"]["items"]

    def get_streamer_token(self) -> tuple:
        # Correct endpoint is /api-quote-tokens, returns dxlink-url and token
        resp = requests.get(f"{self.base}/api-quote-tokens", headers=self._h(), timeout=10)
        resp.raise_for_status()
        d = resp.json()["data"]
        # Field is dxlink-url not websocket-url
        ws_url = d.get("dxlink-url") or d.get("websocket-url")
        return ws_url, d["token"]

    def place_market_buy(self, symbol: str, quantity: int) -> str:
        order = {
            "time-in-force": "Day", "order-type": "Market",
            "legs": [{"instrument-type": "Equity", "symbol": symbol,
                      "quantity": quantity, "action": "Buy to Open"}],
        }
        resp = requests.post(
            f"{self.base}/accounts/{self.account_number}/orders",
            headers=self._h(), json=order, timeout=10)
        resp.raise_for_status()
        return str(resp.json()["data"]["order"]["id"])

    def get_order(self, order_id: str) -> dict:
        resp = requests.get(
            f"{self.base}/accounts/{self.account_number}/orders/{order_id}",
            headers=self._h(), timeout=10)
        resp.raise_for_status()
        return resp.json()["data"]

    def poll_fill(self, order_id: str) -> Optional[dict]:
        deadline = time.time() + FILL_POLL_TIMEOUT
        while time.time() < deadline:
            try:
                order = self.get_order(order_id)
                status = order.get("status", "")
                if status == "Filled":
                    legs = order.get("legs", [{}])
                    fills = legs[0].get("fills", []) if legs else []
                    if fills:
                        total_qty  = sum(float(f.get("fill-quantity", 0)) for f in fills)
                        total_cost = sum(float(f.get("fill-quantity", 0)) * float(f.get("fill-price", 0)) for f in fills)
                        avg_price  = total_cost / total_qty if total_qty else 0
                        return {"filled_quantity": int(total_qty), "average_fill_price": round(avg_price, 4)}
                    return {
                        "filled_quantity":    int(order.get("filled-quantity", 0)),
                        "average_fill_price": float(order.get("average-fill-price", 0)),
                    }
                if status in ("Cancelled", "Rejected", "Expired"):
                    log.warning("  ⚠️  Order %s: %s", order_id, status)
                    return None
            except Exception as e:
                log.warning("  ⚠️  Poll error for %s: %s", order_id, e)
            time.sleep(FILL_POLL_INTERVAL)
        log.warning("  ⚠️  Fill timeout for order %s", order_id)
        return None

    def place_stop(self, symbol: str, quantity: int, stop_price: float) -> str:
        order = {
            "time-in-force": "GTC", "order-type": "Stop",
            "stop-trigger": str(round(stop_price, 2)),
            "legs": [{"instrument-type": "Equity", "symbol": symbol,
                      "quantity": quantity, "action": "Sell to Close"}],
        }
        resp = requests.post(
            f"{self.base}/accounts/{self.account_number}/orders",
            headers=self._h(), json=order, timeout=10)
        resp.raise_for_status()
        return str(resp.json()["data"]["order"]["id"])

    def place_limit(self, symbol: str, quantity: int, limit_price: float) -> str:
        order = {
            "time-in-force": "GTC", "order-type": "Limit",
            "price": str(round(limit_price, 2)), "price-effect": "Debit",
            "legs": [{"instrument-type": "Equity", "symbol": symbol,
                      "quantity": quantity, "action": "Sell to Close"}],
        }
        resp = requests.post(
            f"{self.base}/accounts/{self.account_number}/orders",
            headers=self._h(), json=order, timeout=10)
        resp.raise_for_status()
        return str(resp.json()["data"]["order"]["id"])

    def get_daily_candles(self, symbol: str, count: int = 300) -> pd.DataFrame:
        resp = requests.get(
            f"{self.base}/market-data/candles/{symbol}",
            headers=self._h(),
            params={"timeframe": "1d", "count": count},
            timeout=15)
        resp.raise_for_status()
        df = pd.DataFrame(resp.json()["data"]["items"])
        df["close"] = df["close"].astype(float)
        df["time"]  = pd.to_datetime(df["time"])
        return df.sort_values("time").reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# Candle Builder
# ══════════════════════════════════════════════════════════════════════════════

class CandleBuilder:
    def __init__(self, period_seconds: int = 900):
        self.period   = period_seconds
        self._current: dict = {}
        self.history:  dict = defaultdict(list)

    def push(self, symbol, price, size, ts) -> Optional[dict]:
        b = ts - (ts % self.period)
        completed = None
        if symbol not in self._current:
            self._current[symbol] = {"ts": b, "open": price, "high": price,
                                     "low": price, "close": price, "volume": size}
        else:
            c = self._current[symbol]
            if b > c["ts"]:
                completed = dict(c)
                self.history[symbol].append(completed)
                if len(self.history[symbol]) > 300:
                    self.history[symbol] = self.history[symbol][-300:]
                self._current[symbol] = {"ts": b, "open": price, "high": price,
                                         "low": price, "close": price, "volume": size}
            else:
                c["high"]   = max(c["high"], price)
                c["low"]    = min(c["low"],  price)
                c["close"]  = price
                c["volume"] += size
        return completed

    def get_dataframe(self, symbol) -> pd.DataFrame:
        rows = self.history.get(symbol, [])
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df["close"] = df["close"].astype(float)
        return df.sort_values("ts").reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# Signal Logic
# ══════════════════════════════════════════════════════════════════════════════

def _ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()


def check_signal_15min(df, price, cfg, status: StatusTracker, symbol) -> Optional[str]:
    fast = cfg["ema_fast"]
    slow = cfg["ema_slow"]
    if len(df) < max(fast, slow) + 2:
        return None
    df = df.copy()
    df["ema_f"] = _ema(df["close"], fast)
    df["ema_s"] = _ema(df["close"], slow)
    last = df.iloc[-1]
    prev = df.iloc[-2]
    cloud_top      = max(last["ema_f"], last["ema_s"])
    prev_cloud_top = max(prev["ema_f"], prev["ema_s"])
    pullback_pct   = cfg["entry"]["pullback_to_cloud_pct"] / 100
    breakout_pct   = cfg["entry"]["breakout_above_cloud_pct"] / 100

    if price > cloud_top:
        dist = (price - last["ema_f"]) / last["ema_f"]
        fired = dist <= pullback_pct
        status.record_signal(symbol, "S1-15m", "pullback", price,
                             last["ema_f"], dist * 100, fired)
        if fired:
            return "pullback"

    if last["close"] <= prev_cloud_top and price >= last["ema_f"] * (1 + breakout_pct):
        if price > cloud_top:
            dist = (price - last["ema_f"]) / last["ema_f"]
            status.record_signal(symbol, "S1-15m", "breakout", price,
                                 last["ema_f"], dist * 100, True)
            return "breakout"

    return None


def check_signal_daily(daily_df, price, cfg, status: StatusTracker, symbol) -> Optional[str]:
    period = cfg["ema_period"]
    if len(daily_df) < period + 2:
        return None
    df = daily_df.copy()
    df["ema"] = _ema(df["close"], period)
    ema_val = df["ema"].iloc[-1]
    if price <= ema_val:
        return None
    dist_pct  = (price - ema_val) / ema_val * 100
    threshold = cfg["entry"]["pullback_to_ema_pct"]
    fired     = dist_pct <= threshold
    status.record_signal(symbol, "S2-Daily", "pullback", price,
                         ema_val, dist_pct, fired)
    return "daily_pullback" if fired else None


def calc_position_size(equity, entry, stop, risk_pct, max_pos_pct) -> int:
    risk_dollars   = equity * (risk_pct / 100)
    risk_per_share = entry - stop
    if risk_per_share <= 0:
        return 0
    shares     = int(risk_dollars / risk_per_share)
    max_shares = int((equity * (max_pos_pct / 100)) / entry)
    return max(0, min(shares, max_shares))


# ══════════════════════════════════════════════════════════════════════════════
# Order Executor
# ══════════════════════════════════════════════════════════════════════════════

async def execute_entry(client, symbol, intended_price, signal,
                        strategy_cfg, state, position_key, trades_key,
                        tz, status: StatusTracker) -> bool:
    try:
        equity   = client.get_equity()
        risk_cfg = strategy_cfg["risk"]
        stop_pct = risk_cfg["stop_loss_pct"] / 100
        stop_est = round(intended_price * (1 - stop_pct), 2)
        shares   = calc_position_size(equity, intended_price, stop_est,
                                      risk_cfg["risk_per_trade_pct"],
                                      risk_cfg["max_position_size_pct"])
        if shares <= 0:
            log.warning("  ⚠️  [%s] Position size 0 — skipping.", symbol)
            return False

        buy_id = await asyncio.to_thread(client.place_market_buy, symbol, shares)
        log.info("  📤 BUY submitted (id=%s) — awaiting fill...", buy_id)

        fill = await asyncio.to_thread(client.poll_fill, buy_id)
        if not fill or fill["filled_quantity"] <= 0:
            log.error("  ❌ [%s] Fill not confirmed — stop/TP NOT placed.", symbol)
            return False

        filled_qty  = fill["filled_quantity"]
        fill_price  = fill["average_fill_price"] or intended_price
        actual_stop = round(fill_price * (1 - stop_pct), 2)
        slippage    = fill_price - intended_price
        log.info("  ✅ FILLED %d @ $%.4f (slippage: %+.4f)  stop=$%.2f",
                 filled_qty, fill_price, slippage, actual_stop)

        sl_id = await asyncio.to_thread(client.place_stop, symbol, filled_qty, actual_stop)
        log.info("  🛑 Stop-loss @ $%.2f (id=%s)", actual_stop, sl_id)

        initial_risk = fill_price * stop_pct
        tp_placed = []
        for tier in strategy_cfg["take_profit_tiers"]:
            if not tier["enabled"]:
                continue
            tp_price  = round(fill_price + initial_risk * tier["r_multiple"], 2)
            tp_shares = max(1, int(filled_qty * (tier["sell_pct"] / 100)))
            tp_id = await asyncio.to_thread(client.place_limit, symbol, tp_shares, tp_price)
            log.info("  🎯 TP %sR @ $%.2f (%d shares, id=%s)",
                     tier["r_multiple"], tp_price, tp_shares, tp_id)
            tp_placed.append({"r": tier["r_multiple"], "price": tp_price,
                              "shares": tp_shares, "order_id": tp_id})

        state[position_key][symbol] = {
            "entry_price":   fill_price,
            "stop_price":    actual_stop,
            "shares":        filled_qty,
            "signal":        signal,
            "entry_time":    datetime.now(tz).strftime("%H:%M:%S"),
            "buy_order_id":  buy_id,
            "stop_order_id": sl_id,
            "tp_tiers":      tp_placed,
        }
        state[trades_key] = state.get(trades_key, 0) + 1
        save_state(state)

        strat_label = "15-Min EMA Cloud" if "s15" in position_key else "Daily EMA Pullback"
        status.record_fill(symbol, strat_label, signal,
                           intended_price, fill_price, filled_qty,
                           actual_stop, tp_placed)

        log.info("  📋 %s trades today: %d", trades_key, state[trades_key])
        return True

    except Exception as e:
        log.error("  ❌ Entry failed for %s: %s", symbol, e, exc_info=True)
        return False


# ══════════════════════════════════════════════════════════════════════════════
# WebSocket Stream
# ══════════════════════════════════════════════════════════════════════════════

class StreamHandler:
    HEARTBEAT_INTERVAL = 20

    def __init__(self, ws_url, token, symbols, on_tick, status: StatusTracker):
        self.ws_url   = ws_url
        self.token    = token
        self.symbols  = symbols
        self.on_tick  = on_tick
        self.status   = status
        self._running = False

    async def run(self):
        self._running = True
        while self._running:
            self.status.connected = False
            try:
                await self._connect()
            except Exception as e:
                log.warning("🔌 Stream disconnected: %s — reconnecting in 5s", e)
                await asyncio.sleep(5)

    async def _connect(self):
        log.info("🔌 Connecting to DXFeed for %d symbols...", len(self.symbols))
        async with websockets.connect(
            self.ws_url,
            extra_headers={"Authorization": self.token},
            ping_interval=None,
        ) as ws:
            await ws.send(json.dumps([{
                "advice": {"timeout": 60000, "interval": 0},
                "channel": "/meta/handshake",
                "supportedConnectionTypes": ["websocket"],
                "version": "1.0",
            }]))
            await ws.send(json.dumps([{
                "channel": "/service/sub",
                "data": {"add": {"Quote": self.symbols}},
            }]))
            self.status.connected = True
            log.info("📡 Stream live: %s", ", ".join(self.symbols))
            hb = asyncio.create_task(self._heartbeat(ws))
            try:
                async for raw in ws:
                    try:
                        msgs = json.loads(raw)
                        if not isinstance(msgs, list):
                            msgs = [msgs]
                        for msg in msgs:
                            self._dispatch(msg)
                    except json.JSONDecodeError:
                        pass
            finally:
                hb.cancel()
                self.status.connected = False

    async def _heartbeat(self, ws):
        while True:
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)
            try:
                await ws.send(json.dumps([{"channel": "/meta/connect",
                                            "connectionType": "websocket"}]))
            except Exception:
                break

    def _dispatch(self, msg: dict):
        if msg.get("channel") != "/service/data":
            return
        feed = msg.get("data", {}).get("Quote", {})
        ts   = time.time()
        for symbol, quote in feed.items():
            try:
                bid = float(quote.get("bidPrice", 0) or 0)
                ask = float(quote.get("askPrice", 0) or 0)
                if bid > 0 and ask > 0:
                    price = (bid + ask) / 2
                elif bid > 0:
                    price = bid
                elif ask > 0:
                    price = ask
                else:
                    continue
                size = float(quote.get("bidSize", 0) or 0)
                self.status.record_tick(symbol, price, ts)
                self.on_tick(symbol, price, size, ts)
            except (TypeError, ValueError):
                continue

    def stop(self):
        self._running = False


# ══════════════════════════════════════════════════════════════════════════════
# Main Algo Engine
# ══════════════════════════════════════════════════════════════════════════════

class AlgoEngine:
    def __init__(self):
        self.settings = load_settings()
        self.state    = load_state()
        self.status   = StatusTracker()
        self.client:  Optional[TastyTradeClient] = None
        self.tz       = ZoneInfo(self.settings["execution"]["timezone"])
        tf = self.settings["strategy_15min"]["timeframe_minutes"]
        self.candles_15m = CandleBuilder(period_seconds=tf * 60)
        self.daily_candles: dict[str, pd.DataFrame] = {}
        self._daily_candles_date: Optional[str] = None
        self._last_signal = {"s15": defaultdict(float), "sd": defaultdict(float)}
        self._signal_cooldown = 60
        self._lock = asyncio.Lock()

    def reload_settings(self):
        prev_paper   = self.settings["execution"].get("paper_trading", True)
        self.settings = load_settings()
        s15 = self.settings["strategy_15min"]
        sd  = self.settings["strategy_daily"]
        ex  = self.settings["execution"]
        self.status.s15_enabled     = s15.get("enabled", True)
        self.status.sd_enabled      = sd.get("enabled", False)
        self.status.s15_daily_limit = s15.get("daily_trade_limit", 2)
        self.status.sd_daily_limit  = sd.get("daily_trade_limit", 1)
        self.status.algo_enabled    = ex.get("algo_enabled", False)

        # If paper_trading flag changed, reinitialize the TastyTrade client
        # so it connects to the correct environment (cert vs live)
        new_paper = ex.get("paper_trading", True)
        if self.client is not None and new_paper != prev_paper:
            log.info("🔄 paper_trading changed to %s — reinitialising client...",
                     "PAPER" if new_paper else "LIVE")
            try:
                if new_paper:
                    client_secret = os.environ.get("TASTYTRADE_CLIENT_SECRET_PAPER") \
                                 or os.environ.get("TASTYTRADE_CLIENT_SECRET")
                    refresh_token = os.environ.get("TASTYTRADE_REFRESH_TOKEN_PAPER") \
                                 or os.environ.get("TASTYTRADE_REFRESH_TOKEN")
                else:
                    client_secret = os.environ.get("TASTYTRADE_CLIENT_SECRET_LIVE") \
                                 or os.environ.get("TASTYTRADE_CLIENT_SECRET")
                    refresh_token = os.environ.get("TASTYTRADE_REFRESH_TOKEN_LIVE") \
                                 or os.environ.get("TASTYTRADE_REFRESH_TOKEN")

                if not client_secret or not refresh_token:
                    log.error("❌ Missing credentials for %s mode — toggle reverted.",
                              "PAPER" if new_paper else "LIVE")
                    return

                self.client = TastyTradeClient(client_secret, refresh_token, paper=new_paper)
                self.client.get_account()
                self.status.mode    = "PAPER" if new_paper else "LIVE"
                self.status.account = self.client.account_number
                self.status.equity  = self.client.get_equity()
                log.info("✅ Client reinitialised — mode: %s | account: %s | equity: $%.2f",
                         self.status.mode, self.status.account, self.status.equity)
            except Exception as e:
                log.error("❌ Client reinitialisation failed: %s", e, exc_info=True)

    def _is_market_hours(self) -> bool:
        now = datetime.now(self.tz)
        if now.weekday() >= 5:
            return False
        cfg = self.settings["execution"]
        open_t = datetime.strptime(cfg["market_open"], "%H:%M").replace(
            tzinfo=self.tz, year=now.year, month=now.month, day=now.day)
        close_t = datetime.strptime(cfg["market_close"], "%H:%M").replace(
            tzinfo=self.tz, year=now.year, month=now.month, day=now.day)
        return open_t <= now <= close_t

    def _reset_daily_if_needed(self):
        today = str(date.today())
        if self.state.get("trade_date") != today:
            self.state.update({"trade_date": today, "s15_trades_today": 0, "sd_trades_today": 0})
            save_state(self.state)
            log.info("🗓️  New trading day — counters reset.")

    def reconcile_positions(self):
        log.info("🔍 Reconciling positions...")
        try:
            live  = self.client.get_positions()
            syms  = {p["symbol"] for p in live if float(p.get("quantity", 0)) > 0}
            for key in ("s15_positions", "sd_positions"):
                stale = [s for s in self.state.get(key, {}) if s not in syms]
                for s in stale:
                    log.warning("  ⚠️  %s: %s closed while offline — removing.", key, s)
                    del self.state[key][s]
            untracked = syms - set(self.state.get("s15_positions", {})) \
                             - set(self.state.get("sd_positions",  {}))
            if untracked:
                log.info("  ℹ️  Manually-opened positions (not tracked): %s",
                         ", ".join(sorted(untracked)))
            save_state(self.state)
            log.info("✅ Reconciliation done.")
        except Exception as e:
            log.error("❌ Reconciliation failed: %s", e, exc_info=True)

    def _load_daily_candles(self):
        today = str(date.today())
        if self._daily_candles_date == today:
            return
        cfg = self.settings["strategy_daily"]
        log.info("📅 Loading daily candles for %d symbols...", len(cfg["watchlist"]))
        for sym in cfg["watchlist"]:
            try:
                self.daily_candles[sym] = self.client.get_daily_candles(sym, count=300)
            except Exception as e:
                log.warning("  ⚠️  Daily candles failed for %s: %s", sym, e)
        self._daily_candles_date = today
        log.info("✅ Daily candles loaded.")

    def on_tick(self, symbol: str, price: float, size: float, ts: float):
        self.candles_15m.push(symbol, price, size, ts)
        asyncio.create_task(self._evaluate(symbol, price, ts))

    async def _evaluate(self, symbol: str, price: float, ts: float):
        async with self._lock:
            self._reset_daily_if_needed()

            # Always keep status fresh — positions, equity, trade counters
            self.status.s15_trades_today = self.state.get("s15_trades_today", 0)
            self.status.sd_trades_today  = self.state.get("sd_trades_today",  0)
            self.status.update_positions(
                self.state.get("s15_positions", {}),
                self.state.get("sd_positions",  {}),
                self.status.last_ticks,
            )

            # Always evaluate signals so they appear in the signal log
            # even when algo is off — only skip actual order execution
            algo_on = self.settings["execution"]["algo_enabled"]
            in_market = self._is_market_hours()

            s15 = self.settings["strategy_15min"]
            sd  = self.settings["strategy_daily"]

            # Strategy 1 — 15-min EMA Cloud
            if (s15.get("enabled") and symbol in s15["watchlist"] and
                    ts - self._last_signal["s15"][symbol] > self._signal_cooldown):

                df = self.candles_15m.get_dataframe(symbol)
                if not df.empty:
                    sig = check_signal_15min(df, price, s15, self.status, symbol)
                    if sig:
                        self._last_signal["s15"][symbol] = ts
                        if algo_on and in_market:
                            if (symbol not in self.state.get("s15_positions", {}) and
                                    self.state.get("s15_trades_today", 0) < s15["daily_trade_limit"]):
                                log.info("🎯 [S1-15M] %s signal — %s @ $%.2f", sig.upper(), symbol, price)
                                await execute_entry(
                                    self.client, symbol, price, sig, s15, self.state,
                                    "s15_positions", "s15_trades_today", self.tz, self.status)
                        else:
                            log.info("👁️  [S1-15M] %s signal detected — %s @ $%.2f (algo %s)",
                                     sig.upper(), symbol, price,
                                     "off" if not algo_on else "outside market hours")

            # Strategy 2 — Daily EMA Pullback
            if (sd.get("enabled") and symbol in sd["watchlist"] and
                    ts - self._last_signal["sd"][symbol] > self._signal_cooldown):

                daily_df = self.daily_candles.get(symbol)
                if daily_df is not None and not daily_df.empty:
                    sig = check_signal_daily(daily_df, price, sd, self.status, symbol)
                    if sig:
                        self._last_signal["sd"][symbol] = ts
                        if algo_on and in_market:
                            if (symbol not in self.state.get("sd_positions", {}) and
                                    self.state.get("sd_trades_today", 0) < sd["daily_trade_limit"]):
                                log.info("🎯 [S2-DAILY] %s signal — %s @ $%.2f", sig.upper(), symbol, price)
                                await execute_entry(
                                    self.client, symbol, price, sig, sd, self.state,
                                    "sd_positions", "sd_trades_today", self.tz, self.status)
                        else:
                            log.info("👁️  [S2-DAILY] %s signal detected — %s @ $%.2f (algo %s)",
                                     sig.upper(), symbol, price,
                                     "off" if not algo_on else "outside market hours")

    async def run(self):
        log.info("═" * 60)
        log.info("  TastyTrade Multi-Strategy Trader  v1.4")
        log.info("═" * 60)

        paper = self.settings["execution"]["paper_trading"]

        if paper:
            client_secret = os.environ.get("TASTYTRADE_CLIENT_SECRET_PAPER") \
                         or os.environ.get("TASTYTRADE_CLIENT_SECRET")
            refresh_token = os.environ.get("TASTYTRADE_REFRESH_TOKEN_PAPER") \
                         or os.environ.get("TASTYTRADE_REFRESH_TOKEN")
        else:
            client_secret = os.environ.get("TASTYTRADE_CLIENT_SECRET_LIVE") \
                         or os.environ.get("TASTYTRADE_CLIENT_SECRET")
            refresh_token = os.environ.get("TASTYTRADE_REFRESH_TOKEN_LIVE") \
                         or os.environ.get("TASTYTRADE_REFRESH_TOKEN")

        if not client_secret or not refresh_token:
            env_prefix = "PAPER" if paper else "LIVE"
            log.error(
                "❌ Missing credentials — set TASTYTRADE_CLIENT_SECRET_%s and "
                "TASTYTRADE_REFRESH_TOKEN_%s in your .env", env_prefix, env_prefix
            )
            raise SystemExit(1)

        self.client = TastyTradeClient(client_secret, refresh_token, paper=paper)
        self.client.get_account()
        equity = self.client.get_equity()

        # Populate status
        self.status.mode         = "PAPER" if paper else "LIVE"
        self.status.account      = self.client.account_number
        self.status.equity       = equity
        self.status.algo_enabled = self.settings["execution"]["algo_enabled"]
        self.reload_settings()

        log.info("💰 Account: %s | Equity: $%.2f | Mode: %s",
                 self.client.account_number, equity, self.status.mode)

        start_status_server(self.status)
        self.reconcile_positions()

        if self.settings["strategy_daily"].get("enabled"):
            await asyncio.to_thread(self._load_daily_candles)

        s15_list    = self.settings["strategy_15min"]["watchlist"]
        sd_list     = self.settings["strategy_daily"]["watchlist"]
        all_symbols = list(dict.fromkeys(s15_list + sd_list))
        log.info("📋 Streaming %d symbols: %s", len(all_symbols), ", ".join(all_symbols))

        ws_url, token = self.client.get_streamer_token()

        async def settings_reloader():
            while True:
                await asyncio.sleep(60)
                try:
                    self.reload_settings()
                except Exception as e:
                    log.warning("Settings reload failed: %s", e)

        async def daily_candle_refresher():
            while True:
                await asyncio.sleep(3600)
                if self.settings["strategy_daily"].get("enabled"):
                    await asyncio.to_thread(self._load_daily_candles)

        async def status_writer():
            while True:
                await asyncio.sleep(3)
                try:
                    # Refresh equity every 60s
                    if int(time.time()) % 60 < 3:
                        self.status.equity = await asyncio.to_thread(self.client.get_equity)
                    self.status.write()
                except Exception as e:
                    log.debug("Status write error: %s", e)

        stream = StreamHandler(ws_url, token, all_symbols, self.on_tick, self.status)

        await asyncio.gather(
            stream.run(),
            settings_reloader(),
            daily_candle_refresher(),
            status_writer(),
        )


# ══════════════════════════════════════════════════════════════════════════════
# Entry Point
# ══════════════════════════════════════════════════════════════════════════════

def main():
    engine = AlgoEngine()
    loop   = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _shutdown(sig, frame):
        log.info("🛑 Shutdown — saving state...")
        save_state(engine.state)
        engine.status.write()
        loop.stop()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        loop.run_until_complete(engine.run())
    finally:
        loop.close()
        log.info("👋 Trader stopped.")


if __name__ == "__main__":
    main()
