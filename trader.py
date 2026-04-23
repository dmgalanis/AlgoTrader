"""
TastyTrade EMA Cloud Auto-Trader — WebSocket Edition
=====================================================
Persistent async process that:
  1. Authenticates with TastyTrade REST API
  2. Opens a DXFeed WebSocket stream for all watchlist symbols
  3. Aggregates ticks into 15-min candles in memory
  4. Evaluates 72/89 EMA cloud entry conditions on every new tick
  5. Places buy, stop-loss, and take-profit orders immediately on signal

Run with:  python bot/trader.py
Docker:    docker-compose up -d
"""

import asyncio
import json
import logging
import os
import signal
import time
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import requests
import websockets
import pandas as pd
import numpy as np

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

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SETTINGS_PATH = os.path.join(ROOT, "config", "settings.json")
STATE_PATH = os.path.join(ROOT, "config", "state.json")

# ── TastyTrade endpoints ───────────────────────────────────────────────────────
TT_BASE_LIVE = "https://api.tastytrade.com"
TT_BASE_PAPER = "https://api.cert.tastyworks.com"


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
        "trades_today": 0,
        "trade_date": str(date.today()),
        "open_algo_positions": {},
    }


def save_state(state: dict):
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)


# ══════════════════════════════════════════════════════════════════════════════
# TastyTrade REST Client
# ══════════════════════════════════════════════════════════════════════════════

class TastyTradeClient:
    def __init__(self, username: str, password: str, paper: bool = True):
        self.base = TT_BASE_PAPER if paper else TT_BASE_LIVE
        self.paper = paper
        self.session_token: Optional[str] = None
        self.account_number: Optional[str] = None
        self._login(username, password)

    def _login(self, username: str, password: str):
        resp = requests.post(
            f"{self.base}/sessions",
            json={"login": username, "password": password, "remember-me": True},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()["data"]
        self.session_token = data["session-token"]
        log.info("✅ TastyTrade session established (%s)", "PAPER" if self.paper else "LIVE")

    def _h(self) -> dict:
        return {"Authorization": self.session_token, "Content-Type": "application/json"}

    def get_streamer_token(self) -> tuple[str, str]:
        """Returns (websocket_url, streamer_token) for DXFeed."""
        resp = requests.get(
            f"{self.base}/quote-streamer-tokens", headers=self._h(), timeout=10
        )
        resp.raise_for_status()
        d = resp.json()["data"]
        return d["websocket-url"], d["token"]

    def get_account(self) -> str:
        resp = requests.get(
            f"{self.base}/customers/me/accounts", headers=self._h(), timeout=10
        )
        resp.raise_for_status()
        self.account_number = resp.json()["data"]["items"][0]["account"]["account-number"]
        return self.account_number

    def get_equity(self) -> float:
        resp = requests.get(
            f"{self.base}/accounts/{self.account_number}/balances",
            headers=self._h(), timeout=10,
        )
        resp.raise_for_status()
        return float(resp.json()["data"]["net-liquidating-value"])

    def place_market_buy(self, symbol: str, quantity: int) -> str:
        """Returns order ID."""
        order = {
            "time-in-force": "Day",
            "order-type": "Market",
            "legs": [{"instrument-type": "Equity", "symbol": symbol,
                       "quantity": quantity, "action": "Buy to Open"}],
        }
        resp = requests.post(
            f"{self.base}/accounts/{self.account_number}/orders",
            headers=self._h(), json=order, timeout=10,
        )
        resp.raise_for_status()
        return str(resp.json()["data"]["order"]["id"])

    def place_stop(self, symbol: str, quantity: int, stop_price: float) -> str:
        order = {
            "time-in-force": "GTC",
            "order-type": "Stop",
            "stop-trigger": str(round(stop_price, 2)),
            "legs": [{"instrument-type": "Equity", "symbol": symbol,
                       "quantity": quantity, "action": "Sell to Close"}],
        }
        resp = requests.post(
            f"{self.base}/accounts/{self.account_number}/orders",
            headers=self._h(), json=order, timeout=10,
        )
        resp.raise_for_status()
        return str(resp.json()["data"]["order"]["id"])

    def place_limit(self, symbol: str, quantity: int, limit_price: float) -> str:
        order = {
            "time-in-force": "GTC",
            "order-type": "Limit",
            "price": str(round(limit_price, 2)),
            "price-effect": "Debit",
            "legs": [{"instrument-type": "Equity", "symbol": symbol,
                       "quantity": quantity, "action": "Sell to Close"}],
        }
        resp = requests.post(
            f"{self.base}/accounts/{self.account_number}/orders",
            headers=self._h(), json=order, timeout=10,
        )
        resp.raise_for_status()
        return str(resp.json()["data"]["order"]["id"])


# ══════════════════════════════════════════════════════════════════════════════
# Candle Builder  (tick → 15-min OHLCV in memory)
# ══════════════════════════════════════════════════════════════════════════════

class CandleBuilder:
    """
    Aggregates real-time ticks into fixed-duration candles.
    Emits a completed candle dict when a new period starts.
    """

    def __init__(self, period_seconds: int = 900):  # 900s = 15 min
        self.period = period_seconds
        # symbol → {open, high, low, close, volume, ts}
        self._current: dict[str, dict] = {}
        # symbol → list of completed candle dicts
        self.history: dict[str, list] = defaultdict(list)

    def _period_start(self, ts: float) -> float:
        return ts - (ts % self.period)

    def push(self, symbol: str, price: float, size: float, ts: float) -> Optional[dict]:
        """
        Feed a tick. Returns the just-completed candle if the period rolled,
        otherwise None.
        """
        bucket = self._period_start(ts)
        completed = None

        if symbol not in self._current:
            self._current[symbol] = {
                "ts": bucket, "open": price, "high": price,
                "low": price, "close": price, "volume": size,
            }
        else:
            c = self._current[symbol]
            if bucket > c["ts"]:
                # Period rolled — emit completed candle
                completed = dict(c)
                self.history[symbol].append(completed)
                # Keep only last 200 candles
                if len(self.history[symbol]) > 200:
                    self.history[symbol] = self.history[symbol][-200:]
                # Start new candle
                self._current[symbol] = {
                    "ts": bucket, "open": price, "high": price,
                    "low": price, "close": price, "volume": size,
                }
            else:
                c["high"] = max(c["high"], price)
                c["low"] = min(c["low"], price)
                c["close"] = price
                c["volume"] += size

        return completed

    def get_dataframe(self, symbol: str) -> pd.DataFrame:
        """Returns completed candles as a DataFrame."""
        rows = self.history.get(symbol, [])
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df["close"] = df["close"].astype(float)
        return df.sort_values("ts").reset_index(drop=True)

    def current_price(self, symbol: str) -> Optional[float]:
        c = self._current.get(symbol)
        return c["close"] if c else None


# ══════════════════════════════════════════════════════════════════════════════
# EMA / Signal Logic
# ══════════════════════════════════════════════════════════════════════════════

def compute_emas(df: pd.DataFrame, fast: int, slow: int) -> pd.DataFrame:
    df = df.copy()
    df[f"ema{fast}"] = df["close"].ewm(span=fast, adjust=False).mean()
    df[f"ema{slow}"] = df["close"].ewm(span=slow, adjust=False).mean()
    return df


def check_signal(df: pd.DataFrame, current_price: float, settings: dict) -> Optional[str]:
    """
    Evaluate entry signal using completed candles + current live price.
    Returns 'pullback', 'breakout', or None.
    """
    fast = settings["ema_fast"]
    slow = settings["ema_slow"]
    pullback_pct = settings["entry"]["pullback_to_cloud_pct"] / 100
    breakout_pct = settings["entry"]["breakout_above_cloud_pct"] / 100

    min_candles = max(fast, slow) + 2
    if len(df) < min_candles:
        return None

    df = compute_emas(df, fast, slow)
    last = df.iloc[-1]
    prev = df.iloc[-2]

    ema_f = last[f"ema{fast}"]
    ema_s = last[f"ema{slow}"]
    cloud_top = max(ema_f, ema_s)

    prev_ema_f = prev[f"ema{fast}"]
    prev_ema_s = prev[f"ema{slow}"]
    prev_cloud_top = max(prev_ema_f, prev_ema_s)

    price = current_price  # use live tick price, not candle close

    # ── Pullback Entry ─────────────────────────────────────────────────────
    # Price is above cloud, and has pulled back to within pullback_pct of fast EMA
    if price > cloud_top:
        distance_pct = (price - ema_f) / ema_f
        if distance_pct <= pullback_pct:
            return "pullback"

    # ── Breakout Entry ─────────────────────────────────────────────────────
    # Previous candle close was at or below cloud top; current price is now
    # breakout_pct above the fast EMA and above the full cloud
    if last["close"] <= prev_cloud_top and price >= ema_f * (1 + breakout_pct):
        if price > cloud_top:
            return "breakout"

    return None


def calc_position_size(
    equity: float,
    entry: float,
    stop: float,
    risk_pct: float,
    max_pos_pct: float,
) -> int:
    risk_dollars = equity * (risk_pct / 100)
    risk_per_share = entry - stop
    if risk_per_share <= 0:
        return 0
    shares = int(risk_dollars / risk_per_share)
    max_shares = int((equity * (max_pos_pct / 100)) / entry)
    return max(0, min(shares, max_shares))


# ══════════════════════════════════════════════════════════════════════════════
# DXFeed WebSocket Stream
# ══════════════════════════════════════════════════════════════════════════════

class StreamHandler:
    """
    Connects to TastyTrade's DXFeed WebSocket, subscribes to Quote events
    for all watchlist symbols, and calls on_tick(symbol, price, size, ts)
    for each update.
    """

    HEARTBEAT_INTERVAL = 20  # seconds

    def __init__(self, ws_url: str, token: str, symbols: list[str], on_tick):
        self.ws_url = ws_url
        self.token = token
        self.symbols = symbols
        self.on_tick = on_tick
        self._running = False

    async def run(self):
        self._running = True
        while self._running:
            try:
                await self._connect()
            except Exception as e:
                log.warning("WebSocket disconnected: %s — reconnecting in 5s", e)
                await asyncio.sleep(5)

    async def _connect(self):
        log.info("🔌 Connecting to DXFeed stream...")
        async with websockets.connect(
            self.ws_url,
            extra_headers={"Authorization": self.token},
            ping_interval=None,  # we manage heartbeat manually
        ) as ws:
            log.info("📡 Stream connected. Subscribing to %d symbols.", len(self.symbols))

            # DXFeed handshake
            await ws.send(json.dumps([{
                "advice": {"timeout": 60000, "interval": 0},
                "channel": "/meta/handshake",
                "supportedConnectionTypes": ["websocket"],
                "version": "1.0",
            }]))

            # Subscribe to Quote feed for all symbols
            sub_msg = [{
                "channel": "/service/sub",
                "data": {
                    "add": {
                        "Quote": self.symbols
                    }
                },
            }]
            await ws.send(json.dumps(sub_msg))

            heartbeat_task = asyncio.create_task(self._heartbeat(ws))

            try:
                async for raw in ws:
                    msgs = json.loads(raw)
                    if not isinstance(msgs, list):
                        msgs = [msgs]
                    for msg in msgs:
                        self._handle_message(msg)
            finally:
                heartbeat_task.cancel()

    async def _heartbeat(self, ws):
        while True:
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)
            try:
                await ws.send(json.dumps([{"channel": "/meta/connect",
                                            "connectionType": "websocket"}]))
            except Exception:
                break

    def _handle_message(self, msg: dict):
        channel = msg.get("channel", "")
        if channel != "/service/data":
            return
        data = msg.get("data", {})
        feed_data = data.get("Quote", {})
        ts = time.time()

        for symbol, quote in feed_data.items():
            # DXFeed Quote has bidPrice/askPrice; use midpoint
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
        self.state = load_state()
        self.client: Optional[TastyTradeClient] = None
        self.candles = CandleBuilder(period_seconds=self.settings["timeframe_minutes"] * 60)
        self.tz = ZoneInfo(self.settings["execution"]["timezone"])
        # Debounce: track last signal time per symbol to avoid rapid re-entry
        self._last_signal: dict[str, float] = {}
        self._signal_cooldown = 60  # seconds between signals for same symbol
        self._lock = asyncio.Lock()

    def reload_settings(self):
        self.settings = load_settings()

    def _is_market_hours(self) -> bool:
        now = datetime.now(self.tz)
        if now.weekday() >= 5:  # Saturday/Sunday
            return False
        open_t = datetime.strptime(self.settings["execution"]["market_open"], "%H:%M").replace(
            tzinfo=self.tz, year=now.year, month=now.month, day=now.day
        )
        close_t = datetime.strptime(self.settings["execution"]["market_close"], "%H:%M").replace(
            tzinfo=self.tz, year=now.year, month=now.month, day=now.day
        )
        return open_t <= now <= close_t

    def _reset_daily_state_if_needed(self):
        today = str(date.today())
        if self.state.get("trade_date") != today:
            self.state["trades_today"] = 0
            self.state["trade_date"] = today
            save_state(self.state)
            log.info("🗓️  New trading day — daily counter reset.")

    def on_tick(self, symbol: str, price: float, size: float, ts: float):
        """Called on every incoming tick from the WebSocket stream."""
        # Push tick into candle builder
        completed_candle = self.candles.push(symbol, price, size, ts)
        if completed_candle:
            log.debug("📊 %s candle closed: O=%.2f H=%.2f L=%.2f C=%.2f",
                      symbol,
                      completed_candle["open"], completed_candle["high"],
                      completed_candle["low"], completed_candle["close"])

        # Schedule signal check without blocking the stream
        asyncio.create_task(self._check_signal_async(symbol, price, ts))

    async def _check_signal_async(self, symbol: str, price: float, ts: float):
        async with self._lock:
            self._reset_daily_state_if_needed()

            if not self.settings["execution"]["algo_enabled"]:
                return
            if not self._is_market_hours():
                return

            # Daily limit check
            if self.state["trades_today"] >= self.settings["daily_trade_limit"]:
                return

            # Already in a position for this symbol
            if symbol in self.state.get("open_algo_positions", {}):
                return

            # Signal cooldown debounce
            last = self._last_signal.get(symbol, 0)
            if ts - last < self._signal_cooldown:
                return

            # Need enough candles
            df = self.candles.get_dataframe(symbol)
            if df.empty:
                return

            signal = check_signal(df, price, self.settings)
            if not signal:
                return

            self._last_signal[symbol] = ts
            log.info("🎯 SIGNAL [%s] %s @ $%.2f", signal.upper(), symbol, price)

            await self._execute_entry(symbol, price, signal)

    async def _execute_entry(self, symbol: str, price: float, signal: str):
        try:
            equity = self.client.get_equity()
            stop_pct = self.settings["risk"]["stop_loss_pct"] / 100
            stop_price = round(price * (1 - stop_pct), 2)

            shares = calc_position_size(
                equity=equity,
                entry=price,
                stop=stop_price,
                risk_pct=self.settings["risk"]["risk_per_trade_pct"],
                max_pos_pct=self.settings["risk"]["max_position_size_pct"],
            )

            if shares <= 0:
                log.warning("  ⚠️  Position size 0 for %s — skipping.", symbol)
                return

            risk_amt = (price - stop_price) * shares
            log.info(
                "  📐 %s: entry=$%.2f  stop=$%.2f  shares=%d  risk=$%.2f",
                symbol, price, stop_price, shares, risk_amt,
            )

            # ── Market Buy ────────────────────────────────────────────────
            buy_id = await asyncio.to_thread(
                self.client.place_market_buy, symbol, shares
            )
            log.info("  ✅ BUY order placed (id=%s)", buy_id)

            # ── Stop Loss ─────────────────────────────────────────────────
            sl_id = await asyncio.to_thread(
                self.client.place_stop, symbol, shares, stop_price
            )
            log.info("  🛑 Stop-loss placed @ $%.2f (id=%s)", stop_price, sl_id)

            # ── Take Profit Tiers ─────────────────────────────────────────
            initial_risk = price * stop_pct
            tp_placed = []
            for tier in self.settings["take_profit_tiers"]:
                if not tier["enabled"]:
                    continue
                tp_price = round(price + initial_risk * tier["r_multiple"], 2)
                tp_shares = max(1, int(shares * (tier["sell_pct"] / 100)))
                tp_id = await asyncio.to_thread(
                    self.client.place_limit, symbol, tp_shares, tp_price
                )
                log.info(
                    "  🎯 TP %sR (%s%%) @ $%.2f  shares=%d  (id=%s)",
                    tier["r_multiple"], tier["sell_pct"], tp_price, tp_shares, tp_id,
                )
                tp_placed.append({
                    "r": tier["r_multiple"], "price": tp_price,
                    "shares": tp_shares, "order_id": tp_id,
                })

            # ── Update State ──────────────────────────────────────────────
            self.state.setdefault("open_algo_positions", {})[symbol] = {
                "entry_price": price,
                "stop_price": stop_price,
                "shares": shares,
                "signal": signal,
                "entry_time": datetime.now(self.tz).isoformat(),
                "stop_order_id": sl_id,
                "tp_tiers": tp_placed,
            }
            self.state["trades_today"] = self.state.get("trades_today", 0) + 1
            save_state(self.state)

            log.info(
                "  📋 Trades today: %d/%d",
                self.state["trades_today"],
                self.settings["daily_trade_limit"],
            )

        except Exception as e:
            log.error("  ❌ Entry execution failed for %s: %s", symbol, e, exc_info=True)

    async def run(self):
        log.info("═" * 60)
        log.info("  TastyTrade EMA Cloud Trader — WebSocket Edition")
        log.info("═" * 60)

        username = os.environ["TASTYTRADE_USERNAME"]
        password = os.environ["TASTYTRADE_PASSWORD"]
        paper = self.settings["execution"]["paper_trading"]

        self.client = TastyTradeClient(username, password, paper=paper)
        self.client.get_account()
        equity = self.client.get_equity()
        log.info("💰 Account: %s | Equity: $%.2f", self.client.account_number, equity)

        ws_url, token = self.client.get_streamer_token()
        watchlist = self.settings["watchlist"]
        log.info("📋 Watchlist (%d): %s", len(watchlist), ", ".join(watchlist))

        # Settings reload task — re-reads config every 60s so dashboard changes apply live
        async def settings_reloader():
            while True:
                await asyncio.sleep(60)
                try:
                    self.reload_settings()
                    log.debug("⚙️  Settings reloaded.")
                except Exception as e:
                    log.warning("Could not reload settings: %s", e)

        stream = StreamHandler(ws_url, token, watchlist, self.on_tick)

        # Run stream + settings reloader concurrently
        await asyncio.gather(
            stream.run(),
            settings_reloader(),
        )


# ══════════════════════════════════════════════════════════════════════════════
# Entry Point
# ══════════════════════════════════════════════════════════════════════════════

def main():
    engine = AlgoEngine()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Graceful shutdown on SIGINT/SIGTERM
    def _shutdown(sig, frame):
        log.info("🛑 Shutdown signal received. Saving state...")
        save_state(engine.state)
        loop.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        loop.run_until_complete(engine.run())
    finally:
        loop.close()
        log.info("👋 Trader stopped.")


if __name__ == "__main__":
    main()
