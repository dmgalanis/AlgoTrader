"""
TastyTrade Multi-Strategy Auto-Trader v1.2
==========================================
Strategies:
  1. 15-min EMA Cloud (72/89) — pullback + breakout entries
  2. Daily EMA pullback — price within N% above a single EMA on daily chart

New in v1.2:
  - Fill confirmation: waits for actual fill before placing stop/TP orders,
    sizes stop/TP to actual filled quantity (not intended quantity)
  - Startup reconciliation: syncs open positions from TastyTrade on boot
    so restarts after crashes are safe
  - Daily strategy engine with independent watchlist, risk, and TP settings
  - Combined WebSocket stream for all unique symbols across both strategies

Run:    python bot/trader.py
Docker: docker-compose up -d
"""

import asyncio
import json
import logging
import os
import signal
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
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

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SETTINGS_PATH = os.path.join(ROOT, "config", "settings.json")
STATE_PATH    = os.path.join(ROOT, "config", "state.json")

TT_BASE_LIVE  = "https://api.tastytrade.com"
TT_BASE_PAPER = "https://api.cert.tastyworks.com"

FILL_POLL_INTERVAL = 2    # seconds between order status checks
FILL_POLL_TIMEOUT  = 30   # seconds to wait for a fill before giving up


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
        "trade_date": str(date.today()),
        "s15_trades_today": 0,
        "sd_trades_today": 0,
        "s15_positions": {},   # symbol → position dict (15-min strategy)
        "sd_positions":  {},   # symbol → position dict (daily strategy)
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
        self.session_token = resp.json()["data"]["session-token"]
        log.info("✅ TastyTrade authenticated (%s)", "PAPER" if self.paper else "LIVE")

    def _h(self) -> dict:
        return {"Authorization": self.session_token, "Content-Type": "application/json"}

    # ── Account ───────────────────────────────────────────────────────────────

    def get_account(self) -> str:
        resp = requests.get(f"{self.base}/customers/me/accounts", headers=self._h(), timeout=10)
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

    def get_positions(self) -> list[dict]:
        """Returns list of current open positions from TastyTrade."""
        resp = requests.get(
            f"{self.base}/accounts/{self.account_number}/positions",
            headers=self._h(), timeout=10,
        )
        resp.raise_for_status()
        return resp.json()["data"]["items"]

    # ── Streamer token ────────────────────────────────────────────────────────

    def get_streamer_token(self) -> tuple[str, str]:
        resp = requests.get(
            f"{self.base}/quote-streamer-tokens", headers=self._h(), timeout=10
        )
        resp.raise_for_status()
        d = resp.json()["data"]
        return d["websocket-url"], d["token"]

    # ── Orders ────────────────────────────────────────────────────────────────

    def place_market_buy(self, symbol: str, quantity: int) -> str:
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

    def get_order(self, order_id: str) -> dict:
        """Fetch a single order by ID. Returns the order dict."""
        resp = requests.get(
            f"{self.base}/accounts/{self.account_number}/orders/{order_id}",
            headers=self._h(), timeout=10,
        )
        resp.raise_for_status()
        return resp.json()["data"]

    def poll_fill(self, order_id: str) -> Optional[dict]:
        """
        Polls order status until Filled or timeout.
        Returns dict with {filled_quantity, average_fill_price} or None on timeout.
        """
        deadline = time.time() + FILL_POLL_TIMEOUT
        while time.time() < deadline:
            try:
                order = self.get_order(order_id)
                status = order.get("status", "")
                log.debug("  ⏳ Order %s status: %s", order_id, status)

                if status == "Filled":
                    legs = order.get("legs", [{}])
                    fills = legs[0].get("fills", []) if legs else []
                    if fills:
                        total_qty  = sum(float(f.get("fill-quantity", 0)) for f in fills)
                        total_cost = sum(
                            float(f.get("fill-quantity", 0)) * float(f.get("fill-price", 0))
                            for f in fills
                        )
                        avg_price = total_cost / total_qty if total_qty else 0
                        return {"filled_quantity": int(total_qty), "average_fill_price": round(avg_price, 4)}
                    # Fallback if fills array is absent (sandbox behaviour)
                    return {
                        "filled_quantity": int(order.get("filled-quantity", 0)),
                        "average_fill_price": float(order.get("average-fill-price", 0)),
                    }

                if status in ("Cancelled", "Rejected", "Expired"):
                    log.warning("  ⚠️  Order %s ended with status: %s", order_id, status)
                    return None

            except Exception as e:
                log.warning("  ⚠️  Error polling order %s: %s", order_id, e)

            time.sleep(FILL_POLL_INTERVAL)

        log.warning("  ⚠️  Fill confirmation timed out for order %s", order_id)
        return None

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

    # ── Historical candles (for daily strategy warm-up) ───────────────────────

    def get_daily_candles(self, symbol: str, count: int = 300) -> pd.DataFrame:
        """Fetch daily OHLCV candles via REST for EMA warm-up on the daily strategy."""
        resp = requests.get(
            f"{self.base}/market-data/candles/{symbol}",
            headers=self._h(),
            params={"timeframe": "1d", "count": count},
            timeout=15,
        )
        resp.raise_for_status()
        candles = resp.json()["data"]["items"]
        df = pd.DataFrame(candles)
        df["close"] = df["close"].astype(float)
        df["time"]  = pd.to_datetime(df["time"])
        return df.sort_values("time").reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# Candle Builder  (tick → N-min OHLCV in memory)
# ══════════════════════════════════════════════════════════════════════════════

class CandleBuilder:
    def __init__(self, period_seconds: int = 900):
        self.period = period_seconds
        self._current: dict[str, dict] = {}
        self.history:  dict[str, list] = defaultdict(list)

    def _bucket(self, ts: float) -> float:
        return ts - (ts % self.period)

    def push(self, symbol: str, price: float, size: float, ts: float) -> Optional[dict]:
        b = self._bucket(ts)
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

    def get_dataframe(self, symbol: str) -> pd.DataFrame:
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
# Signal Logic
# ══════════════════════════════════════════════════════════════════════════════

def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def check_signal_15min(df: pd.DataFrame, price: float, cfg: dict) -> Optional[str]:
    """
    Returns 'pullback' or 'breakout' or None.
    Uses 15-min candle history + live tick price.
    """
    fast = cfg["ema_fast"]
    slow = cfg["ema_slow"]
    if len(df) < max(fast, slow) + 2:
        return None

    df = df.copy()
    df["ema_f"] = _ema(df["close"], fast)
    df["ema_s"] = _ema(df["close"], slow)

    last  = df.iloc[-1]
    prev  = df.iloc[-2]
    cloud_top      = max(last["ema_f"], last["ema_s"])
    prev_cloud_top = max(prev["ema_f"], prev["ema_s"])

    pullback_pct  = cfg["entry"]["pullback_to_cloud_pct"] / 100
    breakout_pct  = cfg["entry"]["breakout_above_cloud_pct"] / 100

    # Pullback: above cloud, within pullback_pct of fast EMA
    if price > cloud_top:
        if (price - last["ema_f"]) / last["ema_f"] <= pullback_pct:
            return "pullback"

    # Breakout: last close ≤ prev cloud top, now breakout_pct above fast EMA
    if last["close"] <= prev_cloud_top and price >= last["ema_f"] * (1 + breakout_pct):
        if price > cloud_top:
            return "breakout"

    return None


def check_signal_daily(
    daily_df: pd.DataFrame,
    price: float,
    cfg: dict,
) -> Optional[str]:
    """
    Returns 'daily_pullback' or None.
    Entry: price is above the EMA and within pullback_to_ema_pct of it.
    Uses pre-fetched daily candles + live tick price.
    """
    period = cfg["ema_period"]
    if len(daily_df) < period + 2:
        return None

    df = daily_df.copy()
    df["ema"] = _ema(df["close"], period)
    ema_val = df["ema"].iloc[-1]

    if price <= ema_val:
        return None  # price is not above EMA

    distance_pct = (price - ema_val) / ema_val * 100
    threshold    = cfg["entry"]["pullback_to_ema_pct"]

    if distance_pct <= threshold:
        return "daily_pullback"

    return None


def calc_position_size(equity, entry, stop, risk_pct, max_pos_pct) -> int:
    risk_dollars   = equity * (risk_pct / 100)
    risk_per_share = entry - stop
    if risk_per_share <= 0:
        return 0
    shares     = int(risk_dollars / risk_per_share)
    max_shares = int((equity * (max_pos_pct / 100)) / entry)
    return max(0, min(shares, max_shares))


# ══════════════════════════════════════════════════════════════════════════════
# DXFeed WebSocket Stream
# ══════════════════════════════════════════════════════════════════════════════

class StreamHandler:
    HEARTBEAT_INTERVAL = 20

    def __init__(self, ws_url: str, token: str, symbols: list[str], on_tick):
        self.ws_url  = ws_url
        self.token   = token
        self.symbols = symbols
        self.on_tick = on_tick
        self._running = False

    async def run(self):
        self._running = True
        while self._running:
            try:
                await self._connect()
            except Exception as e:
                log.warning("🔌 WebSocket disconnected: %s — reconnecting in 5s", e)
                await asyncio.sleep(5)

    async def _connect(self):
        log.info("🔌 Connecting to DXFeed stream for %d symbols...", len(self.symbols))
        async with websockets.connect(
            self.ws_url,
            extra_headers={"Authorization": self.token},
            ping_interval=None,
        ) as ws:
            # Handshake
            await ws.send(json.dumps([{
                "advice": {"timeout": 60000, "interval": 0},
                "channel": "/meta/handshake",
                "supportedConnectionTypes": ["websocket"],
                "version": "1.0",
            }]))
            # Subscribe
            await ws.send(json.dumps([{
                "channel": "/service/sub",
                "data": {"add": {"Quote": self.symbols}},
            }]))
            log.info("📡 Stream live. Watching: %s", ", ".join(self.symbols))

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

    async def _heartbeat(self, ws):
        while True:
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)
            try:
                await ws.send(json.dumps([{
                    "channel": "/meta/connect",
                    "connectionType": "websocket",
                }]))
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
                self.on_tick(symbol, price, size, ts)
            except (TypeError, ValueError):
                continue

    def stop(self):
        self._running = False


# ══════════════════════════════════════════════════════════════════════════════
# Shared Order Executor
# ══════════════════════════════════════════════════════════════════════════════

async def execute_entry(
    client: TastyTradeClient,
    symbol: str,
    intended_price: float,
    signal: str,
    strategy_cfg: dict,
    state: dict,
    position_key: str,   # 's15_positions' or 'sd_positions'
    trades_key: str,     # 's15_trades_today' or 'sd_trades_today'
    tz: ZoneInfo,
) -> bool:
    """
    Full entry sequence with fill confirmation:
      1. Place market buy
      2. Poll for fill — get ACTUAL filled qty and fill price
      3. Size stop and TP orders to actual fill
    Returns True on success, False on failure.
    """
    try:
        equity   = client.get_equity()
        risk_cfg = strategy_cfg["risk"]
        stop_pct = risk_cfg["stop_loss_pct"] / 100

        # Estimate stop using intended price for sizing
        stop_estimate = round(intended_price * (1 - stop_pct), 2)
        shares = calc_position_size(
            equity=equity,
            entry=intended_price,
            stop=stop_estimate,
            risk_pct=risk_cfg["risk_per_trade_pct"],
            max_pos_pct=risk_cfg["max_position_size_pct"],
        )

        if shares <= 0:
            log.warning("  ⚠️  [%s] Position size 0 — skipping.", symbol)
            return False

        log.info(
            "  📐 [%s] Intended entry=$%.2f  est.stop=$%.2f  shares=%d",
            symbol, intended_price, stop_estimate, shares,
        )

        # ── 1. Place market buy ───────────────────────────────────────────
        buy_id = await asyncio.to_thread(client.place_market_buy, symbol, shares)
        log.info("  📤 BUY order submitted (id=%s) — awaiting fill...", buy_id)

        # ── 2. Poll for actual fill ───────────────────────────────────────
        fill = await asyncio.to_thread(client.poll_fill, buy_id)

        if not fill or fill["filled_quantity"] <= 0:
            log.error("  ❌ [%s] Fill not confirmed — stop/TP NOT placed. Review manually.", symbol)
            return False

        filled_qty   = fill["filled_quantity"]
        fill_price   = fill["average_fill_price"] or intended_price
        actual_stop  = round(fill_price * (1 - stop_pct), 2)
        slippage     = fill_price - intended_price

        log.info(
            "  ✅ FILLED %d shares @ $%.4f (slippage: %+.4f)  stop=$%.2f",
            filled_qty, fill_price, slippage, actual_stop,
        )

        # ── 3. Stop loss sized to actual fill ─────────────────────────────
        sl_id = await asyncio.to_thread(
            client.place_stop, symbol, filled_qty, actual_stop
        )
        log.info("  🛑 Stop-loss placed @ $%.2f for %d shares (id=%s)",
                 actual_stop, filled_qty, sl_id)

        # ── 4. Take profit tiers sized to actual fill ─────────────────────
        initial_risk = fill_price * stop_pct
        tp_placed = []
        for tier in strategy_cfg["take_profit_tiers"]:
            if not tier["enabled"]:
                continue
            tp_price  = round(fill_price + initial_risk * tier["r_multiple"], 2)
            tp_shares = max(1, int(filled_qty * (tier["sell_pct"] / 100)))
            tp_id = await asyncio.to_thread(
                client.place_limit, symbol, tp_shares, tp_price
            )
            log.info("  🎯 TP %sR (%s%%) @ $%.2f for %d shares (id=%s)",
                     tier["r_multiple"], tier["sell_pct"], tp_price, tp_shares, tp_id)
            tp_placed.append({
                "r": tier["r_multiple"], "price": tp_price,
                "shares": tp_shares, "order_id": tp_id,
            })

        # ── 5. Persist position state ─────────────────────────────────────
        state[position_key][symbol] = {
            "entry_price":    fill_price,
            "stop_price":     actual_stop,
            "shares":         filled_qty,
            "signal":         signal,
            "entry_time":     datetime.now(tz).isoformat(),
            "buy_order_id":   buy_id,
            "stop_order_id":  sl_id,
            "tp_tiers":       tp_placed,
        }
        state[trades_key] = state.get(trades_key, 0) + 1
        save_state(state)

        log.info("  📋 [%s] Position saved. %s trades today: %d",
                 symbol, trades_key, state[trades_key])
        return True

    except Exception as e:
        log.error("  ❌ Entry execution failed for %s: %s", symbol, e, exc_info=True)
        return False


# ══════════════════════════════════════════════════════════════════════════════
# Main Algo Engine
# ══════════════════════════════════════════════════════════════════════════════

class AlgoEngine:
    def __init__(self):
        self.settings = load_settings()
        self.state    = load_state()
        self.client:  Optional[TastyTradeClient] = None
        self.tz       = ZoneInfo(self.settings["execution"]["timezone"])

        # 15-min candle builder
        tf = self.settings["strategy_15min"]["timeframe_minutes"]
        self.candles_15m = CandleBuilder(period_seconds=tf * 60)

        # Daily candle cache — pre-loaded at startup, updated once per day
        self.daily_candles: dict[str, pd.DataFrame] = {}
        self._daily_candles_date: Optional[str] = None

        # Signal debounce — {strategy_key: {symbol: last_ts}}
        self._last_signal: dict[str, dict[str, float]] = {
            "s15": defaultdict(float),
            "sd":  defaultdict(float),
        }
        self._signal_cooldown = 60  # seconds
        self._lock = asyncio.Lock()

    def reload_settings(self):
        self.settings = load_settings()

    def _is_market_hours(self) -> bool:
        now = datetime.now(self.tz)
        if now.weekday() >= 5:
            return False
        cfg    = self.settings["execution"]
        open_t = datetime.strptime(cfg["market_open"],  "%H:%M").replace(
            tzinfo=self.tz, year=now.year, month=now.month, day=now.day)
        close_t = datetime.strptime(cfg["market_close"], "%H:%M").replace(
            tzinfo=self.tz, year=now.year, month=now.month, day=now.day)
        return open_t <= now <= close_t

    def _reset_daily_if_needed(self):
        today = str(date.today())
        if self.state.get("trade_date") != today:
            self.state["trade_date"]      = today
            self.state["s15_trades_today"] = 0
            self.state["sd_trades_today"]  = 0
            save_state(self.state)
            log.info("🗓️  New trading day — counters reset.")

    # ── Startup Reconciliation ────────────────────────────────────────────────

    def reconcile_positions(self):
        """
        On startup, fetch real open positions from TastyTrade and cross-check
        against our saved state. Removes stale entries (positions that were
        closed while bot was offline). Adds a warning for any live positions
        not tracked in state (opened manually or after a crash mid-order).
        """
        log.info("🔍 Reconciling positions with TastyTrade account...")
        try:
            live_positions = self.client.get_positions()
            live_symbols   = {p["symbol"] for p in live_positions if float(p.get("quantity", 0)) > 0}

            for strat_key, pos_dict in [
                ("s15_positions", self.state.get("s15_positions", {})),
                ("sd_positions",  self.state.get("sd_positions",  {})),
            ]:
                stale = [sym for sym in pos_dict if sym not in live_symbols]
                for sym in stale:
                    log.warning(
                        "  ⚠️  [%s] %s in state but NOT in live account — removing from tracking.",
                        strat_key, sym,
                    )
                    del self.state[strat_key][sym]

            all_tracked = set(self.state.get("s15_positions", {}).keys()) | \
                          set(self.state.get("sd_positions",  {}).keys())
            untracked = live_symbols - all_tracked
            if untracked:
                log.info(
                    "  ℹ️  Live positions not tracked by algo (manually opened): %s",
                    ", ".join(sorted(untracked)),
                )

            save_state(self.state)
            log.info("✅ Reconciliation complete. Tracking %d algo position(s).",
                     len(self.state.get("s15_positions", {})) +
                     len(self.state.get("sd_positions",  {})))
        except Exception as e:
            log.error("❌ Reconciliation failed: %s", e, exc_info=True)

    # ── Daily candle warm-up ──────────────────────────────────────────────────

    def _load_daily_candles(self):
        """Fetch/refresh daily candle history for daily strategy symbols."""
        today = str(date.today())
        if self._daily_candles_date == today:
            return  # already loaded today
        cfg = self.settings["strategy_daily"]
        log.info("📅 Loading daily candles for %d symbols...", len(cfg["watchlist"]))
        for sym in cfg["watchlist"]:
            try:
                df = self.client.get_daily_candles(sym, count=300)
                self.daily_candles[sym] = df
                log.debug("  %s: %d daily candles loaded.", sym, len(df))
            except Exception as e:
                log.warning("  ⚠️  Could not load daily candles for %s: %s", sym, e)
        self._daily_candles_date = today
        log.info("✅ Daily candles loaded.")

    # ── Tick handler ──────────────────────────────────────────────────────────

    def on_tick(self, symbol: str, price: float, size: float, ts: float):
        self.candles_15m.push(symbol, price, size, ts)
        asyncio.create_task(self._evaluate(symbol, price, ts))

    async def _evaluate(self, symbol: str, price: float, ts: float):
        async with self._lock:
            self._reset_daily_if_needed()

            if not self.settings["execution"]["algo_enabled"]:
                return
            if not self._is_market_hours():
                return

            s15 = self.settings["strategy_15min"]
            sd  = self.settings["strategy_daily"]

            # ── Strategy 1: 15-min EMA Cloud ──────────────────────────────
            if (s15.get("enabled") and
                    symbol in s15["watchlist"] and
                    symbol not in self.state.get("s15_positions", {}) and
                    self.state.get("s15_trades_today", 0) < s15["daily_trade_limit"] and
                    ts - self._last_signal["s15"][symbol] > self._signal_cooldown):

                df = self.candles_15m.get_dataframe(symbol)
                sig = check_signal_15min(df, price, s15) if not df.empty else None
                if sig:
                    self._last_signal["s15"][symbol] = ts
                    log.info("🎯 [15M/%s] %s signal for %s @ $%.2f",
                             sig.upper(), "S1", symbol, price)
                    await execute_entry(
                        client=self.client,
                        symbol=symbol,
                        intended_price=price,
                        signal=sig,
                        strategy_cfg=s15,
                        state=self.state,
                        position_key="s15_positions",
                        trades_key="s15_trades_today",
                        tz=self.tz,
                    )

            # ── Strategy 2: Daily EMA Pullback ────────────────────────────
            if (sd.get("enabled") and
                    symbol in sd["watchlist"] and
                    symbol not in self.state.get("sd_positions", {}) and
                    self.state.get("sd_trades_today", 0) < sd["daily_trade_limit"] and
                    ts - self._last_signal["sd"][symbol] > self._signal_cooldown):

                daily_df = self.daily_candles.get(symbol)
                if daily_df is not None and not daily_df.empty:
                    sig = check_signal_daily(daily_df, price, sd)
                    if sig:
                        self._last_signal["sd"][symbol] = ts
                        log.info("🎯 [DAILY/%s] %s signal for %s @ $%.2f",
                                 sig.upper(), "S2", symbol, price)
                        await execute_entry(
                            client=self.client,
                            symbol=symbol,
                            intended_price=price,
                            signal=sig,
                            strategy_cfg=sd,
                            state=self.state,
                            position_key="sd_positions",
                            trades_key="sd_trades_today",
                            tz=self.tz,
                        )

    # ── Main run loop ─────────────────────────────────────────────────────────

    async def run(self):
        log.info("═" * 60)
        log.info("  TastyTrade Multi-Strategy Trader  v1.2")
        log.info("═" * 60)

        username = os.environ["TASTYTRADE_USERNAME"]
        password = os.environ["TASTYTRADE_PASSWORD"]
        paper    = self.settings["execution"]["paper_trading"]

        self.client = TastyTradeClient(username, password, paper=paper)
        self.client.get_account()
        equity = self.client.get_equity()
        log.info("💰 Account: %s | Equity: $%.2f | Mode: %s",
                 self.client.account_number, equity,
                 "PAPER" if paper else "LIVE")

        # Reconcile state with live account before doing anything
        self.reconcile_positions()

        # Load daily candles for strategy 2
        if self.settings["strategy_daily"].get("enabled"):
            await asyncio.to_thread(self._load_daily_candles)

        # Combined watchlist — union of both strategies
        s15_list = self.settings["strategy_15min"]["watchlist"]
        sd_list  = self.settings["strategy_daily"]["watchlist"]
        all_symbols = list(dict.fromkeys(s15_list + sd_list))  # preserve order, dedupe
        log.info("📋 Streaming %d symbols: %s", len(all_symbols), ", ".join(all_symbols))

        ws_url, token = self.client.get_streamer_token()

        async def settings_reloader():
            while True:
                await asyncio.sleep(60)
                try:
                    self.reload_settings()
                    log.debug("⚙️  Settings reloaded.")
                except Exception as e:
                    log.warning("Could not reload settings: %s", e)

        async def daily_candle_refresher():
            """Refresh daily candles once per day, shortly after market open."""
            while True:
                await asyncio.sleep(3600)  # check every hour
                if self.settings["strategy_daily"].get("enabled"):
                    await asyncio.to_thread(self._load_daily_candles)

        stream = StreamHandler(ws_url, token, all_symbols, self.on_tick)

        await asyncio.gather(
            stream.run(),
            settings_reloader(),
            daily_candle_refresher(),
        )


# ══════════════════════════════════════════════════════════════════════════════
# Entry Point
# ══════════════════════════════════════════════════════════════════════════════

def main():
    engine = AlgoEngine()
    loop   = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _shutdown(sig, frame):
        log.info("🛑 Shutdown signal received — saving state...")
        save_state(engine.state)
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
