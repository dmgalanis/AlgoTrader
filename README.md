# 🟢 TastyTrade EMA Cloud Trader — WebSocket Edition

Real-time 72/89 EMA Cloud strategy bot. Streams live quotes via TastyTrade's DXFeed WebSocket — catches every tick, never misses a fast touch of the cloud.

---

## Architecture

```
┌─────────────────────────────────────────┐
│           VPS / Local Machine           │
│                                         │
│  ┌──────────────────────────────────┐   │
│  │   Docker: ema-cloud-trader       │   │
│  │                                  │   │
│  │  TastyTrade WebSocket ──► ticks  │   │
│  │         │                        │   │
│  │  CandleBuilder (15-min OHLCV)    │   │
│  │         │                        │   │
│  │  EMA 72/89 Cloud Signal Check    │   │
│  │         │                        │   │
│  │  TastyTrade REST → Orders        │   │
│  └──────────────────────────────────┘   │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│           GitHub Pages                  │
│   dashboard/index.html                  │
│   Edit watchlist, risk, TP tiers        │
│   → commits settings.json → VPS        │
│     pulls on next settings reload       │
└─────────────────────────────────────────┘
```

---

## Repository Structure

```
tastytrade-algo/
├── bot/
│   └── trader.py              # Async WebSocket trading engine
├── config/
│   ├── settings.json          # All strategy settings
│   └── state.json             # Runtime state (auto-managed, gitignored)
├── dashboard/
│   └── index.html             # GitHub Pages control panel
├── logs/                      # Log output (gitignored)
├── .github/
│   └── workflows/
│       └── deploy-dashboard.yml  # GitHub Pages deploy only
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── requirements.txt
└── README.md
```

---

## 🚀 Deployment Guide

### Option A — VPS (Recommended: DigitalOcean, Hetzner, Oracle Free Tier)

**1. Provision a server**

Any Linux VPS works. Minimum spec: 1 vCPU, 512MB RAM.

| Provider | Size | Cost |
|---|---|---|
| Oracle Cloud | Ampere A1 | **Free forever** |
| Hetzner | CX11 | ~$4/mo |
| DigitalOcean | Basic Droplet | ~$4/mo |
| Railway.app | Starter | ~$5/mo |

**2. Install Docker on the server**

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
```

**3. Clone your repo onto the server**

```bash
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO
```

**4. Create your .env file**

```bash
cp .env.example .env
nano .env   # fill in your TastyTrade credentials
```

**5. Start the bot**

```bash
docker-compose up -d
```

**6. Check it's running**

```bash
docker-compose logs -f      # live log stream
docker ps                   # confirm container is up
```

**7. Stop the bot**

```bash
docker-compose down
```

---

### Option B — Run Locally (no Docker)

```bash
pip install -r requirements.txt
export TASTYTRADE_USERNAME="your@email.com"
export TASTYTRADE_PASSWORD="yourpassword"
python bot/trader.py
```

---

## Dashboard Setup (GitHub Pages)

1. Push this repo to GitHub
2. Go to repo **Settings → Pages**
3. Source: **GitHub Actions**
4. The dashboard auto-deploys on every push to `main` that touches `dashboard/` or `settings.json`
5. Access it at: `https://YOUR_USERNAME.github.io/YOUR_REPO/`

### Applying Dashboard Changes to the Bot

The bot reloads `settings.json` every 60 seconds automatically. Workflow:

1. Edit settings in the dashboard → click **Save & Deploy**
2. This commits `settings.json` to the repo (if you wire up the GitHub API token in the dashboard)
3. Pull on the VPS: `git pull && docker-compose restart` (or automate with a cron/webhook)

---

## ⚙️ Settings Reference

| Setting | Description |
|---|---|
| `watchlist` | Symbols to subscribe to and scan |
| `ema_fast` / `ema_slow` | EMA periods (default 72/89) |
| `entry.pullback_to_cloud_pct` | Max % from 72 EMA for pullback entry |
| `entry.breakout_above_cloud_pct` | Min % above 72 EMA for breakout entry |
| `risk.risk_per_trade_pct` | % of equity risked per trade |
| `risk.stop_loss_pct` | Stop distance below entry |
| `risk.max_position_size_pct` | Hard cap on position size |
| `daily_trade_limit` | Max algo-opened positions per day |
| `take_profit_tiers` | Array of `{r_multiple, sell_pct, enabled}` |
| `execution.paper_trading` | `true` = sandbox, `false` = live |
| `execution.algo_enabled` | Master on/off switch |

---

## 📊 Signal Logic

### Pullback Entry
- Current live price > cloud top (both EMAs)  
- `(price - ema72) / ema72 ≤ pullback_to_cloud_pct`

### Breakout Entry
- Last completed candle close ≤ previous cloud top  
- Current live price ≥ `ema72 × (1 + breakout_above_cloud_pct)`  
- Current live price > cloud top

### Position Sizing
```
risk_dollars  = equity × (risk_per_trade_pct / 100)
shares        = risk_dollars / (entry_price - stop_price)
shares        = min(shares, equity × max_position_size_pct / entry_price)
```

### Take Profit
Each enabled tier sells `sell_pct`% of shares at `entry + initial_risk × r_multiple`.  
Stop loss stays at original level after partial fills.

---

## ⚠️ Important Notes

1. **Paper trade first.** Keep `paper_trading: true` until you have confidence.
2. The bot does **not** touch positions you open manually — only positions it opens are tracked.
3. The daily trade counter resets automatically each morning.
4. The bot reconnects automatically if the WebSocket drops.
5. Settings reload every 60 seconds — no restart needed for most config changes.
6. State (`state.json`) persists across restarts via Docker volume mount.

---

## 📝 License

Personal use. Not financial advice.
