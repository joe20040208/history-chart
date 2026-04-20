# History Chart — Explosive Mover Study Dashboard

Build a historical study list of big liquid winners across US, HK, Taiwan, Japan, Korea.
Flip through charts, teleport to the exact setup, study before/during/after conditions.

## Setup

```bash
# 1. Install deps
pip install -r requirements.txt

# 2. Add API key to .env
cp .env.example .env
# edit .env: EODHD_API_KEY=your_key_here

# 3. Run the one-time data pull (takes 6-12 hours, 15-30GB disk)
python -m scan.fetch_universe          # ~30 min — get ticker lists per exchange
python -m scan.fetch_ohlc              # ~6-10 hrs — pull 10yr OHLC for every name
python -m scan.fetch_fundamentals      # ~1-2 hrs — shares outstanding for market cap
python -m scan.screen                  # ~15 min — find runners, save runners.parquet

# 4. Cancel EODHD subscription — you have everything locally now

# 5. Launch dashboard
python -m dashboard.server
# open http://localhost:8000
```

## Dashboard controls

- Sliders: % change, min market cap, min avg $vol (30d), performance period, country
- Preset filters: 300%/6mo, 500%/12mo, custom
- `j`/`k` flip through names, `space` toggle zoom
- Pattern tags auto-detected: VCP, flat base, IPO base, power trend, stage-2 breakout
- Each chart: 90 days before move start → move → 90 days after

## Data sources

- EODHD All-In-One tier ($79.99/mo) — covers US/HK/TW/JP/KR with delisted + split-adj
- Chart rendering: TradingView Lightweight Charts (free JS lib, local)

Pull once, cancel subscription, dashboard runs fully offline forever.
