"""Config: exchange codes, screen defaults, paths."""
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"
OHLC_DIR = DATA_DIR / "ohlc"
UNIVERSE_DIR = DATA_DIR / "universe"
FUND_DIR = DATA_DIR / "fundamentals"
for d in (DATA_DIR, OHLC_DIR, UNIVERSE_DIR, FUND_DIR):
    d.mkdir(parents=True, exist_ok=True)

EODHD_API_KEY = os.getenv("EODHD_API_KEY", "")
EODHD_BASE = "https://eodhd.com/api"

# EODHD exchange codes for our five scopes.
# Each key is a user-facing country label, value is list of EODHD exchange codes.
EXCHANGES = {
    "US":     ["US"],              # NYSE + NASDAQ + AMEX lumped under US
    "HK":     ["HK"],
    "TW":     ["TW", "TWO"],       # TWSE + TPEx (OTC / GreTai)

    "KR":     ["KO", "KQ"],        # KOSPI + KOSDAQ
}

# Default screen thresholds — fully adjustable in dashboard
DEFAULTS = {
    "min_pct_gain":    300,        # %
    "period_days":     126,        # ~6 trading months
    "min_mcap_usd":    1_000_000_000,
    "min_avg_dollar_vol_30d": 5_000_000,
    "history_start":   "2015-01-01",
}

# When we pull EOD, how far back
HISTORY_START = "2015-01-01"

# FX rates to USD for market cap normalisation (refreshed each run via EODHD)
FX_PAIRS = {
    "HK":  "HKD.FOREX",
    "TW":  "TWD.FOREX",
    "KR":  "KRW.FOREX",

    "US":  None,
}
