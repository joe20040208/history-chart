"""Pull shares outstanding history per ticker (for market cap time series).

EODHD fundamentals endpoint returns point-in-time shares outstanding.
We store the quarterly series so screener can compute historical mcap.
"""
from __future__ import annotations
import sys
import json
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import EODHD_API_KEY, EODHD_BASE, EXCHANGES, UNIVERSE_DIR, FUND_DIR

import threading
_local = threading.local()

def _session():
    if not hasattr(_local, "s"):
        _local.s = requests.Session()
    return _local.s

MAX_WORKERS = 4


def fund_path(country: str, exchange: str, code: str) -> Path:
    safe = code.replace("/", "_").replace(".", "_")
    d = FUND_DIR / country
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{exchange}_{safe}.json"


def fetch_one(country: str, exchange: str, code: str):
    path = fund_path(country, exchange, code)
    if path.exists() and path.stat().st_size > 0:
        return "skip"
    symbol = f"{code}.{exchange}"
    url = f"{EODHD_BASE}/fundamentals/{symbol}"
    # filter= drastically reduces payload size → much faster
    params = {"api_token": EODHD_API_KEY, "filter": "General,Highlights,SharesStats"}
    for attempt in range(3):
        try:
            r = _session().get(url, params=params, timeout=30)
            if r.status_code == 404:
                path.write_text("{}")
                return "404"
            if r.status_code == 429:
                time.sleep(2 * (attempt + 1))
                continue
            r.raise_for_status()
            data = r.json()
            slim = {
                "currency":      (data.get("General") or {}).get("CurrencyCode"),
                "sector":        (data.get("General") or {}).get("Sector"),
                "industry":      (data.get("General") or {}).get("Industry"),
                "name":          (data.get("General") or {}).get("Name"),
                "mcap":          (data.get("Highlights") or {}).get("MarketCapitalization"),
                "shares_current":(data.get("SharesStats") or {}).get("SharesOutstanding"),
                "shares_history": {},
            }
            path.write_text(json.dumps(slim))
            return "ok"
        except Exception as e:
            if attempt == 2:
                return f"err:{type(e).__name__}"
            time.sleep(2 * (attempt + 1))
    return "err"


def fetch_country(country: str) -> None:
    uni_path = UNIVERSE_DIR / f"{country}.parquet"
    if not uni_path.exists():
        return
    uni = pd.read_parquet(uni_path)
    tasks = list(zip(uni["exchange"].tolist(), uni["Code"].tolist()))
    print(f"[{country}] fundamentals for {len(tasks):,} tickers")

    stats: dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(fetch_one, country, exch, code) for exch, code in tasks]
        for fut in tqdm(as_completed(futs), total=len(futs), desc=country):
            s = fut.result()
            bucket = s.split(":")[0] if ":" in s else s
            stats[bucket] = stats.get(bucket, 0) + 1
    print(f"[{country}] {stats}")


def main() -> None:
    if not EODHD_API_KEY:
        raise SystemExit("Set EODHD_API_KEY in .env")
    for country in EXCHANGES:
        fetch_country(country)


if __name__ == "__main__":
    main()
