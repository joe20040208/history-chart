"""Pull 10yr daily OHLC (split-adjusted) for every ticker in the universe.

- Uses EODHD /eod/{SYMBOL}.{EXCHANGE} endpoint
- Parallel via threads, resumable: skips tickers already on disk
- Saves each ticker as parquet: data/ohlc/{COUNTRY}/{EXCHANGE}_{CODE}.parquet
- Columns: date, open, high, low, close, adjusted_close, volume
"""
from __future__ import annotations
import sys
import time
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import (
    EODHD_API_KEY, EODHD_BASE, EXCHANGES, UNIVERSE_DIR, OHLC_DIR, HISTORY_START,
)

SESSION = requests.Session()
MAX_WORKERS = 8  # EODHD all-in-one allows generous concurrency


def ticker_path(country: str, exchange: str, code: str) -> Path:
    safe = code.replace("/", "_").replace(".", "_")
    d = OHLC_DIR / country
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{exchange}_{safe}.parquet"


def fetch_one(country: str, exchange: str, code: str) -> tuple[str, str]:
    path = ticker_path(country, exchange, code)
    if path.exists() and path.stat().st_size > 0:
        return code, "skip"
    symbol = f"{code}.{exchange}"
    url = f"{EODHD_BASE}/eod/{symbol}"
    params = {
        "api_token": EODHD_API_KEY,
        "fmt": "json",
        "from": HISTORY_START,
        "period": "d",
    }
    for attempt in range(3):
        try:
            r = SESSION.get(url, params=params, timeout=30)
            if r.status_code == 404:
                return code, "404"
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            r.raise_for_status()
            data = r.json()
            if not data or not isinstance(data, list):
                return code, "empty"
            df = pd.DataFrame(data)
            if df.empty or len(df) < 20:
                return code, "short"
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            # Add dollar volume in local currency (USD-normalised later during screen)
            df["dollar_vol_local"] = df["adjusted_close"] * df["volume"]
            df.to_parquet(path, index=False)
            return code, "ok"
        except Exception as e:
            if attempt == 2:
                return code, f"err:{type(e).__name__}"
            time.sleep(2 * (attempt + 1))
    return code, "err"


def fetch_country(country: str) -> None:
    uni_path = UNIVERSE_DIR / f"{country}.parquet"
    if not uni_path.exists():
        print(f"[{country}] no universe file — run fetch_universe first")
        return
    uni = pd.read_parquet(uni_path)
    tasks = list(zip(uni["exchange"].tolist(), uni["Code"].tolist()))
    print(f"[{country}] {len(tasks):,} tickers to fetch")

    stats = {"ok": 0, "skip": 0, "404": 0, "empty": 0, "short": 0, "err": 0}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_one, country, ex_code, code): code
                for ex_code, code in tasks}
        for fut in tqdm(as_completed(futs), total=len(futs), desc=country):
            _, status = fut.result()
            bucket = status.split(":")[0] if ":" in status else status
            stats[bucket] = stats.get(bucket, 0) + 1
    print(f"[{country}] {stats}")


def main() -> None:
    if not EODHD_API_KEY:
        raise SystemExit("Set EODHD_API_KEY in .env")
    for country in EXCHANGES:
        fetch_country(country)


if __name__ == "__main__":
    main()
