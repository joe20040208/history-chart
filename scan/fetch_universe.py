"""Pull the full ticker list (active + delisted) for each target exchange.

Writes one parquet per exchange: data/universe/{EXCHANGE}.parquet
Columns: code, name, country, exchange, currency, type, isin, delisted
"""
from __future__ import annotations
import sys
import time
import requests
import pandas as pd
from tqdm import tqdm

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
from config import EODHD_API_KEY, EODHD_BASE, EXCHANGES, UNIVERSE_DIR


def fetch_symbols(exchange: str, delisted: int = 0) -> pd.DataFrame:
    url = f"{EODHD_BASE}/exchange-symbol-list/{exchange}"
    params = {"api_token": EODHD_API_KEY, "fmt": "json", "delisted": delisted}
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    df["exchange"] = exchange
    df["delisted"] = bool(delisted)
    return df


def main() -> None:
    if not EODHD_API_KEY:
        raise SystemExit("Set EODHD_API_KEY in .env")

    for country, codes in EXCHANGES.items():
        frames = []
        for ex in codes:
            for delisted in (0, 1):
                try:
                    df = fetch_symbols(ex, delisted)
                    frames.append(df)
                    print(f"  {ex} delisted={delisted}: {len(df):,} symbols")
                except Exception as e:
                    print(f"  [!] {ex} delisted={delisted} failed: {e}")
                time.sleep(0.2)

        if not frames:
            continue
        out = pd.concat(frames, ignore_index=True)
        # Keep only common stocks and ADRs — exclude ETFs, funds, warrants, preferred, etc.
        if "Type" in out.columns:
            keep = out["Type"].isin(["Common Stock", "ADR"])
            out = out[keep].copy()
        out = out.drop_duplicates(subset=["Code", "exchange"])
        out["country"] = country
        path = UNIVERSE_DIR / f"{country}.parquet"
        out.to_parquet(path, index=False)
        print(f"[{country}] saved {len(out):,} symbols → {path}")


if __name__ == "__main__":
    main()
