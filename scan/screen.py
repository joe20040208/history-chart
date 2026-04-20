"""Screen all tickers for explosive moves and write runners.parquet.

For each ticker:
  - Find the highest rolling N-day return (low→high within window)
  - If >= pct_gain_threshold, record: start_date, end_date, pct_gain
  - Require market cap >= min_mcap and avg 30d $vol >= min_dollar_vol
    at start_date (both in USD)
  - Dedupe: one event per ticker per 12 months

Runs the "widest" screen (lowest bar) so the dashboard can re-filter locally
without re-reading all OHLC.

Output columns in runners.parquet:
  ticker, exchange, country, name, sector, industry, currency,
  start_date, peak_date, end_date, pct_gain, days_to_peak,
  start_price, peak_price, start_mcap_usd, start_dollar_vol_30d_usd,
  pre_52w_high, pre_consolidation_days, pre_atr_pct, pre_range_pct,
  post_90d_return, delisted
"""
from __future__ import annotations
import sys
import json
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import (
    EODHD_API_KEY, EODHD_BASE, EXCHANGES, UNIVERSE_DIR, OHLC_DIR, FUND_DIR,
    DATA_DIR, FX_PAIRS,
)
from scan.patterns import summarize_setup  # noqa: E402

# Widest screen — dashboard filters re-apply stricter criteria client-side
WIDEST_PCT = 150          # %
WIDEST_WINDOW = 252       # 1y — lets dashboard pick shorter windows from raw data
MIN_PRICE = 1.0           # USD equivalent
MAX_WINDOW_FOR_SCAN = 252 # we scan windows up to 1y; dashboard narrows this


def load_fx_series() -> dict[str, pd.Series]:
    """Return {country: daily USD-per-local Series}. 1.0 for US."""
    out: dict[str, pd.Series] = {}
    for country, pair in FX_PAIRS.items():
        if pair is None:
            out[country] = None  # marker for "multiply by 1"
            continue
        url = f"{EODHD_BASE}/eod/{pair}"
        params = {"api_token": EODHD_API_KEY, "fmt": "json", "from": "2015-01-01"}
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            df = pd.DataFrame(r.json())
            df["date"] = pd.to_datetime(df["date"])
            s = df.set_index("date")["adjusted_close"].sort_index()
            # EODHD FOREX quotes: HKD.FOREX = USD/HKD (i.e. 1 USD = x HKD). We want USD per local.
            out[country] = 1.0 / s
        except Exception as e:
            print(f"[fx] {country} {pair} failed: {e}; using 1.0")
            out[country] = None
    return out


def load_fundamentals(country: str, exchange: str, code: str) -> dict:
    safe = code.replace("/", "_").replace(".", "_")
    p = FUND_DIR / country / f"{exchange}_{safe}.json"
    if not p.exists() or p.stat().st_size < 3:
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def shares_at(fund: dict, dt: pd.Timestamp) -> float | None:
    """Best-effort point-in-time shares outstanding."""
    hist = fund.get("shares_history") or {}
    if hist:
        items = []
        for k, v in hist.items():
            try:
                d = pd.to_datetime(v.get("dateFormatted") or k)
                s = float(v.get("shares") or v.get("sharesMln") or 0)
                if s > 0:
                    items.append((d, s))
            except Exception:
                pass
        if items:
            items.sort()
            # use most recent entry on-or-before dt
            past = [s for d, s in items if d <= dt]
            if past:
                return past[-1]
            return items[0][1]
    return fund.get("shares_current")


def scan_ticker(df: pd.DataFrame, fund: dict, fx: pd.Series | None,
                country: str) -> list[dict]:
    """Find all qualifying explosive moves in this ticker's history."""
    if len(df) < 60:
        return []
    px = df["adjusted_close"].values
    lo = df["low"].values if "low" in df else px
    hi = df["high"].values if "high" in df else px
    vol = df["volume"].values
    dollar_vol_local = df["dollar_vol_local"].values
    dates = df["date"].values

    # 30d rolling avg dollar volume (local) and share volume
    s = pd.Series(dollar_vol_local)
    adv30 = s.rolling(30, min_periods=15).mean().values
    adv30_shares = pd.Series(vol).rolling(30, min_periods=15).mean().values

    # For each bar i, compute the max close within the next MAX_WINDOW_FOR_SCAN bars
    n = len(df)
    events: list[dict] = []
    i = 0
    while i < n - 30:
        start_p = px[i]
        if start_p < MIN_PRICE / (fx.iloc[fx.index.get_indexer([dates[i]], method="ffill")[0]] if fx is not None and len(fx) else 1):
            i += 1
            continue

        j_end = min(i + MAX_WINDOW_FOR_SCAN, n)
        window = px[i:j_end]
        if len(window) < 20:
            break
        peak_idx_rel = int(np.argmax(window))
        peak_p = float(window[peak_idx_rel])
        pct = (peak_p / start_p - 1) * 100
        if pct < WIDEST_PCT:
            i += 1
            continue

        peak_idx = i + peak_idx_rel
        # Compute USD mcap & ADV at start
        shares = shares_at(fund, pd.Timestamp(dates[i]))
        fx_val = 1.0
        if fx is not None:
            try:
                idx = fx.index.get_indexer([pd.Timestamp(dates[i])], method="ffill")[0]
                if idx >= 0:
                    fx_val = float(fx.iloc[idx])
            except Exception:
                fx_val = 1.0
        mcap_usd = (shares * start_p * fx_val) if shares else None
        adv_usd = (adv30[i] * fx_val) if not np.isnan(adv30[i]) else None
        adv_shares = float(adv30_shares[i]) if not np.isnan(adv30_shares[i]) else None

        # Post-move 90d return
        post_idx = min(peak_idx + 63, n - 1)
        post_return = (px[post_idx] / peak_p - 1) * 100 if peak_idx < n - 1 else None

        # Pre-move performance: 3M (63 days) and 6M (126 days) before start
        pre_3m_idx = max(i - 63, 0)
        pre_6m_idx = max(i - 126, 0)
        pre_perf_3m = (start_p / px[pre_3m_idx] - 1) * 100 if i >= 63 else None
        pre_perf_6m = (start_p / px[pre_6m_idx] - 1) * 100 if i >= 126 else None

        setup = summarize_setup(df, i)

        events.append({
            "start_date": pd.Timestamp(dates[i]).date().isoformat(),
            "peak_date":  pd.Timestamp(dates[peak_idx]).date().isoformat(),
            "pct_gain":   round(pct, 1),
            "days_to_peak": int(peak_idx - i),
            "start_price": round(float(start_p), 4),
            "peak_price":  round(float(peak_p), 4),
            "start_mcap_usd": float(mcap_usd) if mcap_usd else None,
            "start_dollar_vol_30d_usd": float(adv_usd) if adv_usd else None,
            "avg_vol_30d_shares": adv_shares,
            "post_90d_return": round(float(post_return), 1) if post_return is not None else None,
            "pre_perf_3m": round(float(pre_perf_3m), 1) if pre_perf_3m is not None else None,
            "pre_perf_6m": round(float(pre_perf_6m), 1) if pre_perf_6m is not None else None,
            **setup,
        })
        # Jump past peak so we don't redetect the same move
        i = peak_idx + 30
    return events


def main() -> None:
    fx_map = load_fx_series()
    all_rows: list[dict] = []

    for country in EXCHANGES:
        uni_path = UNIVERSE_DIR / f"{country}.parquet"
        if not uni_path.exists():
            print(f"[{country}] no universe, skip")
            continue
        uni = pd.read_parquet(uni_path)
        fx = fx_map.get(country)
        country_dir = OHLC_DIR / country

        # Only screen common stocks and ADRs — skip ETFs, funds, warrants, etc.
        if "Type" in uni.columns:
            uni = uni[uni["Type"].isin(["Common Stock", "ADR"])].copy()

        for _, row in tqdm(uni.iterrows(), total=len(uni), desc=country):
            code, exch = row["Code"], row["exchange"]
            safe = str(code).replace("/", "_").replace(".", "_")
            ohlc_path = country_dir / f"{exch}_{safe}.parquet"
            if not ohlc_path.exists():
                continue
            try:
                df = pd.read_parquet(ohlc_path)
            except Exception:
                continue
            fund = load_fundamentals(country, exch, code)
            events = scan_ticker(df, fund, fx, country)
            for ev in events:
                ev.update({
                    "ticker": code,
                    "exchange": exch,
                    "sub_exchange": row.get("Exchange"),  # e.g. NASDAQ, NYSE, AMEX
                    "country": country,
                    "name": fund.get("name") or row.get("Name"),
                    "sector": fund.get("sector"),
                    "industry": fund.get("industry"),
                    "currency": fund.get("currency") or row.get("Currency"),
                    "delisted": bool(row.get("delisted", False)),
                })
                all_rows.append(ev)

    if not all_rows:
        print("No events found.")
        return

    runners = pd.DataFrame(all_rows)
    # Sort: biggest first
    runners = runners.sort_values("pct_gain", ascending=False).reset_index(drop=True)
    out = DATA_DIR / "runners.parquet"
    runners.to_parquet(out, index=False)
    print(f"saved {len(runners):,} events → {out}")
    print(runners.head(20).to_string())


if __name__ == "__main__":
    main()
