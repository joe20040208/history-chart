"""Pattern detection for the bar *leading into* each explosive move.

Given an OHLC DataFrame and the start-of-move index i, measure the
pre-move setup so we can classify the base/launch structure.

Exports `summarize_setup(df, i) -> dict` with these fields:

  pre_52w_high:           ratio of start_price to 52w high (0.0–1.0+)
  pre_consolidation_days: bars in tight range ending at i
  pre_atr_pct:            avg true range / price, 20d (tightness)
  pre_range_pct:          (high − low) / low in the N days before i
  pre_vol_contraction:    30d vol / 90d vol at i (< 1 = drying up)
  stage:                  1 (base), 2 (uptrend), 3 (top), 4 (downtrend) per Weinstein
  setup_tag:              one of: vcp | flat_base | ipo_base | power_trend |
                                  stage2_breakout | pocket_pivot | none

Thresholds are deliberately forgiving — the dashboard surfaces the raw
numbers so the user can filter and judge.
"""
from __future__ import annotations
import numpy as np
import pandas as pd


def _true_range(df: pd.DataFrame) -> pd.Series:
    high = df["high"]
    low = df["low"]
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr


def summarize_setup(df: pd.DataFrame, i: int, lookback: int = 60) -> dict:
    """Measure tightness/trend in the `lookback` bars before bar i."""
    start = max(0, i - lookback)
    pre = df.iloc[start:i + 1].copy()
    if len(pre) < 10:
        return _empty_setup()

    close_i = float(df["adjusted_close"].iloc[i])

    # 52w context
    lookback_252 = df.iloc[max(0, i - 252):i + 1]
    high_52w = float(lookback_252["high"].max()) if len(lookback_252) else close_i
    pre_52w_high = close_i / high_52w if high_52w > 0 else np.nan

    # Tightness: ATR% over last 20d
    tr = _true_range(pre)
    atr20 = tr.tail(20).mean()
    atr_pct = float(atr20 / close_i) if close_i > 0 else np.nan

    # Range over last `lookback` bars: high-to-low contraction
    hi = float(pre["high"].max())
    lo = float(pre["low"].min())
    range_pct = (hi / lo - 1.0) if lo > 0 else np.nan

    # Consolidation days: count of bars back from i where close stays within
    # ±8% of close_i
    consol = 0
    cutoff_hi, cutoff_lo = close_i * 1.08, close_i * 0.92
    for k in range(i, -1, -1):
        c = float(df["adjusted_close"].iloc[k])
        if cutoff_lo <= c <= cutoff_hi:
            consol += 1
        else:
            break

    # Volume contraction: 30d vs 90d avg volume
    v30 = df["volume"].iloc[max(0, i - 30):i].mean()
    v90 = df["volume"].iloc[max(0, i - 90):i].mean()
    vol_contraction = float(v30 / v90) if v90 and v90 > 0 else np.nan

    # Stage: 30w EMA slope
    ema30w = df["adjusted_close"].ewm(span=150, adjust=False).mean()
    if i > 20:
        slope = (ema30w.iloc[i] - ema30w.iloc[i - 20]) / max(ema30w.iloc[i - 20], 1e-9)
    else:
        slope = 0.0
    price_vs_ema = close_i / ema30w.iloc[i] if ema30w.iloc[i] else 1.0
    if slope > 0.02 and price_vs_ema >= 1.0:
        stage = 2
    elif slope < -0.02 and price_vs_ema <= 1.0:
        stage = 4
    elif slope >= -0.02 and slope <= 0.02 and price_vs_ema >= 0.95:
        stage = 1
    else:
        stage = 3

    # Setup tag — priority order
    ipo_age = i  # days since first bar in df
    tag = "none"
    if ipo_age < 60 and range_pct < 0.35 and pre_52w_high >= 0.85:
        tag = "ipo_base"
    elif consol >= 20 and atr_pct < 0.04 and vol_contraction < 0.8 and pre_52w_high >= 0.85:
        tag = "vcp"
    elif consol >= 15 and range_pct < 0.15 and pre_52w_high >= 0.90:
        tag = "flat_base"
    elif stage == 2 and pre_52w_high >= 0.95 and atr_pct < 0.05:
        tag = "stage2_breakout"
    elif stage == 2 and consol < 10 and pre_52w_high >= 0.95:
        tag = "power_trend"
    elif v30 > 1.4 * v90 and pre_52w_high >= 0.85:
        tag = "pocket_pivot"

    return {
        "pre_52w_high":           round(float(pre_52w_high), 3) if not np.isnan(pre_52w_high) else None,
        "pre_consolidation_days": int(consol),
        "pre_atr_pct":            round(float(atr_pct), 4) if not np.isnan(atr_pct) else None,
        "pre_range_pct":          round(float(range_pct), 3) if not np.isnan(range_pct) else None,
        "pre_vol_contraction":    round(float(vol_contraction), 3) if not np.isnan(vol_contraction) else None,
        "stage":                  int(stage),
        "setup_tag":              tag,
    }


def _empty_setup() -> dict:
    return {
        "pre_52w_high": None, "pre_consolidation_days": 0,
        "pre_atr_pct": None, "pre_range_pct": None, "pre_vol_contraction": None,
        "stage": 0, "setup_tag": "none",
    }
