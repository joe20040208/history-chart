"""Build a similarity index over runner pre-event chart shapes.

For each runner event in runners.parquet, take the 60 trading days
immediately before start_date and encode as a 120-dim vector:
  [normalized_adjusted_close (60d), normalized_log_volume (60d)]
Each half is z-normalized so different price/volume regimes are comparable.

Outputs:
  data/patterns.parquet   — metadata (one row per indexed event)
  data/pattern_index.pkl  — dict[setup_tag] -> (NearestNeighbors, np.ndarray of meta-row idx)

Run:  python -m scan.build_pattern_index
"""
from __future__ import annotations
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.neighbors import NearestNeighbors

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import DATA_DIR, OHLC_DIR

WINDOW = 60  # trading days of pre-event setup to encode

# Exclude low-quality OTC bulletin boards — their adjusted_close is unreliable
# (reverse-split artifacts, going-to-zero noise) and pollutes similarity results.
EXCLUDED_SUB_EXCHANGES = {
    "PINK", "OTCQB", "OTCQX", "OTCGREY", "OTCCE", "OTCMKTS", "OTCBB", "NMFQS"
}
MAX_PCT_GAIN = 2000  # drop obvious data errors (real winners rarely exceed this in 6m)


def encode_window(closes: np.ndarray, vols: np.ndarray) -> np.ndarray | None:
    """Return a 120-dim vector or None if the window is degenerate."""
    if len(closes) != WINDOW or len(vols) != WINDOW:
        return None
    if not np.all(np.isfinite(closes)) or closes[0] <= 0:
        return None

    # Price: percent change from window start, then z-norm
    px = closes / closes[0] - 1.0
    px_std = px.std()
    px = (px - px.mean()) / px_std if px_std > 1e-9 else px - px.mean()

    # Volume: log(1+v), then z-norm
    lv = np.log1p(np.clip(vols, 0, None))
    lv_std = lv.std()
    lv = (lv - lv.mean()) / lv_std if lv_std > 1e-9 else lv - lv.mean()

    vec = np.concatenate([px, lv]).astype(np.float32)
    if not np.all(np.isfinite(vec)):
        return None
    return vec


def main():
    runners = pd.read_parquet(DATA_DIR / "runners.parquet")
    runners["start_date"] = pd.to_datetime(runners["start_date"])
    n0 = len(runners)
    runners = runners[~runners["sub_exchange"].isin(EXCLUDED_SUB_EXCHANGES)]
    runners = runners[runners["pct_gain"] <= MAX_PCT_GAIN]
    print(f"Runners: {len(runners)} (filtered from {n0})")

    # Group by ticker so we open each OHLC parquet once
    grouped = runners.groupby(["country", "exchange", "ticker"], sort=False)
    print(f"Unique tickers: {grouped.ngroups}")

    rows = []
    vectors = []
    skipped = 0

    for (country, exch, ticker), g in grouped:
        safe = str(ticker).replace("/", "_").replace(".", "_")
        p = OHLC_DIR / country / f"{exch}_{safe}.parquet"
        if not p.exists():
            skipped += len(g)
            continue
        try:
            df = pd.read_parquet(p)
        except Exception:
            skipped += len(g)
            continue
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
        date_arr = df["date"].values
        close_arr = df["adjusted_close"].to_numpy(dtype=float)
        vol_arr = df["volume"].to_numpy(dtype=float)

        for _, ev in g.iterrows():
            sd = ev["start_date"]
            # Find first index whose date >= start_date
            idx = int(np.searchsorted(date_arr, np.datetime64(sd)))
            if idx < WINDOW:
                skipped += 1
                continue
            closes = close_arr[idx - WINDOW: idx]
            vols = vol_arr[idx - WINDOW: idx]
            vec = encode_window(closes, vols)
            if vec is None:
                skipped += 1
                continue
            vectors.append(vec)
            rows.append({
                "ticker": ticker,
                "country": country,
                "exchange": exch,
                "sub_exchange": ev.get("sub_exchange"),
                "name": ev.get("name"),
                "start_date": sd.strftime("%Y-%m-%d"),
                "peak_date": pd.to_datetime(ev["peak_date"]).strftime("%Y-%m-%d"),
                "pct_gain": float(ev["pct_gain"]),
                "days_to_peak": int(ev["days_to_peak"]),
                "post_90d_return": float(ev["post_90d_return"]) if pd.notna(ev["post_90d_return"]) else None,
                "setup_tag": ev.get("setup_tag") or "none",
            })

    print(f"Indexed: {len(rows)}, skipped: {skipped}")
    meta = pd.DataFrame(rows)
    X = np.vstack(vectors).astype(np.float32)
    print(f"Vectors shape: {X.shape}")

    meta.to_parquet(DATA_DIR / "patterns.parquet", index=False)
    print(f"Wrote {DATA_DIR / 'patterns.parquet'}")

    # Per-tag NearestNeighbors so Option B queries are O(1) dispatch
    index_by_tag: dict[str, tuple[NearestNeighbors, np.ndarray, np.ndarray]] = {}
    for tag, g in meta.groupby("setup_tag"):
        idx = g.index.to_numpy()
        sub = X[idx]
        nn = NearestNeighbors(metric="euclidean", algorithm="auto")
        nn.fit(sub)
        index_by_tag[tag] = (nn, idx, sub)
        print(f"  tag={tag:18s} n={len(idx)}")

    with open(DATA_DIR / "pattern_index.pkl", "wb") as f:
        pickle.dump({"window": WINDOW, "index_by_tag": index_by_tag}, f)
    print(f"Wrote {DATA_DIR / 'pattern_index.pkl'}")


if __name__ == "__main__":
    main()
