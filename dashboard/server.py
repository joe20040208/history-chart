"""FastAPI server — reads runners.parquet + per-ticker OHLC and serves them.

Routes:
  GET /api/runners                — full list of events as JSON
  GET /api/ohlc/{country}/{exch}/{code}?from=YYYY-MM-DD&to=YYYY-MM-DD
  GET /api/similar/{country}/{exch}/{code}?asof=YYYY-MM-DD
  GET /                           — static dashboard

Run:  python -m dashboard.server
Open: http://localhost:8000
"""
from __future__ import annotations
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import DATA_DIR, OHLC_DIR
from scan.build_pattern_index import WINDOW as PATTERN_WINDOW, encode_window
from scan.patterns import summarize_setup

ROOT = Path(__file__).parent
STATIC = ROOT / "static"

app = FastAPI(title="History Chart")


@app.get("/api/runners")
def runners():
    import json, math
    p = DATA_DIR / "runners.parquet"
    if not p.exists():
        raise HTTPException(404, "runners.parquet not built yet — run scan.screen")
    df = pd.read_parquet(p)
    records = df.to_dict(orient="records")
    # replace NaN/Inf with None so JSON serialisation doesn't crash
    # use try/except to catch both Python float and numpy.floating NaN/Inf
    def _safe(v):
        if isinstance(v, (str, bool, type(None))):
            return v
        try:
            if not math.isfinite(float(v)):
                return None
        except (TypeError, ValueError):
            pass
        return v

    clean = [{ k: _safe(v) for k, v in row.items() } for row in records]
    return JSONResponse(clean)


@app.get("/api/ohlc/{country}/{exchange}/{code}")
def ohlc(country: str, exchange: str, code: str,
         from_: str | None = None, to: str | None = None):
    safe = code.replace("/", "_").replace(".", "_")
    p = OHLC_DIR / country / f"{exchange}_{safe}.parquet"
    if not p.exists():
        raise HTTPException(404, f"no ohlc for {code}.{exchange}")
    df = pd.read_parquet(p)
    df["date"] = pd.to_datetime(df["date"])
    if from_:
        df = df[df["date"] >= pd.to_datetime(from_)]
    if to:
        df = df[df["date"] <= pd.to_datetime(to)]

    # Scale OHL to the same split-adjusted basis as adjusted_close.
    # The raw parquet stores unadjusted open/high/low/close alongside
    # adjusted_close — mixing them produces huge candle bodies that look
    # like histogram bars (especially across splits/reverse-splits).
    out = []
    for d, o, h, l, raw_c, adj_c, v in zip(
        df["date"], df["open"], df["high"], df["low"],
        df["close"], df["adjusted_close"], df["volume"]
    ):
        adj_c = float(adj_c)
        raw_c = float(raw_c)
        o, h, l, v = float(o), float(h), float(l), float(v)
        if adj_c <= 0:
            continue
        ratio = (adj_c / raw_c) if raw_c > 0 else 1.0
        o *= ratio; h *= ratio; l *= ratio
        if o <= 0: o = adj_c
        if h < max(o, adj_c): h = max(o, adj_c)
        if l <= 0 or l > min(o, adj_c): l = min(o, adj_c)
        out.append({
            "time": d.strftime("%Y-%m-%d"),
            "open": o, "high": h, "low": l,
            "close": adj_c, "volume": max(v, 0),
        })
    return JSONResponse(out)


# ──────────────── pattern similarity ────────────────

_pattern_bundle = None
_pattern_meta = None


def _load_patterns():
    global _pattern_bundle, _pattern_meta
    if _pattern_bundle is None:
        idx_path = DATA_DIR / "pattern_index.pkl"
        meta_path = DATA_DIR / "patterns.parquet"
        if not idx_path.exists() or not meta_path.exists():
            raise HTTPException(503, "pattern index not built — run scan.build_pattern_index")
        with open(idx_path, "rb") as f:
            _pattern_bundle = pickle.load(f)
        _pattern_meta = pd.read_parquet(meta_path)
    return _pattern_bundle, _pattern_meta


def _load_ohlc(country: str, exchange: str, code: str) -> pd.DataFrame:
    safe = code.replace("/", "_").replace(".", "_")
    p = OHLC_DIR / country / f"{exchange}_{safe}.parquet"
    if not p.exists():
        raise HTTPException(404, f"no ohlc for {code}.{exchange}")
    df = pd.read_parquet(p)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def _slice_for_chart(df: pd.DataFrame, anchor_idx: int,
                     pre: int = 60, post: int = 90) -> list[dict]:
    lo = max(0, anchor_idx - pre)
    hi = min(len(df), anchor_idx + post + 1)
    out = []
    for d, o, h, l, raw_c, adj_c, v in zip(
        df["date"].iloc[lo:hi], df["open"].iloc[lo:hi], df["high"].iloc[lo:hi],
        df["low"].iloc[lo:hi], df["close"].iloc[lo:hi],
        df["adjusted_close"].iloc[lo:hi], df["volume"].iloc[lo:hi]
    ):
        adj_c, raw_c = float(adj_c), float(raw_c)
        o, h, l, v = float(o), float(h), float(l), float(v)
        if adj_c <= 0:
            continue
        ratio = (adj_c / raw_c) if raw_c > 0 else 1.0
        o *= ratio; h *= ratio; l *= ratio
        if o <= 0: o = adj_c
        if h < max(o, adj_c): h = max(o, adj_c)
        if l <= 0 or l > min(o, adj_c): l = min(o, adj_c)
        out.append({
            "time": d.strftime("%Y-%m-%d"),
            "open": o, "high": h, "low": l,
            "close": adj_c, "volume": max(v, 0),
        })
    return out


@app.get("/api/similar/{country}/{exchange}/{code}")
def similar(country: str, exchange: str, code: str,
            asof: str | None = None, max_results: int = 30):
    """Find historical runners with similar pre-event chart shape.

    The query window is the 60 trading days ending on `asof` (or the most
    recent bar if omitted). We compute its setup_tag and only compare against
    historical events with the same tag.
    """
    bundle, meta = _load_patterns()
    df = _load_ohlc(country, exchange, code)
    if asof:
        anchor = int(np.searchsorted(df["date"].values, np.datetime64(pd.to_datetime(asof))))
        anchor = min(anchor, len(df) - 1)
    else:
        anchor = len(df) - 1
    if anchor < PATTERN_WINDOW:
        raise HTTPException(400, f"need at least {PATTERN_WINDOW} bars before asof")

    closes = df["adjusted_close"].to_numpy(dtype=float)[anchor - PATTERN_WINDOW:anchor]
    vols = df["volume"].to_numpy(dtype=float)[anchor - PATTERN_WINDOW:anchor]
    qvec = encode_window(closes, vols)
    if qvec is None:
        raise HTTPException(400, "query window is degenerate (flat price or non-finite values)")

    setup = summarize_setup(df, anchor, lookback=PATTERN_WINDOW)
    tag = setup["setup_tag"]
    if tag not in bundle["index_by_tag"]:
        return JSONResponse({"query_tag": tag, "matches": [], "note": f"no historical events with tag={tag}"})

    nn, idx_map, _ = bundle["index_by_tag"][tag]
    k = min(max_results, len(idx_map))
    dist, nbr = nn.kneighbors(qvec.reshape(1, -1), n_neighbors=k)

    # Convert distance → similarity in (0,1]; scale chosen so d≈10 gives ~0.4
    matches = []
    for d, j in zip(dist[0], nbr[0]):
        m = meta.iloc[idx_map[j]]
        # Skip exact self-match (same ticker + start_date roughly = today)
        if m.ticker == code and m.country == country:
            continue
        try:
            mdf = _load_ohlc(m.country, m.exchange, m.ticker)
        except HTTPException:
            continue
        m_anchor = int(np.searchsorted(mdf["date"].values, np.datetime64(pd.to_datetime(m.start_date))))
        if m_anchor < 1 or m_anchor >= len(mdf):
            continue
        matches.append({
            "ticker": m.ticker,
            "country": m.country,
            "exchange": m.exchange,
            "name": m.name,
            "start_date": m.start_date,
            "peak_date": m.peak_date,
            "pct_gain": float(m.pct_gain),
            "days_to_peak": int(m.days_to_peak),
            "post_90d_return": (None if m.post_90d_return is None or pd.isna(m.post_90d_return)
                                else float(m.post_90d_return)),
            "distance": float(d),
            "similarity": float(np.exp(-d / 8.0)),
            "bars": _slice_for_chart(mdf, m_anchor, pre=PATTERN_WINDOW, post=90),
        })

    return JSONResponse({
        "query": {"ticker": code, "country": country, "exchange": exchange,
                  "asof": df["date"].iloc[anchor].strftime("%Y-%m-%d"),
                  "setup_tag": tag},
        "matches": matches,
    })


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


app.mount("/static", StaticFiles(directory=STATIC), name="static")


def main():
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)


if __name__ == "__main__":
    main()
