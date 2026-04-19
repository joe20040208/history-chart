"""FastAPI server — reads runners.parquet + per-ticker OHLC and serves them.

Routes:
  GET /api/runners                — full list of events as JSON
  GET /api/ohlc/{country}/{exch}/{code}?from=YYYY-MM-DD&to=YYYY-MM-DD
  GET /                           — static dashboard

Run:  python -m dashboard.server
Open: http://localhost:8000
"""
from __future__ import annotations
import sys
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import DATA_DIR, OHLC_DIR

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


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


app.mount("/static", StaticFiles(directory=STATIC), name="static")


def main():
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)


if __name__ == "__main__":
    main()
