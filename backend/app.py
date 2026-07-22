"""
NSE Index Analytics — backend (v2)
-----------------------------------
Adds, on top of the original history endpoint:

  /api/history            now takes explicit from/to dates (was years-only)
  /api/constituents       raw constituent list for one index
  /api/sector-composition industry breakdown for one or more indices
  /api/top-companies      top-N constituents by market cap, with 1Y growth
                          and revenue, for one or more indices

Run:
    pip install -r requirements.txt --break-system-packages
    uvicorn app:app --reload --port 8000
"""

import csv
import io
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Optional

import requests
import yfinance as yf
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="NSE Index Analytics API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
INDEX_MAP = {
    "NIFTY 50":            "^NSEI",
    "NIFTY NEXT 50":       "^NSMIDCP",
    "NIFTY 100":           "^CNX100",
    "NIFTY 200":           "^CNX200",
    "NIFTY 500":           "^CRSLDX",
    "NIFTY MIDCAP 50":     "^NSEMDCP50",
    "NIFTY MIDCAP 100":    "NIFTY_MIDCAP_100.NS",
    "NIFTY MIDCAP 150":    "NIFTYMIDCAP150.NS",
    "NIFTY MIDCAP SELECT": None,
    "NIFTY SMALLCAP 50":   "NIFTYSMLCAP50.NS",
    "NIFTY SMALLCAP 100":  "^CNXSC",
    "NIFTY SMALLCAP 250":  "NIFTYSMLCAP250.NS",
    "NIFTY SMALLCAP 500":  None,
    "NIFTY LARGEMIDCAP 250": None,
    "NIFTY MIDSMALLCAP 400": None,
    "NIFTY MIDSMALLCAP400 50:50": None,
    "NIFTY MICROCAP 250":  None,
    "NIFTY TOTAL MARKET":  None,
    "NIFTY INDIA FPI 150": None,
    "NIFTY500 LARGEMIDSMALL EQUAL-CAP WEIGHTED": None,
    "NIFTY500 MULTICAP 50:25:25": None,
}

BENCHMARK_NAME = "NIFTY 50"

# Best-effort mapping to niftyindices.com's public constituent-list CSVs.
# These carry Company Name / Industry / Symbol — used for sector composition
# and to identify each index's constituent stocks for the top-companies
# feature. Not every niche index has a confirmed, stable URL; those are
# attempted anyway and simply come back "unavailable" if the guess is wrong
# rather than being silently skipped, matching how the rest of this API
# already handles best-effort sources.
CONSTITUENT_CSV_MAP = {
    "NIFTY 50":            "https://niftyindices.com/IndexConstituent/ind_nifty50list.csv",
    "NIFTY NEXT 50":       "https://niftyindices.com/IndexConstituent/ind_niftynext50list.csv",
    "NIFTY 100":           "https://niftyindices.com/IndexConstituent/ind_nifty100list.csv",
    "NIFTY 200":           "https://niftyindices.com/IndexConstituent/ind_nifty200list.csv",
    "NIFTY 500":           "https://niftyindices.com/IndexConstituent/ind_nifty500list.csv",
    "NIFTY MIDCAP 50":     "https://niftyindices.com/IndexConstituent/ind_niftymidcap50list.csv",
    "NIFTY MIDCAP 100":    "https://niftyindices.com/IndexConstituent/ind_niftymidcap100list.csv",
    "NIFTY MIDCAP 150":    "https://niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv",
    "NIFTY MIDCAP SELECT": "https://niftyindices.com/IndexConstituent/ind_niftymidcapselect_list.csv",
    "NIFTY SMALLCAP 50":   "https://niftyindices.com/IndexConstituent/ind_niftysmallcap50list.csv",
    "NIFTY SMALLCAP 100":  "https://niftyindices.com/IndexConstituent/ind_niftysmallcap100list.csv",
    "NIFTY SMALLCAP 250":  "https://niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv",
    "NIFTY SMALLCAP 500":  "https://niftyindices.com/IndexConstituent/ind_niftysmallcap500list.csv",
    "NIFTY LARGEMIDCAP 250": "https://niftyindices.com/IndexConstituent/ind_niftylargemidcap250list.csv",
    "NIFTY MIDSMALLCAP 400": "https://niftyindices.com/IndexConstituent/ind_niftymidsmallcap400list.csv",
    "NIFTY MIDSMALLCAP400 50:50": None,
    "NIFTY MICROCAP 250":  "https://niftyindices.com/IndexConstituent/ind_niftymicrocap250_list.csv",
    "NIFTY TOTAL MARKET":  "https://niftyindices.com/IndexConstituent/ind_niftytotalmarket_list.csv",
    "NIFTY INDIA FPI 150": None,
    "NIFTY500 LARGEMIDSMALL EQUAL-CAP WEIGHTED": None,
    "NIFTY500 MULTICAP 50:25:25": None,
}

HISTORY_CACHE_TTL = 12 * 3600
FUNDAMENTALS_CACHE_TTL = 24 * 3600
CONSTITUENTS_CACHE_TTL = 24 * 3600

_history_cache: dict[str, dict] = {}
_constituents_cache: dict[str, dict] = {}
_fundamentals_cache: dict[str, dict] = {}


def _get(cache: dict, key: str, ttl: int):
    hit = cache.get(key)
    if hit and (time.time() - hit["ts"]) < ttl:
        return hit["data"]
    return None


def _set(cache: dict, key: str, data):
    cache[key] = {"ts": time.time(), "data": data}


# ---------------------------------------------------------------------------
# Yahoo Finance — history, one call per symbol, run concurrently
# ---------------------------------------------------------------------------
def _fetch_one_yahoo_history(name: str, symbol: str, start: str, end: str) -> tuple[str, dict]:
    try:
        hist = yf.Ticker(symbol).history(start=start, end=end, interval="1d", auto_adjust=False)
        closes = hist["Close"].dropna()
        if len(closes) < 2:
            raise ValueError(f"too few points returned ({len(closes)})")
        return name, {
            "dates": [d.strftime("%Y-%m-%d") for d in closes.index],
            "closes": [float(v) for v in closes.tolist()],
            "source": "yahoo",
        }
    except Exception as e:
        return name, {"error": f"yahoo: {e}"}


def fetch_yahoo_history_batch(name_to_symbol: dict[str, str], start: str, end: str) -> dict:
    if not name_to_symbol:
        return {}
    out = {}
    with ThreadPoolExecutor(max_workers=min(8, len(name_to_symbol))) as pool:
        futures = [pool.submit(_fetch_one_yahoo_history, name, sym, start, end) for name, sym in name_to_symbol.items()]
        for fut in as_completed(futures):
            name, data = fut.result()
            out[name] = data
    return out


# ---------------------------------------------------------------------------
# NSE fallback — real session so cookies persist
# ---------------------------------------------------------------------------
_nse_session: Optional[requests.Session] = None


def get_nse_session() -> requests.Session:
    global _nse_session
    if _nse_session is not None:
        return _nse_session
    s = requests.Session()
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0 Safari/537.36"),
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nseindia.com/",
        "Accept-Language": "en-US,en;q=0.9",
    })
    s.get("https://www.nseindia.com", timeout=8)
    _nse_session = s
    return s


def fetch_nse_history(index_name: str, start: str, end: str) -> dict:
    try:
        session = get_nse_session()
        from_d = datetime.strptime(start, "%Y-%m-%d")
        to_d = datetime.strptime(end, "%Y-%m-%d")
        resp = session.get(
            "https://www.nseindia.com/api/historical/indicesHistory",
            params={
                "indexType": index_name,
                "from": from_d.strftime("%d-%m-%Y"),
                "to": to_d.strftime("%d-%m-%Y"),
            },
            timeout=12,
        )
        resp.raise_for_status()
        rows = resp.json()["data"]["indexCloseOnlineRecords"]
        if not rows:
            raise ValueError("no rows returned")
        return {
            "dates": [r["EOD_TIMESTAMP"][:10] for r in rows],
            "closes": [float(r["EOD_CLOSE_INDEX_VAL"]) for r in rows],
            "source": "nse",
        }
    except Exception as e:
        global _nse_session
        _nse_session = None
        return {"error": f"nse: {e}"}


# ---------------------------------------------------------------------------
# Routes — history
# ---------------------------------------------------------------------------
@app.get("/api/indices")
def list_indices():
    return {"indices": [{"name": n, "hasYahoo": bool(s)} for n, s in INDEX_MAP.items()]}


@app.get("/api/history")
def get_history(
    indices: str = Query(..., description="Comma-separated index display names"),
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
):
    names = [n.strip() for n in indices.split(",") if n.strip()]
    out: dict[str, dict] = {}
    to_fetch_yahoo: dict[str, str] = {}
    to_fetch_nse: list[str] = []

    for name in names:
        cache_key = f"{name}:{start}:{end}"
        cached = _get(_history_cache, cache_key, HISTORY_CACHE_TTL)
        if cached:
            out[name] = cached
            continue
        symbol = INDEX_MAP.get(name)
        if symbol:
            to_fetch_yahoo[name] = symbol
        else:
            to_fetch_nse.append(name)

    if to_fetch_yahoo:
        yahoo_results = fetch_yahoo_history_batch(to_fetch_yahoo, start, end)
        for name, data in yahoo_results.items():
            out[name] = data
            if "error" not in data:
                _set(_history_cache, f"{name}:{start}:{end}", data)

    for name in to_fetch_nse:
        data = fetch_nse_history(name, start, end)
        out[name] = data
        if "error" not in data:
            _set(_history_cache, f"{name}:{start}:{end}", data)

    return out


# ---------------------------------------------------------------------------
# Constituents / sector composition
# ---------------------------------------------------------------------------
def fetch_constituents(index_name: str) -> dict:
    """Returns {"rows": [{"symbol","company","industry"}], "error": str|None}"""
    cached = _get(_constituents_cache, index_name, CONSTITUENTS_CACHE_TTL)
    if cached:
        return cached

    url = CONSTITUENT_CSV_MAP.get(index_name)
    if not url:
        result = {"rows": [], "error": "no known constituent list for this index"}
        return result

    try:
        resp = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        resp.raise_for_status()
        text = resp.content.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        fieldnames = reader.fieldnames or []

        def find_col(*keywords):
            for f in fieldnames:
                low = f.lower()
                if any(k in low for k in keywords):
                    return f
            return None

        col_company = find_col("company")
        col_industry = find_col("industry", "sector")
        col_symbol = find_col("symbol")

        rows = []
        for r in reader:
            rows.append({
                "company": (r.get(col_company) or "").strip() if col_company else "",
                "industry": (r.get(col_industry) or "Unclassified").strip() if col_industry else "Unclassified",
                "symbol": (r.get(col_symbol) or "").strip() if col_symbol else "",
            })
        rows = [r for r in rows if r["symbol"]]
        if not rows:
            raise ValueError("CSV parsed but no usable rows (unexpected column layout)")

        result = {"rows": rows, "error": None}
        _set(_constituents_cache, index_name, result)
        return result
    except Exception as e:
        return {"rows": [], "error": str(e)}


@app.get("/api/constituents")
def get_constituents(index: str = Query(...)):
    return fetch_constituents(index)


@app.get("/api/sector-composition")
def get_sector_composition(indices: str = Query(...)):
    names = [n.strip() for n in indices.split(",") if n.strip()]
    out = {}
    for name in names:
        data = fetch_constituents(name)
        if data["error"]:
            out[name] = {"error": data["error"]}
            continue
        rows = data["rows"]
        total = len(rows)
        counts: dict[str, int] = {}
        for r in rows:
            counts[r["industry"]] = counts.get(r["industry"], 0) + 1
        breakdown = [
            {"sector": sector, "pct": round(100 * count / total, 1), "count": count}
            for sector, count in sorted(counts.items(), key=lambda kv: -kv[1])
        ]
        out[name] = {"breakdown": breakdown, "totalConstituents": total}
    return {
        "results": out,
        "methodology": (
            "Percentage of constituent COMPANIES per sector, not free-float "
            "market-cap weighting — NSE's free constituent lists don't publish "
            "per-stock weights, only membership. Official factsheets on "
            "niftyindices.com show true weighted sector splits if exact figures "
            "are needed."
        ),
    }


# ---------------------------------------------------------------------------
# Top companies — market cap rank, 1Y growth, revenue
# ---------------------------------------------------------------------------
def _fetch_one_fundamentals(symbol: str) -> tuple[str, dict]:
    cached = _get(_fundamentals_cache, symbol, FUNDAMENTALS_CACHE_TTL)
    if cached:
        return symbol, cached
    try:
        t = yf.Ticker(symbol)
        fast = t.fast_info
        try:
            market_cap = fast["market_cap"]
        except Exception:
            market_cap = getattr(fast, "market_cap", None)
        hist = t.history(period="1y", interval="1d", auto_adjust=False)["Close"].dropna()
        growth_1y = float(hist.iloc[-1] / hist.iloc[0] - 1) if len(hist) >= 2 else None

        revenue = None
        try:
            info = t.info
            revenue = info.get("totalRevenue")
        except Exception:
            pass

        data = {
            "marketCap": float(market_cap) if market_cap else None,
            "growth1y": growth_1y,
            "revenue": float(revenue) if revenue else None,
        }
        _set(_fundamentals_cache, symbol, data)
        return symbol, data
    except Exception as e:
        return symbol, {"error": str(e)}


@app.get("/api/top-companies")
def get_top_companies(indices: str = Query(...), n: int = Query(10, ge=1, le=25)):
    names = [n2.strip() for n2 in indices.split(",") if n2.strip()]
    out = {}

    per_index_rows = {}
    all_symbols: set[str] = set()
    for name in names:
        data = fetch_constituents(name)
        if data["error"]:
            out[name] = {"error": data["error"]}
            continue
        per_index_rows[name] = data["rows"]
        for r in data["rows"]:
            all_symbols.add(r["symbol"])

    # cap total unique symbols fetched in one request to keep response time
    # reasonable on a free-tier deployment
    symbols_to_fetch = list(all_symbols)[:80]
    yahoo_symbols = {s: f"{s}.NS" for s in symbols_to_fetch}

    fundamentals: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = [pool.submit(_fetch_one_fundamentals, ysym) for ysym in yahoo_symbols.values()]
        for fut in as_completed(futures):
            ysym, data = fut.result()
            base_symbol = ysym[:-3] if ysym.endswith(".NS") else ysym
            fundamentals[base_symbol] = data

    for name, rows in per_index_rows.items():
        enriched = []
        for r in rows:
            f = fundamentals.get(r["symbol"])
            if not f or "error" in f:
                continue
            enriched.append({
                "symbol": r["symbol"],
                "company": r["company"],
                "marketCap": f.get("marketCap"),
                "growth1y": f.get("growth1y"),
                "revenue": f.get("revenue"),
            })
        enriched = [e for e in enriched if e["marketCap"]]
        enriched.sort(key=lambda e: -e["marketCap"])
        out[name] = {"companies": enriched[:n]}

    return {
        "results": out,
        "methodology": (
            "Ranked by market capitalisation (free yfinance data). Revenue is "
            "shown as 'N/A' where the free source doesn't expose it for that "
            "company — this is a data-availability gap, not a zero."
        ),
    }


@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "history_cached": len(_history_cache),
        "constituents_cached": len(_constituents_cache),
        "fundamentals_cached": len(_fundamentals_cache),
    }
