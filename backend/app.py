"""
NSE Index Analytics — backend (v3)
-----------------------------------
Changes from v2:
  - Constituent CSV fetch now tries two known free sources (niftyindices.com,
    then archives.nseindia.com) instead of one, since cloud-hosted IPs
    (Render, etc.) get blocked by one host more often than a home IP would.
  - /api/top-companies rewritten as two phases: a cheap market-cap-only call
    across ALL constituents for ranking, then the expensive 1Y-growth +
    revenue calls only for the actual top N. This is the fix for it being
    slow/unreliable on a free-tier deploy — the old version did the
    expensive fetch for up to 80 stocks; this does it for ~10.
  - New /api/agent/chat: a small tool-calling financial agent (via Groq's
    free API) that answers questions using the real functions above rather
    than letting the model guess numbers.

Run:
    pip install -r requirements.txt --break-system-packages
    export GROQ_API_KEY=your_key_here     # from console.groq.com, free
    uvicorn app:app --reload --port 8000
"""

import csv
import io
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Optional

import requests
import yfinance as yf
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

# Load a local .env file if one exists (python-dotenv). This only matters
# for local development — on Render (or any real deploy), environment
# variables come from the platform's own dashboard/config instead, and a
# .env file wouldn't be deployed anyway. Safe to leave in for both cases:
# does nothing if no .env file is present.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

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

# Two independent free sources for constituent lists, tried in order. A
# best-effort slug guess for each — wrong guesses just come back
# "unavailable" rather than breaking anything, same pattern as the rest of
# this API.
def _niftyindices_url(slug: str) -> str:
    return f"https://niftyindices.com/IndexConstituent/{slug}.csv"

def _archives_url(slug: str) -> str:
    return f"https://archives.nseindia.com/content/indices/{slug}.csv"

CONSTITUENT_SLUGS = {
    "NIFTY 50":            "ind_nifty50list",
    "NIFTY NEXT 50":       "ind_niftynext50list",
    "NIFTY 100":           "ind_nifty100list",
    "NIFTY 200":           "ind_nifty200list",
    "NIFTY 500":           "ind_nifty500list",
    "NIFTY MIDCAP 50":     "ind_niftymidcap50list",
    "NIFTY MIDCAP 100":    "ind_niftymidcap100list",
    "NIFTY MIDCAP 150":    "ind_niftymidcap150list",
    "NIFTY MIDCAP SELECT": "ind_niftymidcapselect_list",
    "NIFTY SMALLCAP 50":   "ind_niftysmallcap50list",
    "NIFTY SMALLCAP 100":  "ind_niftysmallcap100list",
    "NIFTY SMALLCAP 250":  "ind_niftysmallcap250list",
    "NIFTY SMALLCAP 500":  "ind_niftysmallcap500list",
    "NIFTY LARGEMIDCAP 250": "ind_niftylargemidcap250list",
    "NIFTY MIDSMALLCAP 400": "ind_niftymidsmallcap400list",
    "NIFTY MIDSMALLCAP400 50:50": None,
    "NIFTY MICROCAP 250":  "ind_niftymicrocap250_list",
    "NIFTY TOTAL MARKET":  "ind_niftytotalmarket_list",
    "NIFTY INDIA FPI 150": None,
    "NIFTY500 LARGEMIDSMALL EQUAL-CAP WEIGHTED": None,
    "NIFTY500 MULTICAP 50:25:25": None,
}

BROWSER_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "text/csv,application/csv,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://niftyindices.com/",
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
        **BROWSER_HEADERS,
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nseindia.com/",
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
def _parse_constituent_csv(text: str) -> list[dict]:
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
    return [r for r in rows if r["symbol"]]


def fetch_constituents(index_name: str) -> dict:
    """Returns {"rows": [...], "error": str|None}. Tries two independent
    free sources in order before giving up."""
    cached = _get(_constituents_cache, index_name, CONSTITUENTS_CACHE_TTL)
    if cached:
        return cached

    slug = CONSTITUENT_SLUGS.get(index_name)
    if not slug:
        return {"rows": [], "error": "no known constituent list for this index"}

    errors = []
    for url in (_niftyindices_url(slug), _archives_url(slug)):
        try:
            resp = requests.get(url, timeout=10, headers=BROWSER_HEADERS, allow_redirects=True)
            resp.raise_for_status()
            ctype = resp.headers.get("content-type", "")
            if "html" in ctype.lower():
                raise ValueError(f"got an HTML page instead of a CSV from {url} (likely blocked/redirected)")
            text = resp.content.decode("utf-8-sig", errors="replace")
            rows = _parse_constituent_csv(text)
            if not rows:
                raise ValueError("CSV parsed but no usable rows (unexpected column layout)")
            result = {"rows": rows, "error": None}
            _set(_constituents_cache, index_name, result)
            return result
        except Exception as e:
            errors.append(f"{url} -> {e}")
            continue

    return {"rows": [], "error": " | ".join(errors)}


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
# Top companies — two-phase: cheap market-cap rank first, then expensive
# growth/revenue calls only for the winners.
# ---------------------------------------------------------------------------
def _fetch_market_cap_only(symbol: str) -> tuple[str, Optional[float]]:
    try:
        fast = yf.Ticker(symbol).fast_info
        try:
            mc = fast["market_cap"]
        except Exception:
            mc = getattr(fast, "market_cap", None)
        return symbol, (float(mc) if mc else None)
    except Exception:
        return symbol, None


def _fetch_growth_and_revenue(symbol: str) -> tuple[str, dict]:
    cached = _get(_fundamentals_cache, symbol, FUNDAMENTALS_CACHE_TTL)
    if cached:
        return symbol, cached
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="1y", interval="1d", auto_adjust=False)["Close"].dropna()
        growth_1y = float(hist.iloc[-1] / hist.iloc[0] - 1) if len(hist) >= 2 else None
        revenue = None
        try:
            revenue = t.info.get("totalRevenue")
        except Exception:
            pass
        data = {"growth1y": growth_1y, "revenue": float(revenue) if revenue else None}
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

    # Phase 1: cheap market-cap-only lookup across every constituent, so we
    # can rank BEFORE paying for the expensive calls.
    symbols_list = list(all_symbols)[:150]  # sanity cap even for the cheap phase
    market_caps: dict[str, Optional[float]] = {}
    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = [pool.submit(_fetch_market_cap_only, f"{s}.NS") for s in symbols_list]
        for fut in as_completed(futures):
            ysym, mc = fut.result()
            base = ysym[:-3] if ysym.endswith(".NS") else ysym
            market_caps[base] = mc

    # Rank each index's constituents by market cap, keep top N symbols only
    top_symbols_per_index: dict[str, list[dict]] = {}
    winners: set[str] = set()
    for name, rows in per_index_rows.items():
        ranked = [r for r in rows if market_caps.get(r["symbol"])]
        ranked.sort(key=lambda r: -market_caps[r["symbol"]])
        top = ranked[:n]
        top_symbols_per_index[name] = top
        winners.update(r["symbol"] for r in top)

    # Phase 2: expensive growth + revenue calls, only for the winners
    fundamentals: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(_fetch_growth_and_revenue, f"{s}.NS") for s in winners]
        for fut in as_completed(futures):
            ysym, data = fut.result()
            base = ysym[:-3] if ysym.endswith(".NS") else ysym
            fundamentals[base] = data

    for name, top in top_symbols_per_index.items():
        companies = []
        for r in top:
            f = fundamentals.get(r["symbol"], {})
            companies.append({
                "symbol": r["symbol"],
                "company": r["company"],
                "marketCap": market_caps.get(r["symbol"]),
                "growth1y": f.get("growth1y"),
                "revenue": f.get("revenue"),
            })
        out[name] = {"companies": companies}

    return {
        "results": out,
        "methodology": (
            "Ranked by market capitalisation (free yfinance data, phase 1). "
            "Growth and revenue are then fetched only for the top constituents "
            "(phase 2) to keep this fast and reliable on a free-tier deploy. "
            "Revenue shows as 'N/A' where the free source doesn't expose it — "
            "a data-availability gap, not a zero."
        ),
    }


# ---------------------------------------------------------------------------
# Financial AI agent — Groq (free tier) tool-calling agent that answers
# questions using the real functions above instead of guessing numbers.
# ---------------------------------------------------------------------------
_groq_client = None

def get_groq_client():
    global _groq_client
    if _groq_client is None:
        from groq import Groq
        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            raise RuntimeError("GROQ_API_KEY environment variable is not set")
        _groq_client = Groq(api_key=api_key)
    return _groq_client


def agent_tool_get_metrics(index_name: str, years: int = 5) -> dict:
    symbol = INDEX_MAP.get(index_name.upper())
    if not symbol:
        return {"error": f"no Yahoo symbol known for '{index_name}'"}
    end = datetime.today()
    start = end - timedelta(days=365 * years)
    hist = fetch_yahoo_history_batch(
        {index_name: symbol}, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
    )
    data = hist.get(index_name, {})
    if "error" in data:
        return {"error": data["error"]}
    closes = data["closes"]
    years_actual = len(data["dates"]) / 252  # approx trading years
    cagr = (closes[-1] / closes[0]) ** (1 / max(years_actual, 0.1)) - 1
    return {
        "index": index_name,
        "cagr_pct": round(cagr * 100, 2),
        "years_requested": years,
        "start_value": round(closes[0], 2),
        "end_value": round(closes[-1], 2),
    }


def agent_tool_get_sector_breakdown(index_name: str) -> dict:
    data = fetch_constituents(index_name.upper())
    if data["error"]:
        return {"error": data["error"]}
    rows = data["rows"]
    total = len(rows)
    counts: dict[str, int] = {}
    for r in rows:
        counts[r["industry"]] = counts.get(r["industry"], 0) + 1
    breakdown = sorted(
        [{"sector": s, "pct": round(100 * c / total, 1)} for s, c in counts.items()],
        key=lambda d: -d["pct"],
    )
    return {"index": index_name, "sector_breakdown": breakdown[:8]}


def agent_tool_get_top_companies(index_name: str) -> dict:
    result = get_top_companies(indices=index_name.upper(), n=5)
    return result["results"].get(index_name.upper(), {"error": "not found"})


AGENT_FUNCTIONS = {
    "get_metrics": agent_tool_get_metrics,
    "get_sector_breakdown": agent_tool_get_sector_breakdown,
    "get_top_companies": agent_tool_get_top_companies,
}

AGENT_TOOLS = [
    {"type": "function", "function": {
        "name": "get_metrics",
        "description": "Get CAGR (annualised growth) for an NSE index over the last N years, computed from real historical data.",
        "parameters": {"type": "object", "properties": {
            "index_name": {"type": "string", "description": "e.g. 'NIFTY 50', 'NIFTY MIDCAP 100'"},
            "years": {"type": "integer", "description": "lookback window in years, default 5"},
        }, "required": ["index_name"]},
    }},
    {"type": "function", "function": {
        "name": "get_sector_breakdown",
        "description": "Get the sector/industry composition of an NSE index by percentage of constituent companies.",
        "parameters": {"type": "object", "properties": {
            "index_name": {"type": "string"},
        }, "required": ["index_name"]},
    }},
    {"type": "function", "function": {
        "name": "get_top_companies",
        "description": "Get the top constituent companies of an NSE index ranked by market cap, with 1-year growth and revenue.",
        "parameters": {"type": "object", "properties": {
            "index_name": {"type": "string"},
        }, "required": ["index_name"]},
    }},
]

AGENT_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
AGENT_SYSTEM_PROMPT = (
    "You are a financial analyst assistant for NSE (Indian stock exchange) broad-market "
    "indices. Always call a tool to get real numbers before answering a question that "
    "needs one — never invent a figure. Index names must be one of: " +
    ", ".join(INDEX_MAP.keys()) +
    ". Keep answers concise and note this isn't investment advice when relevant."
)


@app.post("/api/agent/chat")
def agent_chat(payload: dict):
    question = (payload or {}).get("question", "").strip()
    if not question:
        return {"error": "empty question"}

    try:
        client = get_groq_client()
    except Exception as e:
        return {"error": str(e)}

    messages = [
        {"role": "system", "content": AGENT_SYSTEM_PROMPT},
        {"role": "user", "content": question},
    ]

    try:
        for _ in range(4):  # cap the tool-call loop so a stuck model can't hang the request
            resp = client.chat.completions.create(
                model=AGENT_MODEL, messages=messages, tools=AGENT_TOOLS, tool_choice="auto"
            )
            msg = resp.choices[0].message
            if not msg.tool_calls:
                return {"answer": msg.content}

            messages.append(msg)
            for call in msg.tool_calls:
                fn = AGENT_FUNCTIONS.get(call.function.name)
                if not fn:
                    result = {"error": f"unknown tool {call.function.name}"}
                else:
                    try:
                        args = json.loads(call.function.arguments)
                        result = fn(**args)
                    except Exception as e:
                        result = {"error": str(e)}
                messages.append({
                    "role": "tool", "tool_call_id": call.id, "content": json.dumps(result)
                })
        return {"answer": "I wasn't able to finish reasoning about that in time — try a more specific question."}
    except Exception as e:
        return {"error": f"agent error: {e}"}


@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "history_cached": len(_history_cache),
        "constituents_cached": len(_constituents_cache),
        "fundamentals_cached": len(_fundamentals_cache),
        "groq_key_set": bool(os.environ.get("GROQ_API_KEY")),
    }