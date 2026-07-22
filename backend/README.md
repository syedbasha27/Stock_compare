# Running the Index Analytics backend

1. Install dependencies (Python 3.9+):
   pip install -r requirements.txt --break-system-packages

2. Start the server:
   uvicorn app:app --reload --port 8000

3. Leave that terminal running, then open `nifty_dashboard.html` in your
   browser as usual. It talks to http://localhost:8000 automatically.

Check it's alive any time at: http://localhost:8000/api/health

## Notes
- First request for a given index takes a couple of seconds (yfinance /
  NSE fetch); repeats within 12h are served from the backend's in-memory
  cache and are near-instant.
- The NSE fallback (for indices Yahoo doesn't carry) uses a real
  `requests.Session()` so cookies persist properly — this is the piece
  that could never work reliably from the browser alone.
- To add a new index later: add one line to `INDEX_MAP` in `app.py` and
  to the `INDICES` array in the HTML file — both are plain config, no
  logic changes needed.
