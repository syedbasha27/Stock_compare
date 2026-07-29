# Running the Index Analytics backend

1. Install dependencies (Python 3.9+):
   pip install -r requirements.txt --break-system-packages

2. (Optional, for the "Ask the index analyst" chat panel) Get a free Groq
   API key at https://console.groq.com and set it:
   export GROQ_API_KEY=your_key_here
   Everything else works fine without this — only the agent chat needs it.

3. Start the server:
   uvicorn app:app --reload --port 8000

4. Leave that terminal running, then open `nifty_dashboard.html` in your
   browser as usual. It talks to http://localhost:8000 automatically.

Check it's alive any time at: http://localhost:8000/api/health
(also reports whether GROQ_API_KEY is set)

## Notes
- First request for a given index takes a couple of seconds (yfinance /
  NSE fetch); repeats within 12h are served from the backend's in-memory
  cache and are near-instant.
- The NSE fallback (for indices Yahoo doesn't carry) uses a real
  `requests.Session()` so cookies persist properly — this is the piece
  that could never work reliably from the browser alone.
- Sector composition / top companies pull from niftyindices.com and
  archives.nseindia.com (tried in that order); top-companies ranks by
  market cap cheaply first, then only fetches growth/revenue for the
  actual top N, to stay fast on a free-tier deploy.
- To add a new index later: add one line to `INDEX_MAP` (and, if you want
  sector/company data for it, `CONSTITUENT_SLUGS`) in `app.py`, and to the
  `INDICES` array in the HTML file — all plain config, no logic changes.
