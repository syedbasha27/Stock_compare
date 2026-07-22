# Deploying the backend

This folder is a complete, ready-to-push repo: `app.py`, `requirements.txt`,
`render.yaml`, `Procfile`, `runtime.txt`, `.gitignore`. Push it as-is.

## 1. Create the GitHub repo

```
cd path/to/this/folder
git init
git add .
git commit -m "Index analytics backend"
gh repo create nse-index-backend --public --source=. --push
```

(No `gh` CLI? Create an empty repo on github.com first, then:)

```
git remote add origin https://github.com/<your-username>/nse-index-backend.git
git branch -M main
git push -u origin main
```

## 2. Deploy on Render (uses render.yaml automatically)

1. Go to https://dashboard.render.com
2. **New +** → **Blueprint**
3. Connect the `nse-index-backend` repo
4. Render reads `render.yaml` and shows the service it's about to create —
   click **Apply**
5. Wait for the build to finish (2–4 min first time), then copy the URL
   Render gives you, e.g. `https://nse-index-analytics-api.onrender.com`

Check it's alive: open `<that-url>/api/health` in a browser — should show
`{"status":"ok","cached_entries":0}`.

## 3. Point the frontend at it

In `nifty_dashboard.html`, find:

```js
const API_BASE = "http://localhost:8000";
```

Change it to your Render URL (no trailing slash):

```js
const API_BASE = "https://nse-index-analytics-api.onrender.com";
```

Re-save the file. That's the entire integration — the frontend is still
one self-contained HTML file, it just calls a remote API now instead of
your machine.

## Notes specific to Render's free tier

- Spins down after ~15 min idle. First request after a gap takes 30–60s
  to wake back up — the dashboard's loading state will just sit there
  a bit longer than usual, that's expected, not a bug.
- The `healthCheckPath: /api/health` in render.yaml lets Render confirm
  the service is actually up before routing traffic to it.
- Free plan = 750 hrs/month, more than enough for a class assignment.

## If you'd rather use Fly.io or Railway instead

- **Procfile** is included for platforms that expect one instead of a
  Render blueprint (Railway auto-detects it).
- **Fly.io**: run `fly launch` in this folder, it'll detect the Procfile
  and Python runtime and generate a `fly.toml` for you; then `fly deploy`.
- Either way, the start command is the same one line:
  `uvicorn app:app --host 0.0.0.0 --port $PORT`
