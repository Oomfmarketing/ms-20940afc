# Museokauppa Dashboard — GitHub Actions deploy

Cloud-hosted version of the daily Museokortti-channel sales dashboard.
Runs on a GitHub Actions cron, pulls Bokún, renders a static HTML page,
commits it to `docs/`, and GitHub Pages serves it on an obscure URL.

## What this repo does

- **Every morning at 07:00 Europe/Helsinki** (04:00 UTC in summer, 05:00 UTC in winter; the workflow is set to 04:00 UTC as a compromise)
- Calls `POST https://api.bokun.io/booking.json/product-booking-search` with HMAC-SHA1 auth
- Aggregates **yesterday's** bookings (Monday rolls up Fri + Sat + Sun)
- Computes: orders, gross sales EUR (BM, not FAS LV), AOV, week-over-week Δ, top 10 products, country mix
- Writes `docs/YYYY-MM-DD.html` and overwrites `docs/latest.html`
- Commits the changes back so GitHub Pages re-serves them

Dependencies: **none** — stdlib Python 3.12 only. No `requirements.txt` needed.

## Trade-off you accepted

GitHub Pages on the free plan requires a **public** repo. The rendered HTML
(with sales numbers) is therefore technically visible to anyone who discovers
the repo or guesses the Pages URL. Mitigation: give the repo an unguessable
name (e.g. `ms-kauppa-2f8a91e7`). Nothing on the public internet links to it.

If you ever want true auth-gating, the options are:
- Move to a GitHub Team/Enterprise plan and make the repo private (Pages supports private repos there)
- Or switch host to **Cloudflare Pages** (supports private source repos on the free tier) and put Cloudflare Access in front

## One-time setup (~5 minutes)

1. **Create a new GitHub repo** with an obscure name, e.g.
   `ms-kauppa-2f8a91e7`. Do **not** initialise with a README.

2. **Push this folder** to it:
   ```sh
   cd "Elämys Group/museokauppa-cloud"
   git init -b main
   git add .
   git commit -m "initial: museokauppa dashboard"
   git remote add origin git@github.com:<your-user>/ms-kauppa-2f8a91e7.git
   git push -u origin main
   ```

3. **Add secrets** (GitHub → Settings → Secrets and variables → Actions → New repository secret):
   - `BOKUN_ACCESS_KEY` — value from `.bokun-credentials`
   - `BOKUN_SECRET_KEY` — value from `.bokun-credentials`

4. **Enable GitHub Pages** (Settings → Pages):
   - Source: **Deploy from a branch**
   - Branch: **`main`**, folder: **`/docs`**
   - Save. Pages will give you a URL like
     `https://<your-user>.github.io/ms-kauppa-2f8a91e7/`

5. **Trigger the first run manually** (Actions → "museokauppa-daily" → Run workflow).
   Wait ~30 seconds. The workflow will fetch yesterday's bookings and commit
   `docs/YYYY-MM-DD.html` + `docs/latest.html`. Pages will rebuild automatically.

6. **Bookmark**: `https://<your-user>.github.io/ms-kauppa-2f8a91e7/latest.html`

## Files

```
museokauppa-cloud/
├── .github/workflows/daily.yml   # cron + commit workflow
├── scripts/build_dashboard.py    # fetch + aggregate + render (stdlib only)
├── docs/                         # served by GitHub Pages
│   ├── index.html                # listing of recent runs
│   ├── latest.html               # overwritten each run (the one to bookmark)
│   ├── YYYY-MM-DD.html           # dated archive (one per run)
│   └── .nojekyll                 # disables Jekyll processing
├── .gitignore
└── README.md
```

## Common adjustments

**Change the run time.** Edit `.github/workflows/daily.yml`, the `cron:` line.
Remember: GitHub Actions cron is in **UTC**. Helsinki summer = UTC+3,
Helsinki winter = UTC+2.

**Switch the sales-day definition** from *creation date* to *experience date*.
In `scripts/build_dashboard.py`, `search_product_bookings()` uses
`creationDateRange`. Swap to `startDateRange` for experience-date reporting.

**Add Trip (elamys-trip2) data later.** Drop a nightly
`trip-orders-YYYY-MM-DD.json` export into the repo (or fetch it from an
endpoint once one exists) and extend `aggregate()` to union the records.

**Notify to MS365 / Slack** after a successful run. Add a step after the
"Commit updated docs" step that POSTs to a Teams/Slack incoming webhook with
the summary line printed by the build script.

## Troubleshooting

- **Workflow fails with HTTP 401/403 from Bokún** → secret values wrong or the
  key lost its Museokortti channel scope. Re-paste from `.bokun-credentials`.
- **Pages URL returns 404** → Pages hasn't built yet. Check Settings → Pages;
  first build can take 1–2 minutes after enabling.
- **Workflow commit step fails** → make sure repo Settings → Actions →
  General → "Workflow permissions" is set to **Read and write**.
- **Cron ran but HTML shows `Blocked`** → the dashboard renders a status page
  with the exact error whenever the fetch fails. Check the Actions log for the
  full stack trace.

## What the dashboard looks like when numbers flow

- 4 KPI tiles: Orders, Gross sales (BM) EUR, AOV, WoW Δ
- Top 10 products table (by gross EUR)
- Country split (if Bokún returns `customerCountry` on line items)
- Data sources panel — green/yellow/red pill per source
- Footer notes: "Gross sales (BM)" = bruttomyynti, not FAS liikevaihto.
  Cancellations are excluded from totals but the count is noted.
