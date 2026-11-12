# Market Terminal

A personal Indian-equities dashboard in the style of a Bloomberg-lite terminal.

## Run it
**Windows:** double-click `run.bat` (it creates a virtualenv, installs the packages, and opens http://127.0.0.1:8000)
**Mac/Linux:** `./run.sh`, then open http://127.0.0.1:8000

You need Python 3.10 or newer.

## What's on it
- **Geopolitical alert banner**: the latest GEO-tagged headline from the last 6 hours
- **KPI tiles**: Nifty 50, Nifty Smallcap 100, FII/DII net flows (NSE), COMEX gold, USD/INR, each with a 20-day sparkline
- **Live intelligence feed**: ET Markets, Moneycontrol, Livemint and Business Standard, auto-tagged EARNINGS / GEO / CRUDE / MACRO / IPO / FLOWS, with filter chips
- **Global cues**: US, Europe and Asia indices, plus Sensex and India VIX
- **Crude & commodities**: Brent, WTI, natural gas, silver, copper, US 10Y yield, dollar index
- **Nifty 50 movers**: the top 6 gainers and top 6 losers
- **Watchlist**: add or remove tickers in the UI, with live search-as-you-type (type a company name like "Lemon Tree" and pick the ticker from the dropdown). A plain name is tried on NSE first and then as-is, so `ETERNAL` becomes `ETERNAL.NS` while `AAPL` stays `AAPL`. Use `.BO` to force BSE. A ticker that quotes nowhere is rejected rather than silently added. Stored in **your browser** (localStorage), so each visitor has their own list and the server holds no state.
- **Global news**: CNBC and ET global markets

## Refresh
Market data is cached for **5 minutes** (`REFRESH_MINUTES`). Locally it refreshes in the background; on Vercel the CDN caches each API response (`s-maxage`). The page skips polling while its tab is hidden.
If a source is unreachable, that panel shows sample data, and the status dot turns amber with a "SAMPLE DATA" label.
Per-symbol failures are printed to the console (`[prices] <symbol>: no data`) — useful when an NSE ticker gets renamed.

Note: Yahoo only serves a same-day snapshot for Nifty Smallcap 100 (`^CNXSC`), so that tile
shows a real price and day change but no sparkline.

## Customise
- Name: set the `TERMINAL_NAME` environment variable (default "Market Terminal")
- Symbols and feeds: edit the `KPIS`, `GLOBAL`, `CRUDE` and `FEEDS` lists at the top of `app.py`
- News tags: edit the `TAGS` regex list in `app.py`

## Deploy to Vercel
1. Push this folder to a GitHub repo (`.gitignore` already excludes `.venv`, logs and `watchlist.json`).
2. On vercel.com: **Add New → Project**, import the repo. Vercel detects FastAPI from `app.py` and serves `public/` from its CDN. No build settings needed.
3. Optional env vars: `TERMINAL_NAME`, `REFRESH_MINUTES`.

Or with the CLI: `npm i -g vercel`, then `vercel` in this folder (`vercel --prod` to promote).

Caveats: NSE often blocks datacenter IPs, so on Vercel the FII/DII tile may show sample data (labelled). Yahoo can rate-limit shared IPs; failed symbols fall back to labelled sample data.

Data comes from Yahoo Finance (delayed about 15 min for NSE), NSE provisional FII/DII figures and public RSS feeds. Unofficial endpoints; not financial advice, and check each provider's terms before running this publicly.
