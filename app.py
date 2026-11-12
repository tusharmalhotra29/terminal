"""
Market Terminal — an Indian-equities dashboard.
Run locally:  python app.py   then open http://127.0.0.1:8000
Deploy:       Vercel (see README). Zero state on the server: watchlists live in
              each visitor's browser, caching is done by the CDN.
Data: Yahoo Finance (prices), NSE (FII/DII), RSS (news). Falls back to sample
data per-field when a source is unreachable, so the UI always renders.
"""
import html, os, random, re, sys, time, threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path

# Windows consoles default to cp1252, which cannot encode the banner's arrow or
# the rupee signs and ellipses in feed titles — printing them raises
# UnicodeEncodeError and kills the process rather than just garbling output.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import requests
import feedparser
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import FileResponse

ROOT = Path(__file__).parent
PUBLIC = ROOT / "public"
APP_NAME = os.environ.get("TERMINAL_NAME", "Market Terminal")
SERVERLESS = bool(os.environ.get("VERCEL"))   # no background threads between requests
MAX_WATCHLIST = 25
SYMBOL_RE = re.compile(r"^[A-Z0-9.^=&\-]{1,20}$")
YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124 Safari/537.36"}
IST = timezone(timedelta(hours=5, minutes=30))
REFRESH_MINUTES = int(os.environ.get("REFRESH_MINUTES", 5))  # how often prices, news and FII/DII are re-pulled
PRICE_TTL = NEWS_TTL = FLOW_TTL = REFRESH_MINUTES * 60

# ---------------------------------------------------------------- symbols
KPIS = [  # (label, yahoo symbol, decimals)
    ("NIFTY 50", "^NSEI", 2),
    ("NIFTY SMALLCAP 100", "^CNXSC", 2),
    ("GOLD (COMEX, $/OZ)", "GC=F", 2),
    ("USD/INR", "INR=X", 2),
]
GLOBAL = [
    ("S&P 500", "^GSPC"), ("Dow Jones", "^DJI"), ("Nasdaq", "^IXIC"),
    ("FTSE 100", "^FTSE"), ("DAX", "^GDAXI"), ("Nikkei 225", "^N225"),
    ("Hang Seng", "^HSI"), ("Shanghai Composite", "000001.SS"),
    ("Nifty 50", "^NSEI"), ("Sensex", "^BSESN"), ("India VIX", "^INDIAVIX"),
]
CRUDE = [
    ("Brent Crude", "BZ=F", "$/bbl"), ("WTI Crude", "CL=F", "$/bbl"),
    ("Natural Gas", "NG=F", "$/MMBtu"), ("Silver", "SI=F", "$/oz"),
    ("Copper", "HG=F", "$/lb"), ("US 10Y Yield", "^TNX", "%"),
    ("Dollar Index", "DX-Y.NYB", ""),
]
NIFTY50 = """ADANIENT ADANIPORTS APOLLOHOSP ASIANPAINT AXISBANK BAJAJ-AUTO BAJFINANCE
BAJAJFINSV BEL BHARTIARTL BPCL CIPLA COALINDIA DRREDDY EICHERMOT ETERNAL GRASIM
HCLTECH HDFCBANK HDFCLIFE HEROMOTOCO HINDALCO HINDUNILVR ICICIBANK INDUSINDBK INFY
ITC JIOFIN JSWSTEEL KOTAKBANK LT M&M MARUTI NESTLEIND NTPC ONGC POWERGRID RELIANCE
SBILIFE SBIN SHRIRAMFIN SUNPHARMA TATACONSUM TMPV TATASTEEL TCS TECHM TITAN
TRENT ULTRACEMCO WIPRO""".split()
# TMPV = Tata Motors Passenger Vehicles. The old TATAMOTORS.NS line stopped
# resolving on Yahoo after the 2025 demerger; TMLCV (the commercial-vehicle
# half) has no Yahoo history yet, so only the PV entity is tracked here.

FEEDS = {
    "india": [
        ("Economic Times Markets", "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
        ("Moneycontrol", "https://www.moneycontrol.com/rss/marketreports.xml"),
        ("Livemint Markets", "https://www.livemint.com/rss/markets"),
        ("Business Standard", "https://www.business-standard.com/rss/markets-106.rss"),
    ],
    "global": [
        ("CNBC World", "https://www.cnbc.com/id/100727362/device/rss/rss.html"),
        ("CNBC Markets", "https://www.cnbc.com/id/15839069/device/rss/rss.html"),
        ("ET Global Markets", "https://economictimes.indiatimes.com/markets/stocks/news/rssfeeds/2146842.cms"),
    ],
}
TAGS = [  # first match wins
    ("GEO", r"\b(war|attack|strike[sd]?|missile|iran|israel|russia|ukraine|sanction|tariff|geopolit|border|conflict|ceasefire)\b"),
    ("EARNINGS", r"\b(q[1-4]|profit|earnings|results?|revenue|ebitda|net loss|guidance)\b"),
    ("CRUDE", r"\b(crude|oil|brent|opec|wti)\b"),
    ("MACRO", r"\b(rbi|fed|inflation|cpi|gdp|repo|rate cut|rate hike|fiscal|treasury|yield)\b"),
    ("IPO", r"\b(ipo|listing|gmp|subscribed)\b"),
    ("FLOWS", r"\b(fii|fpi|dii|outflow|inflow)\b"),
]

# ---------------------------------------------------------------- cache
# _refreshing maps key -> Event, so a second caller waits for an in-flight fetch
# instead of kicking off a duplicate one.
_cache, _lock, _refreshing = {}, threading.Lock(), {}

def _refresh(key, fn):
    """Run fn and store the result. Never raises: logs and keeps any older value."""
    try:
        val = fn()
        with _lock:
            _cache[key] = (time.time(), val)
    except Exception as e:
        print(f"[cache] {key} refresh failed:", e)
    finally:
        with _lock:
            done = _refreshing.pop(key, None)
        if done:
            done.set()

def cached(key, ttl, fn):
    """Stale-while-revalidate: serve the last value instantly, refresh in background."""
    with _lock:
        hit = _cache.get(key)
        fresh = hit and time.time() - hit[0] < ttl
        waiter, starter = _refreshing.get(key), False
        if not fresh and waiter is None:
            waiter, starter = threading.Event(), True
            _refreshing[key] = waiter
    if fresh:
        return hit[1]
    if hit and not SERVERLESS:  # stale: hand back the old value, refresh behind the request
        if starter:
            threading.Thread(target=_refresh, args=(key, fn), daemon=True).start()
        return hit[1]
    if starter:          # nothing cached yet (or serverless): this caller blocks once
        _refresh(key, fn)
    else:                # someone else is already fetching it, ride along
        waiter.wait(timeout=90)
    with _lock:
        got = _cache.get(key)   # on a failed serverless refresh this is still the stale value
    if not got:
        raise HTTPException(503, f"{key} is unavailable, please retry shortly")
    return got[1]

def _invalidate(key):
    """Force the next cached() call for key to rebuild from scratch."""
    with _lock:
        _cache.pop(key, None)

# ---------------------------------------------------------------- prices
def _sample_quote(sym):
    """Deterministic-ish fallback so the UI is usable offline."""
    base = {"^NSEI": 24187.7, "^CNXSC": 19428.05, "GC=F": 4066.6, "INR=X": 96.24,
            "^GSPC": 7443.28, "^DJI": 51839.26, "^IXIC": 25508.07, "^N225": 66232.19,
            "^HSI": 25132.29, "000001.SS": 3864.37, "^FTSE": 9812.4, "^GDAXI": 24510.2,
            "^BSESN": 79420.5, "^INDIAVIX": 13.42, "BZ=F": 82.15, "CL=F": 78.40,
            "NG=F": 3.21, "SI=F": 48.7, "HG=F": 4.92, "^TNX": 4.31, "DX-Y.NYB": 101.2}
    rnd = random.Random(sym + datetime.now(IST).strftime("%Y%m%d%H%M"))
    p = base.get(sym, rnd.uniform(200, 4000))
    chg = rnd.uniform(-2.5, 2.5)
    return {"price": round(p, 2), "change_pct": round(chg, 2), "spark": [], "live": False}

def _fetch_one(sym):
    """One symbol via Yahoo's chart endpoint. Returns a quote dict or None."""
    for host in ("query1", "query2"):
        try:
            r = requests.get(f"https://{host}.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(sym)}",
                             params={"range": "3mo", "interval": "1d"},
                             headers=YAHOO_HEADERS, timeout=8)
            if r.status_code == 404:
                print(f"[prices] {sym}: not found (delisted or wrong symbol?)")
                return None
            r.raise_for_status()
            res = r.json()["chart"]["result"][0]
            quote = res["indicators"]["quote"][0]
            closes = [float(x) for x in quote.get("close", []) if x is not None]
            meta = res.get("meta", {})
            if not closes:
                return None
            last = closes[-1]
            if len(closes) >= 2:
                prev = closes[-2]
            else:
                # Yahoo serves only a same-day snapshot for a few indices
                # (^CNXSC): price is real, so change off the previous close/open.
                opens = [x for x in quote.get("open", []) if x is not None]
                prev = meta.get("chartPreviousClose") or (float(opens[-1]) if opens else last)
            ret = lambda n: round((last / closes[-1 - n] - 1) * 100, 2) if len(closes) > n else None
            return {"r5": ret(5), "r21": ret(21), "price": round(last, 2),
                    "change_pct": round((last / prev - 1) * 100, 2) if prev else 0.0,
                    "spark": [round(x, 2) for x in closes[-20:]], "live": True}
        except Exception as e:
            print(f"[prices] {sym} via {host} failed: {type(e).__name__}: {e}")
    return None

def fetch_quotes(symbols):
    """Returns {symbol: {price, change_pct, spark, live}}, fetching in parallel."""
    symbols = list(dict.fromkeys(symbols))
    out = {}
    if symbols:
        with ThreadPoolExecutor(max_workers=16) as pool:
            for s, q in zip(symbols, pool.map(_fetch_one, symbols)):
                if q:
                    out[s] = q
    missing = [s for s in symbols if s not in out]
    if missing:
        print("[prices] falling back to sample data for:", ", ".join(missing))
    for s in missing:
        out[s] = _sample_quote(s)
    return out

def search_symbols(query):
    """Company-name -> ticker lookup via Yahoo Finance's own search endpoint.

    Lets the watchlist box take "Lemon Tree" and resolve it to LEMONTREE.NS,
    instead of requiring the exact ticker up front.
    """
    query = query.strip()
    if len(query) < 2:
        return []
    try:
        r = requests.get("https://query2.finance.yahoo.com/v1/finance/search",
                         params={"q": query, "quotesCount": 8, "newsCount": 0,
                                 "listsCount": 0, "enableFuzzyQuery": True},
                         headers=YAHOO_HEADERS, timeout=6)
        quotes = r.json().get("quotes", [])
    except Exception as e:
        print("[search] yahoo failed:", e)
        return []
    out = []
    for q in quotes:
        sym = q.get("symbol")
        if not sym or q.get("quoteType") not in ("EQUITY", "INDEX", "ETF", "CURRENCY", "FUTURE"):
            continue
        out.append({"symbol": sym, "name": q.get("shortname") or q.get("longname") or sym,
                    "exchange": q.get("exchange", "")})
    # NSE/BSE first since this is an India-focused terminal, then everything else.
    out.sort(key=lambda q: 0 if q["exchange"] in ("NSI", "BSE") else 1)
    return out

def market_open():
    """NSE cash session: Mon-Fri 09:15-15:30 IST (holidays not modelled)."""
    n = datetime.now(IST)
    return n.weekday() < 5 and (9, 15) <= (n.hour, n.minute) <= (15, 30)

def _row(q, label, sym, **extra):
    return {"label": label, "symbol": sym, **q[sym], **extra}

def watch_row(q, sym):
    # RELIANCE.NS and RELIANCE.BO would otherwise render as the same label.
    base = sym.replace(".NS", "").replace(".BO", "")
    return _row(q, base, sym, unit="BSE") if sym.endswith(".BO") else _row(q, base, sym)

def build_market():
    nifty_syms = [f"{t}.NS" for t in NIFTY50]
    syms = {s for _, s, _ in KPIS} | {s for _, s in GLOBAL} | {s for _, s, _ in CRUDE} | set(nifty_syms)
    q = fetch_quotes(sorted(syms))
    row = lambda l, s, **x: _row(q, l, s, **x)
    movers = sorted((row(s[:-3], s) for s in nifty_syms), key=lambda r: r["change_pct"])
    live_count = sum(1 for v in q.values() if v["live"])
    return {
        "kpis": [row(l, s, decimals=d) for l, s, d in KPIS],
        "global": [row(l, s) for l, s in GLOBAL],
        "crude": [row(l, s, unit=u) for l, s, u in CRUDE],
        "movers": {"gainers": movers[::-1][:6], "losers": movers[:6]},
        "live": live_count > 0,
        "live_ratio": f"{live_count}/{len(q)}",
    }

# ---------------------------------------------------------------- FII / DII
def fetch_flows():
    """NSE provisional cash-market FII/DII numbers (Rs crore)."""
    try:
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124 Safari/537.36",
                          "Accept": "application/json", "Referer": "https://www.nseindia.com/"})
        s.get("https://www.nseindia.com/", timeout=8)
        data = s.get("https://www.nseindia.com/api/fiidiiTradeReact", timeout=8).json()
        res = {"live": True}
        for d in data:
            key = "fii" if "FII" in d["category"].upper() or "FPI" in d["category"].upper() else "dii"
            res[key] = round(float(d["netValue"]), 0)
            res["date"] = d.get("date")
        return res
    except Exception as e:
        print("[flows] NSE failed:", e)
        return {"fii": -1121, "dii": 1312, "date": None, "live": False}

# ---------------------------------------------------------------- news
def _tag(text):
    t = text.lower()
    for tag, pat in TAGS:
        if re.search(pat, t):
            return tag
    return "MARKETS"

def _clean(html_):
    txt = html.unescape(html.unescape(re.sub(r"<[^>]+>", " ", html_ or ""))).replace(" ", " ")
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt[:220] + ("…" if len(txt) > 220 else "")

def _ts(entry):
    for k in ("published", "updated"):
        if entry.get(k):
            try:
                return parsedate_to_datetime(entry[k]).timestamp()
            except Exception:
                pass
    return time.time()

def fetch_news(kind):
    items = []
    for source, url in FEEDS[kind]:
        try:
            r = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            for e in feedparser.parse(r.content).entries[:25]:
                title = html.unescape(html.unescape(e.get("title") or "")).strip()
                if not title:
                    continue
                summary = _clean(e.get("summary", ""))
                items.append({"title": title, "summary": summary, "link": e.get("link"),
                              "source": source, "ts": _ts(e), "tag": _tag(title + " " + summary)})
        except Exception as ex:
            print(f"[news] {source} failed:", ex)
    seen, uniq = set(), []
    for it in sorted(items, key=lambda x: -x["ts"]):
        k = re.sub(r"\W+", "", it["title"].lower())[:60]
        if k not in seen:
            seen.add(k); uniq.append(it)
    if not uniq:
        uniq = _sample_news(kind)
    return uniq[:60]

def _sample_news(kind):
    now = time.time()
    s = [
        ("EARNINGS", "Bandhan Bank Q1 profit jumps 35% as provisions decline sharply",
         "Bandhan Bank reported a 35% rise in Q1 net profit to Rs 502 crore, driven by lower provisions."),
        ("GEO", "Crude oil prices rise 2% on fresh geopolitical tensions in the Middle East",
         "Oil prices surged nearly two percent amid escalating tensions; traders watch supply routes."),
        ("EARNINGS", "US stock futures gain as chip stocks extend recovery; earnings in focus", ""),
        ("MACRO", "RBI keeps repo rate unchanged, maintains neutral stance", "MPC voted 5-1 to hold rates."),
        ("FLOWS", "FPIs turn net sellers for third straight session", "Foreign investors pulled out Rs 1,121 crore."),
        ("IPO", "SME IPO subscribed 48 times on final day; GMP signals strong listing", ""),
    ]
    return [{"title": t, "summary": d, "link": None, "source": "Sample data (offline)",
             "ts": now - i * 120 - 1380, "tag": g} for i, (g, t, d) in enumerate(s)]

# ---------------------------------------------------------------- sectors
SECTORS = [  # (name, yahoo index, headline keywords)
    ("Banks", "^NSEBANK", r"\b(bank|banks|banking|hdfc bank|icici|sbi|axis bank|kotak|npa|credit growth|nim)\b"),
    ("Financial Services", "NIFTY_FIN_SERVICE.NS", r"\b(nbfc|bajaj fin|insurance|mutual fund|sebi|lender|housing finance|broker)\w*"),
    ("PSU Banks", "^CNXPSUBANK", r"\b(psu bank|public sector bank|sbi|bank of baroda|canara|pnb)\b"),
    ("IT", "^CNXIT", r"\b(it stocks?|infosys|tcs|wipro|hcl ?tech|tech mahindra|software|ai demand|h-?1b|nasscom)\b"),
    ("Auto", "^CNXAUTO", r"\b(auto|automobile|maruti|tata motors|mahindra|two-wheeler|ev|vehicle sales|bajaj auto|hero moto)\b"),
    ("Pharma", "^CNXPHARMA", r"\b(pharma|drug|usfda|sun pharma|cipla|dr reddy|biocon|generic|healthcare)\b"),
    ("FMCG", "^CNXFMCG", r"\b(fmcg|hul|hindustan unilever|itc|nestle|britannia|rural demand|consumer goods)\b"),
    ("Metals", "^CNXMETAL", r"\b(metal|steel|aluminium|copper|hindalco|tata steel|jsw|vedanta|iron ore|zinc)\b"),
    ("Energy", "^CNXENERGY", r"\b(crude|oil|gas|reliance|ongc|opec|brent|refin\w*|power|ntpc|energy)\b"),
    ("Realty", "^CNXREALTY", r"\b(real estate|realty|dlf|godrej properties|housing|property|home loan|rera)\b"),
    ("Infrastructure", "^CNXINFRA", r"\b(infra\w*|capex|l&t|larsen|cement|road|railway|port|construction|order book)\b"),
    ("PSU", "^CNXPSE", r"\b(psu|public sector|disinvestment|ongc|coal india|bhel|hal|bel|defence)\b"),
    ("Consumption", "^CNXCONSUM", r"\b(consumption|consumer|retail|demand|festive|titan|trent|discretionary)\b"),
    ("MNC", "^CNXMNC", r"\b(mnc|multinational|nestle|hindustan unilever|maruti suzuki|abb|siemens)\b"),
]
POS = r"\b(surge[sd]?|jump[sd]?|rall(y|ies|ied)|gain[s]?|rise[s]?|rose|record|beat[s]?|upgrade[sd]?|boost|strong|growth|buy|outperform|wins?|order win|profit rises?)\b"
NEG = r"\b(fall[s]?|fell|drop[s]?|slump[s]?|plunge[sd]?|crash\w*|tumble[sd]?|slide[s]?|weak|miss(es|ed)?|downgrade[sd]?|probe|ban|penalty|loss|cut[s]?|sell-?off|selloff|concern[s]?|risk[s]?)\b"

def _z(vals):
    vals = [v for v in vals if v is not None]
    if len(vals) < 2:
        return lambda v: 0.0
    m = sum(vals) / len(vals)
    sd = (sum((v - m) ** 2 for v in vals) / len(vals)) ** 0.5 or 1.0
    return lambda v: 0.0 if v is None else (v - m) / sd

def build_sectors():
    q = fetch_quotes([sym for _, sym, _ in SECTORS])
    try:
        heads = (fetch_news_cached("india") + fetch_news_cached("global"))
    except Exception:
        heads = []
    cutoff = time.time() - 48 * 3600
    heads = [h for h in heads if h["ts"] >= cutoff]
    rows = []
    for name, sym, kw in SECTORS:
        if not q[sym]["live"]:
            continue
        hits = [h for h in heads if re.search(kw, (h["title"] + " " + h["summary"]).lower())]
        sent = sum((1 if re.search(POS, h["title"].lower()) else 0)
                   - (1 if re.search(NEG, h["title"].lower()) else 0) for h in hits)
        rows.append({"sector": name, "symbol": sym, "price": q[sym]["price"], "d1": q[sym]["change_pct"],
                     "r5": q[sym].get("r5"), "r21": q[sym].get("r21"), "spark": q[sym]["spark"],
                     "news": len(hits), "sentiment": sent,
                     "headlines": [{"title": h["title"], "link": h["link"], "source": h["source"]} for h in hits[:3]]})
    zd, z5, z21 = (_z([r[k] for r in rows]) for k in ("d1", "r5", "r21"))
    zn, zs = _z([r["news"] for r in rows]), _z([r["sentiment"] for r in rows])
    for r in rows:   # momentum (60%) + news attention (15%) + news tone (25%)
        r["score"] = round(0.2 * zd(r["d1"]) + 0.25 * z5(r["r5"]) + 0.15 * z21(r["r21"])
                           + 0.15 * zn(r["news"]) + 0.25 * zs(r["sentiment"]), 2)
        r["view"] = ("Rally watch" if r["score"] > 0.5 else "Under pressure" if r["score"] < -0.5 else "Neutral")
        bits = []
        if r["r5"] is not None: bits.append(f"{r['r5']:+.1f}% over 5 sessions")
        if r["r21"] is not None: bits.append(f"{r['r21']:+.1f}% over ~1 month")
        bits.append(f"{r['news']} headline{'s' if r['news'] != 1 else ''} in 48h" +
                    (f" (net tone {r['sentiment']:+d})" if r["news"] else ""))
        r["why"] = " · ".join(bits)
    rows.sort(key=lambda r: -r["score"])
    return {"top": rows[:5], "bottom": rows[-5:][::-1] if len(rows) >= 10 else rows[5:][::-1],
            "count": len(rows)}

def fetch_news_cached(kind):
    return cached(f"news:{kind}", NEWS_TTL, lambda: fetch_news(kind))

# ---------------------------------------------------------------- API
PREWARM = [("market", build_market), ("flows", fetch_flows),
           ("news:india", lambda: fetch_news("india")),
           ("news:global", lambda: fetch_news("global"))]

def _prewarm_one(key, fn):
    with _lock:
        if key in _refreshing or key in _cache:
            return
        _refreshing[key] = threading.Event()
    _refresh(key, fn)

@asynccontextmanager
async def lifespan(_app):
    """Warm every panel in the background so the first page load is instant."""
    if not SERVERLESS:
        for key, fn in PREWARM:
            threading.Thread(target=_prewarm_one, args=(key, fn), daemon=True).start()
    yield

app = FastAPI(title=APP_NAME, lifespan=lifespan)

def edge_cache(response, seconds):
    """Let Vercel's CDN (shared cache, s-maxage) absorb traffic. Browsers ignore s-maxage."""
    response.headers["Cache-Control"] = f"public, s-maxage={seconds}, stale-while-revalidate={seconds * 2}"

@app.get("/")
def index():  # on Vercel the CDN serves public/index.html directly; this is for local runs
    return FileResponse(PUBLIC / "index.html")

@app.get("/api/config")
def config(response: Response):
    edge_cache(response, 3600)
    return {"name": APP_NAME, "max_watchlist": MAX_WATCHLIST,
            "default_watchlist": ["RELIANCE.NS", "HDFCBANK.NS", "TCS.NS", "INFY.NS"]}

@app.get("/api/market")
def market(response: Response):
    edge_cache(response, PRICE_TTL)
    data = cached("market", PRICE_TTL, build_market)
    flows = cached("flows", FLOW_TTL, fetch_flows)
    return {**data, "flows": flows, "market_open": market_open(),
            "generated": datetime.now(IST).isoformat()}

@app.get("/api/quotes")
def quotes(response: Response, s: str = ""):
    """Watchlist quotes for the symbols the browser holds (comma-separated)."""
    syms = list(dict.fromkeys(x.strip().upper() for x in s.split(",") if x.strip()))
    if len(syms) > MAX_WATCHLIST or not all(SYMBOL_RE.match(x) for x in syms):
        raise HTTPException(400, f"send at most {MAX_WATCHLIST} valid symbols")
    if not syms:
        return {"watchlist": []}
    edge_cache(response, 60)
    q = cached("wl:" + ",".join(sorted(syms)), 60, lambda: fetch_quotes(syms))
    return {"watchlist": [watch_row(q, x) for x in syms]}

@app.get("/api/sectors")
def sectors(response: Response):
    edge_cache(response, PRICE_TTL)
    return {**cached("sectors", PRICE_TTL, build_sectors), "generated": datetime.now(IST).isoformat()}

@app.get("/api/search")
def search(response: Response, q: str = ""):
    q = q.strip()[:60]
    if len(q) < 2:
        return {"results": []}
    edge_cache(response, 600)
    return {"results": cached(f"search:{q.lower()}", 600, lambda: search_symbols(q))}

@app.get("/api/news")
def news(response: Response, kind: str = "india"):
    if kind not in FEEDS:
        raise HTTPException(400, "kind must be india or global")
    edge_cache(response, NEWS_TTL)
    items = cached(f"news:{kind}", NEWS_TTL, lambda: fetch_news(kind))
    alert = next((i for i in items if i["tag"] == "GEO" and time.time() - i["ts"] < 6 * 3600), None)
    return {"items": items, "alert": alert}

def _resolve(sym):
    """Pick the exchange for a user-typed ticker and confirm it actually quotes.

    A plain name tries NSE first and then the bare symbol, so ETERNAL becomes
    ETERNAL.NS while AAPL stays AAPL. An explicit suffix (.NS/.BO), an index
    (^NSEI) or a pair (INR=X) is taken as given.
    """
    sym = sym.strip().upper()
    if not sym or len(sym) > 40:
        raise HTTPException(400, "empty or too-long symbol")
    explicit = "." in sym or sym.startswith("^") or "=" in sym
    candidates = [sym] if explicit else [f"{sym}.NS", sym]
    candidates = [c for c in candidates if SYMBOL_RE.match(c)]
    if candidates:
        quotes = fetch_quotes(candidates)
        for c in candidates:
            if quotes[c]["live"]:
                return c
    # Not a valid ticker as typed — maybe it's a company name ("Lemon Tree").
    hit = next(iter(search_symbols(sym)), None)
    if hit and SYMBOL_RE.match(hit["symbol"].upper()):
        q = fetch_quotes([hit["symbol"]])
        if q[hit["symbol"]]["live"]:
            return hit["symbol"]
    raise HTTPException(404, f"no market data for {sym} "
                             f"(unknown ticker/name, or Yahoo Finance is unreachable)")

@app.get("/api/resolve")
def resolve(q: str = ""):
    """Validate/resolve a ticker or company name. The browser stores the result."""
    return {"symbol": _resolve(q)}

@app.get("/api/health")
def health():
    return {"ok": True, "serverless": SERVERLESS}

if __name__ == "__main__":
    import uvicorn
    print(f"\n  {APP_NAME} -> http://127.0.0.1:8000\n")
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")
