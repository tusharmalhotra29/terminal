"""
Telegram bot for Market Terminal.

- Commands arrive on a webhook (/api/telegram/webhook) and edit a per-chat record.
- A scheduler (GitHub Actions, see .github/workflows/tick.yml) POSTs /api/cron/tick
  every ~30 min; each tick sends price alerts, breaking-news/market-shock alerts
  and a digest roughly every 3 hours.
- State lives in Redis over REST (Upstash / Vercel Marketplace). With no Redis
  configured it falls back to process memory, which is fine for local testing only.
"""
import hashlib, hmac, html, json, os, re, time
from datetime import datetime

import requests
from fastapi import HTTPException, Request
from fastapi.concurrency import run_in_threadpool

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
WEBHOOK_SECRET = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "")
CRON_SECRET = os.environ.get("CRON_SECRET", "")
REDIS_URL = os.environ.get("UPSTASH_REDIS_REST_URL") or os.environ.get("KV_REST_API_URL", "")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN") or os.environ.get("KV_REST_API_TOKEN", "")

DIGEST_EVERY = 3 * 3600
QUIET_HOURS = (23, 7)          # IST; digests and ordinary alerts are held back overnight
BREAKING_THRESHOLD = 3
MAX_ALERTS_PER_USER = 10
MAX_BREAKING_PER_TICK = 3

HELP = """<b>Market Terminal bot</b>

/watch TCS INFY - add to your watchlist
/unwatch TCS - remove
/list - watchlist with live prices
/now - market update right now
/alert TCS above 4200 - price alert (or below)
/alerts - list alerts, /unalert 2 removes #2
/digest on|off - update every ~3h (quiet 11pm-7am IST)
/breaking on|off - major world/market events
/stop - delete all my data"""


def esc(s):
    return html.escape(str(s), quote=False)


# ---------------------------------------------------------------- storage
class Store:
    def __init__(self):
        self.redis = bool(REDIS_URL and REDIS_TOKEN)
        self._users, self._seen = {}, {}

    def _cmd(self, *args):
        r = requests.post(REDIS_URL, headers={"Authorization": f"Bearer {REDIS_TOKEN}"},
                          json=list(args), timeout=6)
        r.raise_for_status()
        return r.json().get("result")

    def get_user(self, cid):
        raw = self._cmd("HGET", "tg:users", cid) if self.redis else self._users.get(cid)
        return _user_defaults(json.loads(raw) if raw else {})

    def save_user(self, cid, user):
        raw = json.dumps(user)
        if self.redis:
            self._cmd("HSET", "tg:users", cid, raw)
        else:
            self._users[cid] = raw

    def delete_user(self, cid):
        if self.redis:
            self._cmd("HDEL", "tg:users", cid)
        else:
            self._users.pop(cid, None)

    def all_users(self):
        if self.redis:
            flat = self._cmd("HGETALL", "tg:users") or []
            return {flat[i]: _user_defaults(json.loads(flat[i + 1])) for i in range(0, len(flat), 2)}
        return {cid: _user_defaults(json.loads(raw)) for cid, raw in self._users.items()}

    def first_seen(self, key, ttl=3 * 86400):
        """True exactly once per key within ttl (atomic SET NX on Redis)."""
        if self.redis:
            return self._cmd("SET", f"tg:seen:{key}", "1", "NX", "EX", ttl) == "OK"
        if key in self._seen:
            return False
        self._seen[key] = time.time()
        return True


store = Store()


def _user_defaults(u):
    return {"watch": [], "digest": True, "breaking": True, "alerts": [], "last_digest": 0, **u}


# ---------------------------------------------------------------- telegram api
def tg(method, **payload):
    r = requests.post(f"https://api.telegram.org/bot{TOKEN}/{method}", json=payload, timeout=10)
    return r


def send(cid, text):
    """Send one message. Returns False (and drops the user) if they blocked the bot."""
    try:
        r = tg("sendMessage", chat_id=cid, text=text[:4000], parse_mode="HTML",
               disable_web_page_preview=True)
    except Exception as e:
        print("[telegram] send failed:", e)
        return True
    if r.status_code in (400, 403) and ("blocked" in r.text or "chat not found" in r.text
                                         or "deactivated" in r.text):
        store.delete_user(cid)
        return False
    if not r.ok:
        print("[telegram] send error:", r.status_code, r.text[:200])
    return True


# ---------------------------------------------------------------- formatting
def fmt_q(label, q, dec=2):
    c = q["change_pct"]
    mark = "▲" if c > 0 else "▼" if c < 0 else "■"
    return f"{mark} <b>{esc(label)}</b> {q['price']:,.{dec}f} ({c:+.2f}%)"


def quiet_now(deps):
    h = datetime.now(deps.ist).hour
    return h >= QUIET_HOURS[0] or h < QUIET_HOURS[1]


def build_digest(deps, user):
    now = datetime.now(deps.ist)
    m = deps.market()
    lines = [f"<b>{esc(deps.app_name)} · {now:%a %d %b, %H:%M} IST</b>"]
    if not m.get("live"):
        lines.append("<i>Data sources unreachable: figures below are sample data.</i>")
    lines.append("")
    lines += [fmt_q(k["label"], k, k.get("decimals", 2)) for k in m["kpis"]]
    try:
        f = deps.flows()
        if f.get("live"):
            lines.append(f"FII {f['fii']:+,.0f} cr · DII {f['dii']:+,.0f} cr")
    except Exception:
        pass

    wl = user["watch"] or deps.default_watchlist
    try:
        q = deps.quotes(wl)
        lines += ["", "<b>Your watchlist</b>" if user["watch"] else "<b>Watchlist</b> (default; /watch to customise)"]
        lines += [fmt_q(deps.watch_row(q, s)["label"], q[s]) for s in wl]
    except Exception as e:
        print("[digest] watchlist failed:", e)

    g, l = m["movers"]["gainers"][:3], m["movers"]["losers"][:3]
    lines += ["", "<b>Nifty 50 movers</b>"]
    lines += [fmt_q(r["label"], r) for r in g + l]

    try:
        s = deps.sectors()
        lines += ["", "<b>Sectors</b>",
                  "In focus: " + ", ".join(f"{r['sector']} ({r['d1']:+.1f}%)" for r in s["top"][:3]),
                  "Lagging: " + ", ".join(f"{r['sector']} ({r['d1']:+.1f}%)" for r in s["bottom"][:3])]
    except Exception as e:
        print("[digest] sectors failed:", e)

    try:
        cutoff = time.time() - 6 * 3600
        heads = [i for i in deps.news("india") if i["ts"] >= cutoff][:4] + \
                [i for i in deps.news("global") if i["ts"] >= cutoff][:3]
        if heads:
            lines += ["", "<b>Headlines</b>"]
            for i in heads:
                t = esc(i["title"])
                lines.append(f"• <a href=\"{esc(i['link'])}\">{t}</a>" if i.get("link") else f"• {t}")
    except Exception as e:
        print("[digest] news failed:", e)
    return "\n".join(lines)


# ---------------------------------------------------------------- breaking news
SEVERE = re.compile(
    r"\b(declares? war|invasion|invades?|missile|airstrikes?|nuclear|explosions?|coup|assassinat\w*|"
    r"emergency|circuit breaker|trading halt|crash(es|ed)?|plunges?|plummets?|collapse[sd]?|default(s|ed)?|"
    r"bank run|surprise (rate )?(hike|cut)|sanctions?|ceasefire|earthquake|tsunami|pandemic|lockdown|"
    r"bankruptcy|black swan|flash crash)\b")
STOP = {"with", "that", "this", "from", "have", "will", "into", "over", "after", "says", "amid",
        "about", "their", "more", "than", "what", "news", "market", "markets", "stock", "stocks"}


def _words(t):
    return set(re.findall(r"[a-z]{4,}", t.lower())) - STOP


def breaking_score(item, items):
    s = 2 if SEVERE.search(item["title"].lower()) else 0
    if item["tag"] in ("GEO", "MACRO"):
        s += 1
    w = _words(item["title"])
    sources = {o["source"] for o in items
               if o is not item and o["source"] != item["source"] and len(w & _words(o["title"])) >= 3}
    return s + min(len(sources), 2)


SHOCKS = [  # (label, threshold in %, direction-sensitive)
    ("NIFTY 50", 2.0), ("India VIX", 10.0), ("Brent Crude", 4.0), ("GOLD (COMEX, $/OZ)", 3.0), ("USD/INR", 1.0),
]


def market_shocks(deps):
    m = deps.market()
    if not m.get("live"):
        return []
    rows = {r["label"]: r for r in m["kpis"] + m["global"] + m["crude"]}
    today = datetime.now(deps.ist).strftime("%Y%m%d")
    out = []
    for label, th in SHOCKS:
        r = rows.get(label) or rows.get(label.title())
        if r and abs(r["change_pct"]) >= th:
            out.append({"key": f"shock:{label}:{today}", "score": 5,
                        "text": f"<b>Market move: {esc(label)}</b> {r['price']:,.2f} ({r['change_pct']:+.2f}% today)"})
    return out


def find_breaking(deps):
    items = deps.news("india") + deps.news("global")
    fresh = [i for i in items if time.time() - i["ts"] < 4 * 3600]
    out = []
    for i in fresh:
        sc = breaking_score(i, items)
        if sc >= BREAKING_THRESHOLD:
            link = f"\n{esc(i['link'])}" if i.get("link") else ""
            out.append({"key": "news:" + hashlib.md5(re.sub(r"\W+", "", i["title"].lower())[:60].encode()).hexdigest(),
                        "score": sc, "text": f"<b>Breaking · {esc(i['tag'])}</b>\n{esc(i['title'])}\n<i>{esc(i['source'])}</i>{link}"})
    try:
        out += market_shocks(deps)
    except Exception as e:
        print("[tick] shocks failed:", e)
    out.sort(key=lambda a: -a["score"])
    return out


# ---------------------------------------------------------------- tick
def tick(deps):
    users = store.all_users()
    stats = {"users": len(users), "alerts": 0, "breaking": 0, "digests": 0}
    if not users:
        return stats
    quiet = quiet_now(deps)
    users = {cid: _user_defaults(u) for cid, u in users.items()}

    # price alerts: not held back overnight, the user asked for them explicitly
    syms = {a["sym"] for u in users.values() for a in u["alerts"]}
    if syms:
        q = deps.quotes(sorted(syms))
        for cid, u in users.items():
            hit = [a for a in u["alerts"] if q[a["sym"]]["live"] and
                   (q[a["sym"]]["price"] >= a["price"] if a["dir"] == "above" else q[a["sym"]]["price"] <= a["price"])]
            if hit:
                for a in hit:
                    if send(cid, f"<b>Price alert</b>\n{fmt_q(a['sym'], q[a['sym']])}\nTarget: {a['dir']} {a['price']:,.2f}"):
                        stats["alerts"] += 1
                u["alerts"] = [a for a in u["alerts"] if a not in hit]
                store.save_user(cid, u)

    # breaking news / market shocks
    try:
        fresh = []
        for a in find_breaking(deps):
            if len(fresh) >= MAX_BREAKING_PER_TICK:
                break
            if (not quiet or a["score"] >= 5) and store.first_seen(a["key"]):
                fresh.append(a)
        for a in fresh:
            for cid, u in users.items():
                if u["breaking"] and send(cid, a["text"]):
                    stats["breaking"] += 1
    except Exception as e:
        print("[tick] breaking failed:", e)

    # digests
    if not quiet:
        now = time.time()
        due = [(cid, u) for cid, u in users.items() if u["digest"] and now - u["last_digest"] >= DIGEST_EVERY - 600]
        for cid, u in due:
            try:
                if send(cid, build_digest(deps, u)):
                    u["last_digest"] = now
                    store.save_user(cid, u)
                    stats["digests"] += 1
            except Exception as e:
                print(f"[tick] digest for {cid} failed:", e)
    return stats


# ---------------------------------------------------------------- commands
def _onoff(arg):
    a = arg.strip().lower()
    return True if a in ("on", "yes", "1") else False if a in ("off", "no", "0") else None


def handle_text(deps, cid, text):
    cmd, _, arg = text.strip().partition(" ")
    cmd, arg = cmd.split("@")[0].lower(), arg.strip()
    if cmd in ("/start", "/help"):
        store.save_user(cid, store.get_user(cid))
        return HELP
    if cmd == "/stop":
        store.delete_user(cid)
        return "All your data is deleted and you won't get further messages. /start to come back."

    u = store.get_user(cid)
    if cmd == "/watch":
        names = [x for x in re.split(r"[\s,]+", arg) if x][:5]
        if not names:
            return "Usage: /watch TCS INFY"
        msgs = []
        for n in names:
            if len(u["watch"]) >= deps.max_watchlist:
                msgs.append(f"Watchlist full ({deps.max_watchlist}).")
                break
            try:
                sym = deps.resolve(n)
            except Exception:
                msgs.append(f"No market data for {esc(n)}.")
                continue
            if sym not in u["watch"]:
                u["watch"].append(sym)
            msgs.append(f"Added {esc(sym)}.")
        store.save_user(cid, u)
        return "\n".join(msgs)
    if cmd == "/unwatch":
        want = arg.upper().replace(".NS", "").replace(".BO", "")
        keep = [s for s in u["watch"] if s.replace(".NS", "").replace(".BO", "") != want]
        if len(keep) == len(u["watch"]):
            return f"{esc(arg)} isn't on your watchlist."
        u["watch"] = keep
        store.save_user(cid, u)
        return f"Removed {esc(arg.upper())}."
    if cmd == "/list":
        wl = u["watch"] or deps.default_watchlist
        q = deps.quotes(wl)
        head = "<b>Your watchlist</b>" if u["watch"] else "<b>Default watchlist</b> (use /watch to build yours)"
        return head + "\n" + "\n".join(fmt_q(deps.watch_row(q, s)["label"], q[s]) for s in wl)
    if cmd == "/now":
        return build_digest(deps, u)
    if cmd in ("/digest", "/breaking"):
        on = _onoff(arg)
        if on is None:
            return f"Usage: {cmd} on|off"
        u[cmd[1:]] = on
        store.save_user(cid, u)
        return f"{cmd[1:].capitalize()} {'on' if on else 'off'}."
    if cmd == "/alert":
        parts = arg.split()
        if len(parts) != 3 or parts[1].lower() not in ("above", "below"):
            return "Usage: /alert TCS above 4200"
        try:
            price = float(parts[2].replace(",", ""))
            sym = deps.resolve(parts[0])
        except Exception:
            return "Couldn't read that: check the ticker and price."
        if len(u["alerts"]) >= MAX_ALERTS_PER_USER:
            return f"Max {MAX_ALERTS_PER_USER} alerts. Remove one with /unalert N."
        u["alerts"].append({"sym": sym, "dir": parts[1].lower(), "price": price})
        store.save_user(cid, u)
        return f"Alert set: {esc(sym)} {parts[1].lower()} {price:,.2f}. It fires once."
    if cmd == "/alerts":
        if not u["alerts"]:
            return "No alerts. Try /alert TCS above 4200"
        return "\n".join(f"{i}. {esc(a['sym'])} {a['dir']} {a['price']:,.2f}" for i, a in enumerate(u["alerts"], 1))
    if cmd == "/unalert":
        try:
            u["alerts"].pop(int(arg) - 1)
        except Exception:
            return "Usage: /unalert N (see /alerts)"
        store.save_user(cid, u)
        return "Removed."
    return "Unknown command. /help lists what I can do."


def handle_update(deps, update):
    msg = update.get("message") or update.get("edited_message")
    if not msg or not msg.get("text") or msg["chat"].get("type") != "private":
        return
    cid = str(msg["chat"]["id"])
    if os.environ.get("VERCEL") and not store.redis:
        send(cid, "The bot's storage isn't configured yet (Redis env vars missing).")
        return
    try:
        reply = handle_text(deps, cid, msg["text"])
    except Exception as e:
        print("[telegram] command failed:", type(e).__name__, e)
        reply = "Something went wrong, please try again."
    if reply:
        send(cid, reply)


# ---------------------------------------------------------------- routes
def _authed(request: Request):
    if not CRON_SECRET:
        raise HTTPException(503, "CRON_SECRET is not set")
    got = request.headers.get("authorization", "")
    if not hmac.compare_digest(got, f"Bearer {CRON_SECRET}"):
        raise HTTPException(401, "unauthorized")


def register(app, deps):
    @app.post("/api/telegram/webhook")
    async def webhook(request: Request):
        if not TOKEN or not WEBHOOK_SECRET:
            raise HTTPException(503, "bot is not configured")
        if not hmac.compare_digest(request.headers.get("x-telegram-bot-api-secret-token", ""), WEBHOOK_SECRET):
            raise HTTPException(401, "bad secret")
        update = await request.json()
        try:
            await run_in_threadpool(handle_update, deps, update)
        except Exception as e:   # always 200, otherwise Telegram retries the same update
            print("[telegram] update failed:", e)
        return {"ok": True}

    @app.post("/api/cron/tick")
    async def cron_tick(request: Request):
        _authed(request)
        if not TOKEN:
            raise HTTPException(503, "TELEGRAM_BOT_TOKEN is not set")
        return await run_in_threadpool(tick, deps)

    @app.post("/api/telegram/setup")
    async def setup(request: Request):
        """One-time: point Telegram at this deployment and publish the command menu."""
        _authed(request)
        if not TOKEN or not WEBHOOK_SECRET:
            raise HTTPException(503, "set TELEGRAM_BOT_TOKEN and TELEGRAM_WEBHOOK_SECRET first")
        host = request.headers.get("x-forwarded-host") or request.headers.get("host")
        url = f"https://{host}/api/telegram/webhook"
        cmds = [("watch", "Add tickers: /watch TCS INFY"), ("unwatch", "Remove a ticker"),
                ("list", "Watchlist with live prices"), ("now", "Market update right now"),
                ("alert", "Price alert: /alert TCS above 4200"), ("alerts", "List price alerts"),
                ("digest", "3-hourly update on/off"), ("breaking", "Breaking alerts on/off"),
                ("help", "All commands"), ("stop", "Delete my data")]
        a = tg("setWebhook", url=url, secret_token=WEBHOOK_SECRET, allowed_updates=["message"]).json()
        b = tg("setMyCommands", commands=[{"command": c, "description": d} for c, d in cmds]).json()
        return {"webhook": url, "setWebhook": a, "setMyCommands": b}
