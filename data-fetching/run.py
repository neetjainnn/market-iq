"""
Market IQ — Data Fetching Agent
================================
Fetches raw market data for every NSE stock + index every hour.
Also pulls Indian market news from RSS feeds.
Saves one clean JSON per cycle to output/

Usage:
    python run.py

Setup:
    pip install -r requirements.txt
    Fill in config.env with your Upstox credentials.
"""

import sys
import json
import gzip
import time
import schedule
import requests
import feedparser
from datetime import datetime, date, timedelta
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
TOKEN_FILE = BASE_DIR / "output" / ".token_cache.json"
INST_CACHE = BASE_DIR / "output" / ".instruments_cache.json"
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Config ─────────────────────────────────────────────────────────────────
def read_config():
    """Read config.env as simple key=value pairs. No library quirks."""
    config = {}
    config_path = BASE_DIR / "config.env"
    if not config_path.exists():
        print(f"[ERROR] config.env not found at {config_path}")
        sys.exit(1)
    with open(config_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, val = line.partition("=")
                config[key.strip()] = val.strip()
    return config

cfg            = read_config()
ACCESS_TOKEN   = cfg.get("UPSTOX_ACCESS_TOKEN", "")
NEWS_API_KEY   = cfg.get("NEWS_API_KEY", "")
FETCH_INTERVAL = int(cfg.get("FETCH_INTERVAL_MINUTES", "60"))
FETCH_OOH      = cfg.get("FETCH_OUTSIDE_HOURS", "yes").lower() == "yes"

BASE_URL = "https://api.upstox.com/v2"

# ── Indices to always include ───────────────────────────────────────────────
INDEX_KEYS = [
    "NSE_INDEX|Nifty 50",
    "NSE_INDEX|Nifty Bank",
    "NSE_INDEX|Nifty IT",
    "NSE_INDEX|Nifty Auto",
    "NSE_INDEX|Nifty FMCG",
    "NSE_INDEX|Nifty Metal",
    "NSE_INDEX|Nifty Pharma",
    "NSE_INDEX|Nifty Midcap 100",
    "NSE_INDEX|Nifty Smallcap 100",
    "NSE_INDEX|India VIX",
]

# ── Nifty 100 symbols (Nifty 50 + Nifty Next 50) ───────────────────────────
NIFTY_100_SYMBOLS = {
    # Nifty 50
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BHARTIARTL", "BPCL",
    "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB", "DRREDDY",
    "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE",
    "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "INDUSINDBK",
    "INFY", "ITC", "JSWSTEEL", "KOTAKBANK", "LT",
    "M&M", "MARUTI", "NESTLEIND", "NTPC", "ONGC",
    "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SHRIRAMFIN",
    "SUNPHARMA", "TATACONSUM", "TMCV", "TMPV", "TATASTEEL", "TCS",
    "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO",
    # Nifty Next 50
    "ABB", "ADANIENSOL", "ADANIGREEN", "ADANIPOWER", "AMBUJACEM",
    "BAJAJHLDNG", "BEL", "BERGEPAINT", "BOSCHLTD", "CANBK",
    "CHOLAFIN", "COLPAL", "DMART", "GAIL", "GODREJCP",
    "GODREJPROP", "HAVELLS", "HINDPETRO", "ICICIGI", "ICICIPRULI",
    "IRFC", "JIOFIN", "JINDALSTEL", "JSWENERGY", "LODHA",
    "LTM", "LTTS", "MARICO", "MOTHERSON", "MPHASIS",
    "NHPC", "NYKAA", "OFSS", "PAGEIND", "PIDILITIND",
    "PIIND", "PNB", "RECLTD", "SIEMENS", "SRF",
    "TORNTPHARM", "TVSMOTOR", "UBL", "UNIONBANK", "UPL",
    "VBL", "VEDL", "ETERNAL", "ZYDUSLIFE", "SAIL",
}

# ── Option chain targets ────────────────────────────────────────────────────
OPTION_CHAIN_TARGETS = [
    ("NSE_INDEX|Nifty 50",   "Nifty 50"),
    ("NSE_INDEX|Nifty Bank", "Bank Nifty"),
]

# ── News RSS feeds ──────────────────────────────────────────────────────────
# ET returns HTML (blocked). Moneycontrol returns 403. Google News works reliably.
RSS_FEEDS = [
    {"name": "Google News — Indian Market",  "url": "https://news.google.com/rss/search?q=NSE+BSE+Indian+stock+market&hl=en-IN&gl=IN&ceid=IN:en"},
    {"name": "Google News — Nifty Sensex",   "url": "https://news.google.com/rss/search?q=Nifty+Sensex+today&hl=en-IN&gl=IN&ceid=IN:en"},
    {"name": "Google News — RBI FII",        "url": "https://news.google.com/rss/search?q=RBI+FII+DII+India+economy+market&hl=en-IN&gl=IN&ceid=IN:en"},
    {"name": "Google News — F&O",            "url": "https://news.google.com/rss/search?q=Nifty+options+futures+FnO+India&hl=en-IN&gl=IN&ceid=IN:en"},
    {"name": "Google News — Earnings",       "url": "https://news.google.com/rss/search?q=India+quarterly+results+earnings+profit&hl=en-IN&gl=IN&ceid=IN:en"},
]

BULLISH_WORDS = ["surge","rally","gain","rise","rises","jump","jumps","high","bull","buy",
                 "positive","growth","beat","beats","strong","record","upgrade","outperform",
                 "profit","recovery","rebound","breakout","above estimate","all-time high",
                 "soars","climbs","advances","buoyant","optimism","inflow"]
BEARISH_WORDS = ["fall","falls","drop","drops","decline","loss","down","low","bear","sell",
                 "negative","weak","miss","misses","downgrade","crash","concern","fear",
                 "worry","cut","below estimate","warning","selloff","slump","tumble",
                 "plunges","tanks","outflow","offload","pressure","caution"]

TAG_RULES = [
    ("results",  ["quarterly","q1","q2","q3","q4","earnings","profit","revenue","net profit","pat"]),
    ("ipo",      ["ipo","listing","subscription","allotment","grey market","gmp"]),
    ("rbi",      ["rbi","repo rate","monetary policy","mpc","inflation","cpi","wpi"]),
    ("fii",      ["fii","fpi","dii","foreign investor","institutional"]),
    ("budget",   ["budget","fiscal","finance minister","capex","deficit"]),
    ("global",   ["us market","fed","dow","nasdaq","s&p","crude","dollar","yuan","europe"]),
    ("rally",    ["surge","rally","soar","jump","climb","all-time high","52-week high"]),
    ("selloff",  ["crash","fall","plunge","selloff","slump","tumble","decline"]),
    ("merger",   ["merger","acquisition","takeover","stake","buyout","demerger"]),
    ("block",    ["block deal","bulk deal","insider"]),
]


# ══════════════════════════════════════════════════════════════════════════
#  AUTH
# ══════════════════════════════════════════════════════════════════════════

def load_token():
    if TOKEN_FILE.exists():
        data = json.loads(TOKEN_FILE.read_text())
        if data.get("date") == str(date.today()):
            return data.get("access_token")
    return None

def save_token(token):
    TOKEN_FILE.write_text(json.dumps({"access_token": token, "date": str(date.today())}))

def get_access_token():
    # 1. Token pasted directly in config.env — use it
    if ACCESS_TOKEN:
        save_token(ACCESS_TOKEN)
        return ACCESS_TOKEN
    # 2. Valid token cached from earlier today
    cached = load_token()
    if cached:
        return cached
    # 3. Nothing found — tell user exactly which file to edit
    config_path = BASE_DIR / "config.env"
    print("\n[ERROR] UPSTOX_ACCESS_TOKEN is empty.")
    print(f"\nOpen this file and paste your token:\n  {config_path}")
    print("\nIt should look like:")
    print("  UPSTOX_ACCESS_TOKEN=eyJ0eXAiOiJKV1Q...")
    print("\nNo spaces around the = sign. Save the file then run again.")
    sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════
#  INSTRUMENTS
# ══════════════════════════════════════════════════════════════════════════

def load_instruments():
    """Download NSE master file and return all instrument_keys (cached daily)."""
    today = str(date.today())
    if INST_CACHE.exists():
        cached = json.loads(INST_CACHE.read_text())
        if cached.get("date") == today:
            print(f"[instruments] Cached — {len(cached['keys'])} instruments")
            return cached["keys"]
    print("[instruments] Downloading NSE master file...")
    try:
        resp = requests.get(
            "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
            timeout=30,
        )
        import pandas as pd
        data = json.loads(gzip.decompress(resp.content))
        df   = pd.DataFrame(data)
        # Only Nifty 100 equities (skip ETFs, SGBs, warrants, all other stocks)
        mask = (
            (df["instrument_type"] == "EQ") &
            df["trading_symbol"].isin(NIFTY_100_SYMBOLS)
        )
        keys = df[mask]["instrument_key"].tolist()
        INST_CACHE.write_text(json.dumps({"date": today, "keys": keys}))
        print(f"[instruments] Loaded {len(keys)} Nifty 100 instruments")
        return keys
    except Exception as e:
        print(f"[instruments] Failed: {e} — falling back to index keys only")
        return INDEX_KEYS


# ══════════════════════════════════════════════════════════════════════════
#  MARKET DATA — raw per instrument
# ══════════════════════════════════════════════════════════════════════════

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def fetch_all_market_quotes(instrument_keys, headers):
    """
    Fetch Full Market Quotes for every instrument in batches of 500.
    Returns a flat dict keyed by instrument_key with raw data per stock/index.
    """
    all_data = {}
    batches  = list(chunks(instrument_keys, 500))

    for i, batch in enumerate(batches, 1):
        try:
            resp = requests.get(
                f"{BASE_URL}/market-quote/quotes",
                headers=headers,
                params={"instrument_key": ",".join(batch)},
                timeout=15,
            )
            if resp.status_code == 200:
                raw = resp.json().get("data", {})
                # Re-key to match original instrument_key format (API returns with ":" instead of "|")
                for api_key, d in raw.items():
                    original_key = api_key.replace(":", "|")
                    all_data[original_key] = {
                        "instrument_key": original_key,
                        "symbol":         original_key.split("|")[-1],
                        "exchange":       original_key.split("|")[0],
                        "ltp":            d.get("last_price"),
                        "open":           d.get("ohlc", {}).get("open"),
                        "high":           d.get("ohlc", {}).get("high"),
                        "low":            d.get("ohlc", {}).get("low"),
                        "prev_close":     d.get("ohlc", {}).get("close"),
                        "vwap":           d.get("average_price"),
                        "volume":         d.get("volume"),
                        "oi":             d.get("oi"),
                        "oi_day_high":    d.get("oi_day_high"),
                        "oi_day_low":     d.get("oi_day_low"),
                        "net_change":     d.get("net_change"),
                        "total_buy_qty":  d.get("total_buy_quantity"),
                        "total_sell_qty": d.get("total_sell_quantity"),
                        "lower_circuit":  d.get("lower_circuit_limit"),
                        "upper_circuit":  d.get("upper_circuit_limit"),
                        "last_trade_time":d.get("last_trade_time"),
                        "depth": {
                            "buy":  d.get("depth", {}).get("buy",  []),
                            "sell": d.get("depth", {}).get("sell", []),
                        },
                    }
                print(f"[market] Batch {i}/{len(batches)} — {len(all_data)} instruments collected")
            elif resp.status_code == 401:
                print("[ERROR] Token expired. Update UPSTOX_ACCESS_TOKEN in config.env and restart.")
                sys.exit(1)
            else:
                print(f"[market] Batch {i} failed — HTTP {resp.status_code}")
            time.sleep(0.3)
        except Exception as e:
            print(f"[market] Batch {i} error: {e}")

    return all_data


# ══════════════════════════════════════════════════════════════════════════
#  OPTION CHAIN — raw per strike
# ══════════════════════════════════════════════════════════════════════════

def nearest_expiry(instrument_key, headers):
    """Fetch available expiries from Upstox and return the nearest upcoming one."""
    try:
        resp = requests.get(
            f"{BASE_URL}/option/contract",
            headers=headers,
            params={"instrument_key": instrument_key},
            timeout=10,
        )
        expiries = sorted(set(
            d.get("expiry") for d in resp.json().get("data", []) if d.get("expiry")
        ))
        today_str = str(date.today())
        upcoming = [e for e in expiries if e >= today_str]
        if upcoming:
            return upcoming[0]
    except Exception as e:
        print(f"[options] Could not fetch expiries for {instrument_key}: {e}")
    # fallback: next Monday
    today = date.today()
    days = (0 - today.weekday()) % 7 or 7
    return (today + timedelta(days=days)).strftime("%Y-%m-%d")


def fetch_option_chain(instrument_key, label, headers):
    """
    Fetch raw option chain data — every strike with all available fields.
    """
    expiry = nearest_expiry(instrument_key, headers)
    try:
        resp = requests.get(
            f"{BASE_URL}/option/chain",
            headers=headers,
            params={"instrument_key": instrument_key, "expiry_date": expiry},
            timeout=15,
        )
        if resp.status_code != 200:
            print(f"[options] {label} — HTTP {resp.status_code}")
            return {}

        strikes_raw = resp.json().get("data", [])
        strikes     = []

        for item in strikes_raw:
            sp   = item.get("strike_price")
            call = item.get("call_options", {})
            put  = item.get("put_options",  {})

            strikes.append({
                "strike_price": sp,
                "call": {
                    "ltp":    call.get("market_data", {}).get("ltp"),
                    "oi":     call.get("market_data", {}).get("oi"),
                    "volume": call.get("market_data", {}).get("volume"),
                    "bid":    call.get("market_data", {}).get("bid_price"),
                    "ask":    call.get("market_data", {}).get("ask_price"),
                    "iv":     call.get("option_greeks", {}).get("iv"),
                    "delta":  call.get("option_greeks", {}).get("delta"),
                    "gamma":  call.get("option_greeks", {}).get("gamma"),
                    "theta":  call.get("option_greeks", {}).get("theta"),
                    "vega":   call.get("option_greeks", {}).get("vega"),
                },
                "put": {
                    "ltp":    put.get("market_data", {}).get("ltp"),
                    "oi":     put.get("market_data", {}).get("oi"),
                    "volume": put.get("market_data", {}).get("volume"),
                    "bid":    put.get("market_data", {}).get("bid_price"),
                    "ask":    put.get("market_data", {}).get("ask_price"),
                    "iv":     put.get("option_greeks", {}).get("iv"),
                    "delta":  put.get("option_greeks", {}).get("delta"),
                    "gamma":  put.get("option_greeks", {}).get("gamma"),
                    "theta":  put.get("option_greeks", {}).get("theta"),
                    "vega":   put.get("option_greeks", {}).get("vega"),
                },
            })

        print(f"[options] {label} — {len(strikes)} strikes fetched (expiry {expiry})")
        return {
            "instrument_key": instrument_key,
            "label":          label,
            "expiry":         expiry,
            "strikes":        strikes,
        }

    except Exception as e:
        print(f"[options] {label} error: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════════
#  NEWS
# ══════════════════════════════════════════════════════════════════════════

import hashlib
import re as _re

def _strip_html(text):
    from html import unescape
    return unescape(_re.sub(r"<[^>]+>", "", text)).strip()

def _article_id(title, link):
    raw = (title[:80] + link[:40]).encode()
    return hashlib.md5(raw).hexdigest()[:8]

def _sentiment(text):
    t    = text.lower()
    bull = sum(1 for w in BULLISH_WORDS if w in t)
    bear = sum(1 for w in BEARISH_WORDS if w in t)
    score = bull - bear
    if score > 0:  label = "bullish"
    elif score < 0: label = "bearish"
    else:           label = "neutral"
    return label, score

def _tags(text):
    t = text.lower()
    return [tag for tag, keywords in TAG_RULES if any(k in t for k in keywords)]

def _related_stocks(text):
    """Return only symbols that exist in NIFTY_100_SYMBOLS."""
    words = text.upper().split()
    found = []
    for w in words:
        sym = w.strip(".,()[]:-/\"'")
        if sym in NIFTY_100_SYMBOLS and sym not in found:
            found.append(sym)
    return found[:5]

def _normalize_dt(raw):
    """Best-effort parse of published date to ISO 8601 string."""
    if not raw:
        return ""
    import email.utils
    try:
        parsed = email.utils.parsedate_to_datetime(raw)
        return parsed.isoformat()
    except Exception:
        return str(raw)

def _build_article(title, summary, source, source_type, published_raw, link):
    text = title + " " + summary
    sentiment_label, sentiment_score = _sentiment(text)
    return {
        "id":              _article_id(title, link),
        "headline":        title,
        "summary":         _strip_html(summary)[:500],
        "source":          source,
        "source_type":     source_type,
        "published_at":    _normalize_dt(published_raw),
        "fetched_at":      datetime.now().isoformat(),
        "sentiment":       sentiment_label,
        "sentiment_score": sentiment_score,
        "related_stocks":  _related_stocks(title),
        "tags":            _tags(text),
    }

def fetch_news():
    articles = []
    seen     = set()   # deduplicate by article id
    by_source = {}

    # ── RSS feeds ──────────────────────────────────────────────────────────
    rss_headers = {
        "User-Agent": "Mozilla/5.0 (compatible; MarketIQ/1.0; +https://upstox.com)"
    }
    for feed_info in RSS_FEEDS:
        count = 0
        try:
            resp = requests.get(feed_info["url"], headers=rss_headers, timeout=10)
            feed = feedparser.parse(resp.content)  # pass bytes so feedparser detects encoding
            for entry in feed.entries[:15]:
                title   = (entry.get("title") or "").strip()
                summary = (entry.get("summary") or entry.get("description") or "").strip()
                link    = entry.get("link", "")
                if not title:
                    continue
                aid = _article_id(title, link)
                if aid in seen:
                    continue
                seen.add(aid)
                articles.append(_build_article(
                    title, summary,
                    source=feed_info["name"],
                    source_type="rss",
                    published_raw=entry.get("published", ""),
                    link=link,
                ))
                count += 1
        except Exception as e:
            print(f"[news] {feed_info['name']}: {e}")
        by_source[feed_info["name"]] = count
        print(f"[news] {feed_info['name']}: {count} articles")

    # ── NewsAPI ────────────────────────────────────────────────────────────
    if NEWS_API_KEY:
        count = 0
        try:
            resp = requests.get(
                "https://newsapi.org/v2/everything",
                params={
                    "q":        "India NSE BSE Nifty Sensex stock market",
                    "language": "en",
                    "sortBy":   "publishedAt",
                    "pageSize": 20,
                    "apiKey":   NEWS_API_KEY,
                },
                timeout=10,
            )
            for a in resp.json().get("articles", []):
                title   = (a.get("title") or "").strip()
                summary = (a.get("description") or "").strip()
                link    = a.get("url", "")
                if not title or title == "[Removed]":
                    continue
                aid = _article_id(title, link)
                if aid in seen:
                    continue
                seen.add(aid)
                articles.append(_build_article(
                    title, summary,
                    source=a.get("source", {}).get("name", "NewsAPI"),
                    source_type="newsapi",
                    published_raw=a.get("publishedAt", ""),
                    link=link,
                ))
                count += 1
        except Exception as e:
            print(f"[news] NewsAPI: {e}")
        by_source["NewsAPI"] = count
        print(f"[news] NewsAPI: {count} articles")

    # sort newest first (articles without dates go to bottom)
    articles.sort(key=lambda a: a["published_at"], reverse=True)

    print(f"[news] Total: {len(articles)} articles from {len(by_source)} sources")
    return {
        "fetched_at": datetime.now().isoformat(),
        "total":      len(articles),
        "by_source":  by_source,
        "articles":   articles,
    }


# ══════════════════════════════════════════════════════════════════════════
#  MAIN FETCH CYCLE
# ══════════════════════════════════════════════════════════════════════════

def run_fetch_cycle(token):
    now = datetime.now()
    print(f"\n{'═'*50}")
    print(f"  Fetch cycle — {now.strftime('%d %b %Y  %H:%M:%S')}")
    print(f"{'═'*50}\n")

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    # 1. All instrument keys
    instrument_keys = load_instruments()
    all_keys = list(set(instrument_keys + INDEX_KEYS))

    # 2. Raw market data for every stock + index
    print(f"[market] Fetching data for {len(all_keys)} instruments...")
    market_data = fetch_all_market_quotes(all_keys, headers)

    # 3. Raw option chain for each target
    print("\n[options] Fetching option chains...")
    option_chains = {}
    for inst_key, label in OPTION_CHAIN_TARGETS:
        option_chains[label] = fetch_option_chain(inst_key, label, headers)
        time.sleep(0.5)

    # 4. News
    print("\n[news] Fetching news...")
    news = fetch_news()

    # 5. Assemble output — clean, flat, no derived metrics
    output = {
        "meta": {
            "fetch_timestamp": now.isoformat(),
            "market_date":     str(date.today()),
            "total_instruments": len(market_data),
        },
        "market_data":   market_data,   # every stock + index, raw
        "option_chains": option_chains, # every strike, raw
        "news":          news,          # all articles with sentiment tag
    }

    # 6. Save
    latest = OUTPUT_DIR / "latest.json"
    latest.write_text(json.dumps(output, indent=2, default=str))

    print(f"\n[done] {len(market_data)} instruments | {news['total']} news articles")
    print(f"[done] Saved → output/latest.json\n")
    return output


# ══════════════════════════════════════════════════════════════════════════
#  SCHEDULER
# ══════════════════════════════════════════════════════════════════════════

def is_market_hours():
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    return 9 <= now.hour < 16


if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════╗
║      Market IQ — Data Fetching Agent         ║
║  Full Market Quotes + Option Chain + News    ║
╚══════════════════════════════════════════════╝
""")
    token = get_access_token()
    print(f"[auth] Token ready.\n")

    # Run immediately on startup
    run_fetch_cycle(token)

    def scheduled_run():
        t = load_token() or token
        run_fetch_cycle(t)

    schedule.every().day.at("13:00").do(scheduled_run)
    schedule.every().day.at("15:45").do(scheduled_run)
    print(f"[scheduler] Scheduled runs at 13:00 and 15:45 daily. Ctrl+C to stop.\n")
    while True:
        schedule.run_pending()
        time.sleep(30)
