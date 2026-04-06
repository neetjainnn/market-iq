#!/usr/bin/env python3
"""
Market IQ — Live Data Pipeline
Upstox WebSocket → Trigger Detection → LLM Content Generation

SETUP:
1. pip install requests websocket-client anthropic python-dotenv protobuf upstox-client
2. Create a .env file in the same folder with:
   UPSTOX_ACCESS_TOKEN=your_token_here
   ANTHROPIC_API_KEY=your_anthropic_key_here
3. Run: python market_iq_pipeline.py
"""

import os
import json
import time
import requests
import websocket
import threading
from datetime import datetime
from dotenv import load_dotenv
import anthropic

# ─── Load secrets from .env file, never hardcode ───────────────────────────
load_dotenv()
UPSTOX_TOKEN   = os.getenv("UPSTOX_ACCESS_TOKEN")
ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY")

if not UPSTOX_TOKEN or not ANTHROPIC_KEY:
    raise ValueError("Missing UPSTOX_ACCESS_TOKEN or ANTHROPIC_API_KEY in .env file")

# ─── Config ─────────────────────────────────────────────────────────────────
INSTRUMENTS = [
    "NSE_INDEX|Nifty 50",
    "NSE_INDEX|Nifty Bank",
]

TRIGGER_THRESHOLD_PCT = 1.0   # fire experiment if move > 1% or < -1%
COOLDOWN_SECONDS      = 900   # don't fire again for same instrument within 15 min

# ─── State ──────────────────────────────────────────────────────────────────
baseline_prices   = {}   # { instrument_key: close_price }
last_trigger_time = {}   # { instrument_key: timestamp } — cooldown tracker
market_is_open    = False


# ════════════════════════════════════════════════════════════════════════════
# STEP 1 — Get authorized WebSocket URL
# ════════════════════════════════════════════════════════════════════════════

def get_ws_url():
    print("[1/4] Getting authorized WebSocket URL...")
    res = requests.get(
        "https://api.upstox.com/v3/feed/market-data-feed/authorize",
        headers={
            "Authorization": f"Bearer {UPSTOX_TOKEN}",
            "Accept": "*/*"
        }
    )
    if res.status_code != 200:
        raise Exception(f"Auth failed: {res.status_code} — {res.text}")

    url = res.json()["data"]["authorizedRedirectUri"]
    print(f"[1/4] Got WebSocket URL ✓")
    return url


# ════════════════════════════════════════════════════════════════════════════
# STEP 2 — Decode incoming Protobuf messages
# ════════════════════════════════════════════════════════════════════════════

def decode_message(raw_bytes):
    """
    Upstox sends Protobuf-encoded binary.
    Using upstox_client SDK to decode — handles proto file automatically.
    Falls back to raw bytes info if decode fails.
    """
    try:
        from upstox_client.feeder.proto.MarketDataFeed_pb2 import FeedResponse
        feed = FeedResponse()
        feed.ParseFromString(raw_bytes)

        result = {}

        # Market info (first tick)
        if feed.HasField("marketInfo"):
            result["type"] = "market_info"
            result["segments"] = {}
            for seg in feed.marketInfo.segmentStatus:
                result["segments"][seg.segment] = seg.status.name
            return result

        # Live feed ticks
        if feed.feeds:
            result["type"] = "live_feed"
            result["timestamp"] = datetime.now().strftime("%H:%M:%S")
            result["data"] = {}
            for key, val in feed.feeds.items():
                entry = {}
                # LTPC
                if val.HasField("ltpc"):
                    entry["ltp"] = val.ltpc.ltp
                    entry["cp"]  = val.ltpc.cp
                    if val.ltpc.cp > 0:
                        entry["pct_change"] = round(
                            (val.ltpc.ltp - val.ltpc.cp) / val.ltpc.cp * 100, 2
                        )
                # Full feed
                if val.HasField("fullFeed"):
                    ff = val.fullFeed.marketFF
                    entry["ltp"]    = ff.ltpc.ltp
                    entry["cp"]     = ff.ltpc.cp
                    entry["volume"] = ff.vtt
                    entry["oi"]     = ff.oi
                    entry["atp"]    = ff.atp
                    entry["tbq"]    = ff.tbq
                    entry["tsq"]    = ff.tsq
                    if ff.ltpc.cp > 0:
                        entry["pct_change"] = round(
                            (ff.ltpc.ltp - ff.ltpc.cp) / ff.ltpc.cp * 100, 2
                        )
                    # OHLC
                    for ohlc in ff.marketOHLC.ohlc:
                        if ohlc.interval == "1d":
                            entry["ohlc_daily"] = {
                                "open": ohlc.open, "high": ohlc.high,
                                "low": ohlc.low,  "close": ohlc.close,
                                "volume": ohlc.vol
                            }
                result["data"][key] = entry
            return result

    except Exception as e:
        return {"type": "decode_error", "error": str(e), "raw_length": len(raw_bytes)}

    return {"type": "unknown"}


# ════════════════════════════════════════════════════════════════════════════
# STEP 3 — Trigger detection
# ════════════════════════════════════════════════════════════════════════════

def check_trigger(instrument_key, entry):
    """
    Returns trigger dict if conditions met, None otherwise.
    """
    if "pct_change" not in entry:
        return None

    pct   = entry["pct_change"]
    ltp   = entry.get("ltp", 0)
    cp    = entry.get("cp", 0)
    now   = time.time()

    # Check cooldown — don't fire repeatedly for same move
    last = last_trigger_time.get(instrument_key, 0)
    if now - last < COOLDOWN_SECONDS:
        return None

    # Check threshold
    if abs(pct) >= TRIGGER_THRESHOLD_PCT:
        direction = "up" if pct > 0 else "down"
        trigger = {
            "instrument": instrument_key,
            "ltp": ltp,
            "close_price": cp,
            "pct_change": pct,
            "direction": direction,
            "time": datetime.now().strftime("%H:%M"),
            "date": datetime.now().strftime("%Y-%m-%d"),
            "volume": entry.get("volume", "N/A"),
            "oi": entry.get("oi", "N/A"),
            "ohlc": entry.get("ohlc_daily", {}),
        }
        last_trigger_time[instrument_key] = now
        print(f"\n🚨 TRIGGER: {instrument_key} moved {pct:+.2f}% | LTP: {ltp}")
        return trigger

    return None


# ════════════════════════════════════════════════════════════════════════════
# STEP 4 — GenAI content generation
# ════════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """
You are the content engine for Market IQ inside the Upstox trading app.
Your job is financial education for new equity users — people who just opened a Demat account.

HARD RULES — never break these:
1. Never say buy, sell, invest, avoid, target, recommend
2. Keep each body field under 50 words
3. Use plain English — no jargon without explaining it
4. Bold only numbers and the single most important cause per block
5. data_points: only include if real numbers exist. Empty array [] if not.

OUTPUT: strict JSON only, no text outside the JSON, no markdown code blocks.

JSON schema to follow exactly:
{
  "id": "exp_XXX",
  "time": "HH:MM",
  "date": "YYYY-MM-DD",
  "concept_tag": "short concept name",
  "headline": "plain english headline under 12 words",
  "what_happened": {
    "title": "What happened",
    "body": "under 50 words, bold key numbers"
  },
  "why_happened": {
    "title": "Why it happened",
    "body": "under 50 words, bold the key cause"
  },
  "learning": {
    "title": "Learning",
    "body": "one rule they can carry forward, under 30 words"
  },
  "data_points": [
    { "label": "string", "value": "string", "direction": "up|down|neutral" }
  ]
}
"""

def generate_experiment(trigger_data):
    """
    Send trigger data to LLM, get back experiment JSON.
    """
    print(f"\n[4/4] Sending to LLM for content generation...")

    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

    user_message = f"""
Generate a Live Experiment card for new equity users based on this market event:

Instrument: {trigger_data['instrument']}
Current price (LTP): {trigger_data['ltp']}
Yesterday close: {trigger_data['close_price']}
Move: {trigger_data['pct_change']:+.2f}%
Direction: {trigger_data['direction']}
Time: {trigger_data['time']}
Date: {trigger_data['date']}
Volume today: {trigger_data.get('volume', 'N/A')}
Open Interest: {trigger_data.get('oi', 'N/A')}
Day OHLC: {json.dumps(trigger_data.get('ohlc', {}), indent=2)}

Cohort: new equity user (just opened Demat, no prior trading knowledge)
Concept to teach: what causes index moves of this magnitude and what it means for their holdings

Generate the experiment JSON now.
"""

    message = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}]
    )

    raw = message.content[0].text.strip()

    # Parse JSON — strip any accidental markdown wrapping
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    experiment = json.loads(raw)
    return experiment


# ════════════════════════════════════════════════════════════════════════════
# STEP 5 — Save and display output
# ════════════════════════════════════════════════════════════════════════════

def save_experiment(experiment):
    filename = f"experiment_{experiment.get('id', 'unknown')}_{experiment.get('date', 'unknown')}.json"
    with open(filename, "w") as f:
        json.dump(experiment, f, indent=2)
    print(f"\n✅ Experiment saved → {filename}")
    print("\n" + "="*60)
    print("GENERATED EXPERIMENT:")
    print("="*60)
    print(json.dumps(experiment, indent=2))
    print("="*60 + "\n")


# ════════════════════════════════════════════════════════════════════════════
# WebSocket handlers
# ════════════════════════════════════════════════════════════════════════════

def on_message(ws, raw):
    global market_is_open

    if isinstance(raw, str):
        # Sometimes arrives as text JSON (market_info in some versions)
        try:
            msg = json.loads(raw)
            print(f"[TEXT] {msg.get('type', 'unknown')}")
        except:
            pass
        return

    # Binary — decode Protobuf
    msg = decode_message(raw)

    # ── Market info tick (first message) ──
    if msg["type"] == "market_info":
        print("\n[MARKET STATUS]")
        for seg, status in msg.get("segments", {}).items():
            print(f"  {seg}: {status}")
        nse_status = msg.get("segments", {}).get("NSE_EQ", "UNKNOWN")
        nse_index  = msg.get("segments", {}).get("NSE_INDEX", "UNKNOWN")
        market_is_open = nse_status in ["NORMAL_OPEN", "PRE_OPEN_NIFTY"]
        print(f"\nMarket open for triggers: {market_is_open}")
        return

    # ── Live feed tick ──
    if msg["type"] == "live_feed":
        for instrument_key, entry in msg.get("data", {}).items():
            pct = entry.get("pct_change", 0)
            ltp = entry.get("ltp", 0)

            # Print every tick so you can see data flowing
            print(f"[{msg['timestamp']}] {instrument_key} | "
                  f"LTP: {ltp} | Change: {pct:+.2f}%")

            # Check trigger
            if market_is_open:
                trigger = check_trigger(instrument_key, entry)
                if trigger:
                    # Run content generation in background thread
                    # so WebSocket doesn't block
                    t = threading.Thread(
                        target=lambda: save_experiment(
                            generate_experiment(trigger)
                        )
                    )
                    t.daemon = True
                    t.start()

    if msg["type"] == "decode_error":
        print(f"[DECODE ERROR] {msg['error']}")


def on_open(ws):
    print("\n[2/4] WebSocket connected ✓")
    print("[3/4] Subscribing to instruments...")

    subscription = {
        "guid": "marketiq_001",
        "method": "sub",
        "data": {
            "mode": "full",
            "instrumentKeys": INSTRUMENTS
        }
    }

    ws.send(json.dumps(subscription).encode())
    print(f"[3/4] Subscribed to: {INSTRUMENTS} in full mode ✓")
    print("\n--- Listening for market data ---\n")


def on_error(ws, error):
    print(f"[WS ERROR] {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"\n[WS CLOSED] {close_status_code} — {close_msg}")
    print("Reconnecting in 5 seconds...")
    time.sleep(5)
    run()


# ════════════════════════════════════════════════════════════════════════════
# Main runner with auto-reconnect
# ════════════════════════════════════════════════════════════════════════════

def run():
    ws_url = get_ws_url()

    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws.run_forever(
        ping_interval=30,
        ping_timeout=10
    )


if __name__ == "__main__":
    print("="*60)
    print("Market IQ — Live Experiment Pipeline")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Trigger threshold: ±{TRIGGER_THRESHOLD_PCT}%")
    print(f"Watching: {INSTRUMENTS}")
    print("="*60 + "\n")
    run()
