"""
Market IQ — Question Generator
================================
Reads output/latest.json + Excel question bank, generates 1 question per
cohort (FNO + INTRADAY) using the claude CLI (no API key needed), while
avoiding skill repeats across the last 7 runs.

Usage:
    python3 generate_questions.py                    # default: claude CLI
    python3 generate_questions.py --provider openai  # needs OPENAI_API_KEY in config.env
"""

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, date
from pathlib import Path

import pandas as pd

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).parent
OUTPUT_DIR    = BASE_DIR / "output"
HISTORY_FILE  = OUTPUT_DIR / "question_history.json"
RESPONSE_FILE = OUTPUT_DIR / "response.json"
EXCEL_PATH    = BASE_DIR.parent / "market_iq_questions (2).xlsx"

# ── Constants ─────────────────────────────────────────────────────────────────
SKILLS = [
    "OI_AND_MARKET_STRUCTURE",
    "THETA_AND_EXPIRY",
    "IV_AND_VOLATILITY",
    "DELTA_AND_PRICE",
    "STRATEGY",
]
COHORTS        = ["FNO", "INTRADAY"]
HISTORY_WINDOW = 7


# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════

def read_config() -> dict:
    config = {}
    config_path = BASE_DIR / "config.env"
    if not config_path.exists():
        return config
    with open(config_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, val = line.partition("=")
                config[key.strip()] = val.strip()
    return config


# ══════════════════════════════════════════════════════════════════════════════
#  MARKET CONTEXT
# ══════════════════════════════════════════════════════════════════════════════

def _pct(ltp, prev):
    if ltp and prev and prev != 0:
        return round((ltp - prev) / prev * 100, 2)
    return None


def _calc_max_pain(strikes: list):
    all_sp = [s["strike_price"] for s in strikes if s.get("strike_price")]
    if not all_sp:
        return None
    min_pain  = float("inf")
    mp_strike = None
    for test in all_sp:
        total = 0
        for s in strikes:
            sp   = s.get("strike_price", 0)
            c_oi = s.get("call", {}).get("oi") or 0
            p_oi = s.get("put",  {}).get("oi") or 0
            total += max(test - sp, 0) * c_oi
            total += max(sp - test, 0) * p_oi
        if total < min_pain:
            min_pain  = total
            mp_strike = test
    return mp_strike


def extract_market_context(data: dict) -> dict:
    md  = data.get("market_data",   {})
    oc  = data.get("option_chains", {})
    nws = data.get("news",          {})

    def g(key, field, default=None):
        return md.get(key, {}).get(field, default)

    nifty_ltp  = g("NSE_INDEX|Nifty 50", "ltp")
    nifty_prev = g("NSE_INDEX|Nifty 50", "prev_close")
    nifty_high = g("NSE_INDEX|Nifty 50", "high")
    nifty_low  = g("NSE_INDEX|Nifty 50", "low")
    nifty_open = g("NSE_INDEX|Nifty 50", "open")
    nifty_vol  = g("NSE_INDEX|Nifty 50", "volume")
    vix_ltp    = g("NSE_INDEX|India VIX", "ltp")
    vix_prev   = g("NSE_INDEX|India VIX", "prev_close")
    bn_ltp     = g("NSE_INDEX|Nifty Bank", "ltp")
    bn_prev    = g("NSE_INDEX|Nifty Bank", "prev_close")

    atm_strike = round(nifty_ltp / 50) * 50 if nifty_ltp else None

    nifty_chain = oc.get("Nifty 50", {})
    strikes     = nifty_chain.get("strikes", [])

    atm_ce = atm_pe = None
    total_ce_oi = total_pe_oi = 0
    highest_ce_oi = highest_pe_oi = 0
    highest_ce_strike = highest_pe_strike = None

    for s in strikes:
        sp   = s.get("strike_price")
        call = s.get("call", {})
        put  = s.get("put",  {})
        c_oi = call.get("oi") or 0
        p_oi = put.get("oi")  or 0
        total_ce_oi += c_oi
        total_pe_oi += p_oi
        if c_oi > highest_ce_oi:
            highest_ce_oi = c_oi; highest_ce_strike = sp
        if p_oi > highest_pe_oi:
            highest_pe_oi = p_oi; highest_pe_strike = sp
        if sp == atm_strike:
            atm_ce = call; atm_pe = put

    pcr      = round(total_pe_oi / total_ce_oi, 2) if total_ce_oi else None
    max_pain = _calc_max_pain(strikes)

    expiry = nifty_chain.get("expiry")
    days_to_expiry = None
    if expiry:
        try:
            days_to_expiry = (datetime.strptime(expiry, "%Y-%m-%d").date() - date.today()).days
        except Exception:
            pass

    ctx = {
        "NIFTY_LTP":           nifty_ltp,
        "NIFTY_PREV_CLOSE":    nifty_prev,
        "NIFTY_DAY_HIGH":      nifty_high,
        "NIFTY_DAY_LOW":       nifty_low,
        "NIFTY_DAY_OPEN":      nifty_open,
        "NIFTY_PCT_CHANGE":    _pct(nifty_ltp, nifty_prev),
        "NIFTY_RANGE_TODAY":   round(nifty_high - nifty_low, 2) if nifty_high and nifty_low else None,
        "NIFTY_ATM_STRIKE":    atm_strike,
        "NIFTY_VOLUME":        nifty_vol,
        "VIX_CURRENT":         vix_ltp,
        "VIX_PREV_CLOSE":      vix_prev,
        "VIX_PCT_CHANGE":      _pct(vix_ltp, vix_prev),
        "BANKNIFTY_LTP":         bn_ltp,
        "BANKNIFTY_PCT_CHANGE":  _pct(bn_ltp, bn_prev),
        "ATM_CE_PRICE":          atm_ce.get("ltp")   if atm_ce else None,
        "ATM_PE_PRICE":          atm_pe.get("ltp")   if atm_pe else None,
        "STRADDLE_TOTAL_PREMIUM": round(
            (atm_ce.get("ltp") or 0) + (atm_pe.get("ltp") or 0), 1
        ) if atm_ce and atm_pe else None,
        "OPTION_DELTA":    atm_ce.get("delta") if atm_ce else None,
        "ATM_GAMMA":       atm_ce.get("gamma") if atm_ce else None,
        "IV_CURRENT":      atm_ce.get("iv")    if atm_ce else None,
        "CURRENT_PCR":          pcr,
        "MAX_PAIN_LEVEL":       max_pain,
        "HIGHEST_CE_OI_STRIKE": highest_ce_strike,
        "HIGHEST_CE_OI_LAKHS":  round(highest_ce_oi / 1e5, 1) if highest_ce_oi else None,
        "HIGHEST_PE_OI_STRIKE": highest_pe_strike,
        "HIGHEST_PE_OI_LAKHS":  round(highest_pe_oi / 1e5, 1) if highest_pe_oi else None,
        "DAYS_TO_EXPIRY":       days_to_expiry,
        "EXPIRY_DATE":          expiry,
        "TOP_HEADLINES": [
            {"headline": a["headline"], "sentiment": a["sentiment"]}
            for a in nws.get("articles", [])[:5]
        ],
    }
    return {k: v for k, v in ctx.items() if v is not None}


# ══════════════════════════════════════════════════════════════════════════════
#  QUESTION BANK
# ══════════════════════════════════════════════════════════════════════════════

def parse_question_bank(excel_path: Path) -> list:
    df = pd.read_excel(excel_path, sheet_name="Question Bank", header=1)
    samples = []
    for _, row in df.iterrows():
        tid    = str(row.get("Template ID", "")).strip()
        cohort = str(row.get("Cohort", "")).strip()
        skill  = str(row.get("Skill Map Tag", "")).strip()
        sample = str(row.get("Sample Question + Options", "")).strip()
        expl   = str(row.get("Explanation", "")).strip()
        if tid and cohort and skill:
            samples.append({"cohort": cohort, "skill": skill, "sample": sample, "explanation": expl})
    return samples


# ══════════════════════════════════════════════════════════════════════════════
#  HISTORY — 7-run skill rotation
# ══════════════════════════════════════════════════════════════════════════════

def load_history() -> list:
    if HISTORY_FILE.exists():
        return json.loads(HISTORY_FILE.read_text())
    return []


def pick_next_skill(cohort: str, history: list) -> str:
    used = [h["skill"] for h in history if h.get("cohort") == cohort]
    unused = [s for s in SKILLS if s not in used]
    if unused:
        return unused[0]
    # All 5 used at least once — pick the one absent from the most recent window
    recent = used[-len(SKILLS):]
    for skill in SKILLS:
        if skill not in recent:
            return skill
    return SKILLS[0]


def update_history(history: list, new_questions: list) -> list:
    for q in new_questions:
        history.append({
            "run_date":  str(date.today()),
            "cohort":    q["cohort"],
            "skill":     q["skill"],
            "q_hash":    hashlib.md5(q["question"].encode()).hexdigest()[:8],
            "q_snippet": q["question"][:80],
        })
    kept = []
    for cohort in COHORTS:
        entries = [h for h in history if h.get("cohort") == cohort]
        kept.extend(entries[-HISTORY_WINDOW:])
    HISTORY_FILE.write_text(json.dumps(kept, indent=2))
    return kept


# ══════════════════════════════════════════════════════════════════════════════
#  SYSTEM PROMPT
# ══════════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """\
You are Market IQ — a financial quiz engine for active Indian traders on Upstox.

Generate exactly ONE quiz question using the live market data provided.

STRICT RULES:
1. Use the actual market numbers — Nifty LTP, VIX, PCR, OI, ATM strike, greeks, etc.
2. Generate exactly 4 options (A / B / C / D). Exactly one correct. Wrong options must be plausible misconceptions.
3. Match the assigned cohort:
   - FNO: Options Greeks, OI, PCR, IV/VIX, F&O strategies, expiry mechanics
   - INTRADAY: Price action, support/resistance, volume, momentum, breakouts
4. Match the assigned skill tag exactly. Aim for medium or hard difficulty.
5. CRITICAL LENGTH LIMITS:
   - question: MAXIMUM 1 sentence (~25-35 words). State 1-2 key numbers and ask ONE thing.
   - options: MAXIMUM 8 words each. Be specific, no filler.
   - explanation: MAXIMUM 2 sentences. WHY is the answer correct — no restating.
6. Return ONLY valid JSON. No markdown, no code fences.

OUTPUT SCHEMA:
{
  "cohort": "FNO" or "INTRADAY",
  "skill": "<skill tag>",
  "question": "<1 sentence, max 35 words>",
  "options": {
    "A": "<max 8 words>",
    "B": "<max 8 words>",
    "C": "<max 8 words>",
    "D": "<max 8 words>"
  },
  "correct_option": "A" or "B" or "C" or "D",
  "explanation": "<2 sentences max>",
  "difficulty": "easy" or "medium" or "hard"
}
"""


# ── Learning Card System Prompt ───────────────────────────────────────────────

LEARNING_SYSTEM_PROMPT = """\
You are Market IQ — a financial education engine for Indian traders on Upstox.

Generate 2-3 market lesson cards from TODAY'S real market data.

RULES:
1. Use exact numbers from the market data.
2. Each card: time, headline, what, why, learning, dir
3. headline: 5-7 words, punchy, specific (e.g. "VIX held high despite recovery.")
4. what: 1 sentence with 1-2 key numbers — what happened.
5. why: 1-2 sentences — the market mechanism, no fluff.
6. learning: 1 sentence — one actionable takeaway.
7. dir: "up" / "down" / "neutral"
8. time: IST "HH:MM"
9. Cover different concepts across cards. No repeated insights.
10. Return ONLY a valid JSON array. No markdown, no code fences.

OUTPUT FORMAT:
[
  {
    "time": "HH:MM",
    "headline": "5-7 word headline.",
    "what": "1 sentence with numbers.",
    "why": "1-2 sentences on mechanism.",
    "learning": "1 sentence takeaway.",
    "dir": "up" | "down" | "neutral"
  }
]
"""


# ══════════════════════════════════════════════════════════════════════════════
#  PROMPT BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def build_prompt(cohort: str, skill: str, market_ctx: dict, samples: list, history: list) -> str:
    relevant = [s for s in samples if s["skill"] == skill][:2]
    if not relevant:
        relevant = samples[:2]

    recent = [h for h in history if h.get("cohort") == cohort][-5:]
    market_numbers = {k: v for k, v in market_ctx.items() if k != "TOP_HEADLINES"}

    parts = [
        SYSTEM_PROMPT,
        "---",
        f"Generate 1 quiz question for cohort={cohort}, skill={skill}.",
        "",
        "=== LIVE MARKET DATA (use these exact numbers) ===",
        json.dumps(market_numbers, indent=2),
    ]

    if market_ctx.get("TOP_HEADLINES"):
        parts += [
            "",
            "=== TODAY'S TOP HEADLINES ===",
            "\n".join(
                f"• [{h['sentiment'].upper()}] {h['headline']}"
                for h in market_ctx["TOP_HEADLINES"]
            ),
        ]

    parts += [
        "",
        "=== STYLE & DEPTH REFERENCE — do NOT copy, just match the quality ===",
    ]
    for s in relevant:
        parts.append(
            f"\nExample [{s['cohort']} / {s['skill']}]:\n"
            f"{s['sample']}\n"
            f"Explanation: {s['explanation']}"
        )

    if recent:
        parts += [
            "",
            "=== RECENT QUESTIONS — do NOT repeat the same scenario or concept ===",
        ]
        for h in recent:
            parts.append(f"• [{h['skill']}] {h['q_snippet']}...")

    parts += [
        "",
        f"Now generate ONE {cohort} question on skill: {skill}.",
        "Return ONLY the JSON object. Nothing else.",
    ]

    return "\n".join(parts)


# ══════════════════════════════════════════════════════════════════════════════
#  LEARNING CARD GENERATION
# ══════════════════════════════════════════════════════════════════════════════

def build_learning_prompt(market_ctx: dict) -> str:
    market_numbers = {k: v for k, v in market_ctx.items() if k != "TOP_HEADLINES"}
    parts = [
        LEARNING_SYSTEM_PROMPT,
        "---",
        "=== TODAY'S LIVE MARKET DATA ===",
        json.dumps(market_numbers, indent=2),
    ]
    if market_ctx.get("TOP_HEADLINES"):
        parts += [
            "",
            "=== TODAY'S TOP HEADLINES ===",
            "\n".join(
                f"• [{h['sentiment'].upper()}] {h['headline']}"
                for h in market_ctx["TOP_HEADLINES"]
            ),
        ]
    parts += [
        "",
        "Now generate 2-3 market lesson cards for today. Return ONLY the JSON array.",
    ]
    return "\n".join(parts)


def generate_learnings(market_ctx: dict, provider: str, cfg: dict) -> list:
    prompt = build_learning_prompt(market_ctx)
    try:
        raw = call_llm_raw(prompt, provider, cfg)
        # Strip accidental markdown fences
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        result = json.loads(raw.strip())
        if isinstance(result, list):
            return result
        return []
    except Exception as e:
        print(f"[learnings] failed: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
#  LLM BACKENDS
# ══════════════════════════════════════════════════════════════════════════════

def _run_claude_cli(prompt: str) -> str:
    """Runs claude CLI and returns raw stdout string."""
    import os
    env = os.environ.copy()
    env.pop("CLAUDECODE", None)   # allow spawning claude from inside a claude session
    result = subprocess.run(
        ["claude", "--print"],
        input=prompt,
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
    )
    if result.returncode != 0:
        raise RuntimeError(f"claude CLI error: {result.stderr.strip()}")
    return result.stdout.strip()


def call_claude_cli(prompt: str) -> dict:
    """Uses the locally installed `claude` CLI — no API key required."""
    raw = _run_claude_cli(prompt)
    # Strip accidental markdown fences
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())


def call_openai(prompt: str, cfg: dict) -> dict:
    from openai import OpenAI
    api_key = cfg.get("OPENAI_API_KEY", "")
    if not api_key:
        print("[ERROR] OPENAI_API_KEY not set in config.env")
        sys.exit(1)
    client = OpenAI(api_key=api_key)
    resp   = client.chat.completions.create(
        model           = "gpt-4o",
        temperature     = 0.8,
        response_format = {"type": "json_object"},
        messages        = [{"role": "user", "content": prompt}],
    )
    return json.loads(resp.choices[0].message.content)


def call_llm(prompt: str, provider: str, cfg: dict) -> dict:
    if provider == "openai":
        return call_openai(prompt, cfg)
    return call_claude_cli(prompt)   # default


def call_llm_raw(prompt: str, provider: str, cfg: dict) -> str:
    """Like call_llm but returns raw string (for array responses like learnings)."""
    if provider == "openai":
        from openai import OpenAI
        api_key = cfg.get("OPENAI_API_KEY", "")
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model="gpt-4o", temperature=0.8,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.choices[0].message.content
    return _run_claude_cli(prompt)


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Market IQ Question Generator")
    parser.add_argument(
        "--provider",
        choices=["claude", "openai"],
        default=None,
        help="LLM provider. Overrides LLM_PROVIDER in config.env. Default: claude",
    )
    args = parser.parse_args()

    cfg      = read_config()
    provider = args.provider or cfg.get("LLM_PROVIDER", "claude").lower()

    print(f"""
╔══════════════════════════════════════════════╗
║     Market IQ — Question Generator          ║
║  Provider : {provider:<33}║
╚══════════════════════════════════════════════╝
""")

    # ── Load market data ──────────────────────────────────────────────────────
    latest_path = OUTPUT_DIR / "latest.json"
    if not latest_path.exists():
        print("[ERROR] output/latest.json not found. Run run.py first.")
        sys.exit(1)

    data       = json.loads(latest_path.read_text())
    market_ctx = extract_market_context(data)
    print(
        f"[market]  Nifty {market_ctx.get('NIFTY_LTP')}  "
        f"({market_ctx.get('NIFTY_PCT_CHANGE', 0):+.2f}%)  "
        f"VIX {market_ctx.get('VIX_CURRENT')}  "
        f"PCR {market_ctx.get('CURRENT_PCR')}  "
        f"ATM {market_ctx.get('NIFTY_ATM_STRIKE')}  "
        f"DTE {market_ctx.get('DAYS_TO_EXPIRY', '?')}"
    )

    # ── Load sample questions ─────────────────────────────────────────────────
    if not EXCEL_PATH.exists():
        print(f"[ERROR] Excel not found: {EXCEL_PATH}")
        sys.exit(1)
    samples = parse_question_bank(EXCEL_PATH)
    print(f"[samples] {len(samples)} sample questions loaded")

    # ── Load history ──────────────────────────────────────────────────────────
    history = load_history()
    print(f"[history] {len(history)} past questions tracked\n")

    # ── Generate one question per cohort ──────────────────────────────────────
    questions = []
    for cohort in COHORTS:
        skill = pick_next_skill(cohort, history)
        print(f"[generate] {cohort} → skill: {skill} ... ", end="", flush=True)

        prompt = build_prompt(cohort, skill, market_ctx, samples, history)

        try:
            q = call_llm(prompt, provider, cfg)
            q.setdefault("cohort", cohort)
            q.setdefault("skill",  skill)
            q["id"] = f"GEN_{cohort}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            questions.append(q)
            print(f"✓ [{q.get('difficulty', '?')}]")
        except Exception as e:
            print(f"✗ {e}")

    if not questions:
        print("\n[ERROR] No questions generated.")
        sys.exit(1)

    # ── Update history ────────────────────────────────────────────────────────
    update_history(history, questions)

    # ── Generate learning cards ───────────────────────────────────────────────
    print("[generate] learnings ... ", end="", flush=True)
    learnings = generate_learnings(market_ctx, provider, cfg)
    print(f"✓  {len(learnings)} cards")

    # ── Save response.json ────────────────────────────────────────────────────
    output = {
        "generated_at":    datetime.now().isoformat(),
        "market_date":     str(date.today()),
        "provider":        provider,
        "market_snapshot": {k: v for k, v in market_ctx.items() if k != "TOP_HEADLINES"},
        "questions":       questions,
        "learnings":       learnings,
    }
    RESPONSE_FILE.write_text(json.dumps(output, indent=2))

    print(f"\n[done] Saved → output/response.json\n")
    for q in questions:
        print(f"  [{q['cohort']}] [{q['skill']}] correct={q.get('correct_option')}")
        print(f"  Q: {q.get('question', '')[:80]}...")
        print()


if __name__ == "__main__":
    main()
