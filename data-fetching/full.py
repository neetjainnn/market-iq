"""
Market IQ — Full Pipeline
==========================
Runs everything in sequence:
  1. Fetch fresh market data  →  output/latest.json
  2. Generate quiz questions  →  output/response.json

Usage:
    python3 full.py
    python3 full.py --provider openai
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Banner ────────────────────────────────────────────────────────────────────

def banner(title: str):
    width = 50
    pad   = (width - len(title) - 2) // 2
    print(f"\n{'═' * width}")
    print(f"{'║'}{' ' * pad} {title} {' ' * (width - pad - len(title) - 3)}{'║'}")
    print(f"{'═' * width}\n")


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 1 — FETCH MARKET DATA
# ══════════════════════════════════════════════════════════════════════════════

def step_fetch():
    banner("Step 1 · Fetch Market Data")
    # Import run.py functions directly so we share the same process
    import run as r

    token = r.get_access_token()
    print(f"[auth]  Token ready.\n")

    output = r.run_fetch_cycle(token)

    md_count   = len(output.get("market_data", {}))
    news_count = output.get("news", {}).get("total", 0)
    print(f"\n✓  Market data  : {md_count} instruments")
    print(f"✓  News         : {news_count} articles")
    print(f"✓  Saved        : output/latest.json\n")
    return output


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 2 — GENERATE QUESTIONS
# ══════════════════════════════════════════════════════════════════════════════

def step_generate(provider: str):
    banner("Step 2 · Generate Quiz Questions")
    import generate_questions as gq

    cfg     = gq.read_config()
    output  = Path("output")
    latest  = output / "latest.json"

    import json
    data       = json.loads(latest.read_text())
    market_ctx = gq.extract_market_context(data)

    print(
        f"[market]  Nifty {market_ctx.get('NIFTY_LTP')}  "
        f"({market_ctx.get('NIFTY_PCT_CHANGE', 0):+.2f}%)  "
        f"VIX {market_ctx.get('VIX_CURRENT')}  "
        f"PCR {market_ctx.get('CURRENT_PCR')}  "
        f"ATM {market_ctx.get('NIFTY_ATM_STRIKE')}  "
        f"DTE {market_ctx.get('DAYS_TO_EXPIRY', '?')}"
    )

    samples = gq.parse_question_bank(gq.EXCEL_PATH)
    print(f"[samples] {len(samples)} sample questions loaded")

    history = gq.load_history()
    print(f"[history] {len(history)} past questions tracked\n")

    from datetime import date
    import hashlib

    questions = []
    for cohort in gq.COHORTS:
        skill = gq.pick_next_skill(cohort, history)
        print(f"[generate] {cohort} → {skill} ... ", end="", flush=True)

        prompt = gq.build_prompt(cohort, skill, market_ctx, samples, history)

        try:
            q = gq.call_llm(prompt, provider, cfg)
            q.setdefault("cohort", cohort)
            q.setdefault("skill",  skill)
            from datetime import datetime as dt
            q["id"] = f"GEN_{cohort}_{dt.now().strftime('%Y%m%d_%H%M%S')}"
            questions.append(q)
            print(f"✓  [{q.get('difficulty', '?')}]")
        except Exception as e:
            print(f"✗  {e}")

    if not questions:
        print("\n[ERROR] No questions generated.")
        sys.exit(1)

    gq.update_history(history, questions)

    print("[generate] learnings ... ", end="", flush=True)
    learnings = gq.generate_learnings(market_ctx, provider, cfg)
    print(f"✓  {len(learnings)} cards")

    from datetime import datetime as dt
    result = {
        "generated_at":    dt.now().isoformat(),
        "market_date":     str(date.today()),
        "provider":        provider,
        "market_snapshot": {k: v for k, v in market_ctx.items() if k != "TOP_HEADLINES"},
        "questions":       questions,
        "learnings":       learnings,
    }
    gq.RESPONSE_FILE.write_text(json.dumps(result, indent=2))

    print(f"\n✓  Saved : output/response.json\n")

    for q in questions:
        cohort_tag = q["cohort"]
        skill_tag  = q["skill"]
        diff_tag   = q.get("difficulty", "?")
        print(f"  [{cohort_tag}] [{skill_tag}] [{diff_tag}]")
        print(f"  Q: {q.get('question', '')[:80]}...")
        print(f"  ✓  {q['options'][q['correct_option']]}")
        print()

    return result


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Market IQ — Full Pipeline")
    parser.add_argument(
        "--provider",
        choices=["claude", "openai"],
        default=None,
        help="LLM provider for question generation (default: claude)",
    )
    args = parser.parse_args()

    start = time.time()

    print("""
╔══════════════════════════════════════════════════╗
║         Market IQ  —  Full Pipeline             ║
║   Fetch market data  →  Generate questions      ║
╚══════════════════════════════════════════════════╝""")

    # ── Step 1 ────────────────────────────────────────────────────────────────
    step_fetch()

    # ── Step 2 ────────────────────────────────────────────────────────────────
    import generate_questions as gq
    cfg      = gq.read_config()
    provider = args.provider or cfg.get("LLM_PROVIDER", "claude").lower()
    step_generate(provider)

    # ── Done ──────────────────────────────────────────────────────────────────
    elapsed = round(time.time() - start, 1)
    print(f"{'═' * 50}")
    print(f"  Pipeline complete in {elapsed}s")
    print(f"  Check output/response.json for today's questions.")
    print(f"{'═' * 50}\n")


if __name__ == "__main__":
    main()
