#!/usr/bin/env python3
"""Analyze evaluator backtest results — correlation and calibration."""

import sys
import math
import argparse
import uuid
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.validation_db import ValidationDatabase

import re

_URL_PATTERN = re.compile(r"https?://\S+")


def _is_link_only(text: str) -> bool:
    """True if tweet is primarily a URL with minimal original text."""
    stripped = _URL_PATTERN.sub("", text).strip()
    # Minimal text: 3 or fewer non-URL words
    return len(stripped.split()) <= 3


def spearman_rank_correlation(x: list[float], y: list[float]) -> float:
    """Compute Spearman rank correlation coefficient."""
    n = len(x)
    if n < 3:
        return 0.0

    def rank(values):
        sorted_idx = sorted(range(n), key=lambda i: values[i])
        ranks = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j < n - 1 and values[sorted_idx[j]] == values[sorted_idx[j + 1]]:
                j += 1
            avg_rank = (i + j) / 2.0 + 1
            for k in range(i, j + 1):
                ranks[sorted_idx[k]] = avg_rank
            i = j + 1
        return ranks

    rx, ry = rank(x), rank(y)
    d_sq = sum((a - b) ** 2 for a, b in zip(rx, ry))
    return 1 - (6 * d_sq) / (n * (n ** 2 - 1))


def pearson_correlation(x: list[float], y: list[float]) -> float:
    """Compute Pearson correlation coefficient."""
    n = len(x)
    if n < 3:
        return 0.0
    mx = sum(x) / n
    my = sum(y) / n
    sx = math.sqrt(sum((xi - mx) ** 2 for xi in x) / n)
    sy = math.sqrt(sum((yi - my) ** 2 for yi in y) / n)
    if sx == 0 or sy == 0:
        return 0.0
    return sum((xi - mx) * (yi - my) for xi, yi in zip(x, y)) / (n * sx * sy)


def quartile_precision(
    predicted: list[float], actual: list[float], quartile: str
) -> float:
    """Precision of identifying top/bottom quartile tweets."""
    n = len(predicted)
    q_size = max(1, n // 4)

    actual_sorted = sorted(
        range(n), key=lambda i: actual[i], reverse=(quartile == "top")
    )
    predicted_sorted = sorted(
        range(n), key=lambda i: predicted[i], reverse=(quartile == "top")
    )

    actual_set = set(actual_sorted[:q_size])
    predicted_set = set(predicted_sorted[:q_size])

    overlap = len(actual_set & predicted_set)
    return overlap / q_size if q_size > 0 else 0.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze backtest results")
    parser.add_argument(
        "--version", required=True,
        help="Evaluator version to analyze",
    )
    parser.add_argument(
        "--db-path", default="./validation.db",
        help="Path to validation database (default: ./validation.db)",
    )
    parser.add_argument(
        "--save-run", action="store_true",
        help="Save results as a backtest_run record",
    )
    parser.add_argument(
        "--text-only", action="store_true",
        help="Exclude link-only tweets (URLs with minimal text) from analysis",
    )
    args = parser.parse_args()

    db = ValidationDatabase(args.db_path)
    db.connect()

    evals = db.get_evaluations_for_version(args.version)
    if not evals:
        print(f"No evaluations found for version '{args.version}'")
        db.close()
        return

    if args.text_only:
        total_before = len(evals)
        evals = [e for e in evals if not _is_link_only(e.get("text", ""))]
        excluded = total_before - len(evals)
        print(f"Excluded {excluded} link-only tweets ({total_before} → {len(evals)})")

    print(f"Analyzing {len(evals)} evaluations for version '{args.version}'")
    print("=" * 70)

    # --- Overall correlation ---
    predicted = [e["predicted_score"] for e in evals]
    actual = [e["engagement_score"] for e in evals]
    actual_log = [math.log1p(a) for a in actual]

    sp_overall = spearman_rank_correlation(predicted, actual)
    pe_log = pearson_correlation(predicted, actual_log)
    top_q = quartile_precision(predicted, actual, "top")
    bot_q = quartile_precision(predicted, actual, "bottom")

    print(f"\nOVERALL METRICS (n={len(evals)})")
    print(f"  Spearman rank correlation:   {sp_overall:+.3f}")
    print(f"  Pearson (log engagement):    {pe_log:+.3f}")
    print(f"  Top-quartile precision:      {top_q:.1%}")
    print(f"  Bottom-quartile precision:   {bot_q:.1%}")

    # --- Within-account correlation ---
    by_account: dict[str, list[dict]] = defaultdict(list)
    for e in evals:
        by_account[e["username"]].append(e)

    account_spearman = []
    print(f"\nPER-ACCOUNT BREAKDOWN ({len(by_account)} accounts)")
    print(f"  {'Account':<20} {'n':>4} {'Spearman':>10} {'Top-Q':>8}")
    print(f"  {'-' * 20} {'---':>4} {'-' * 10} {'-' * 8}")

    for username, acct_evals in sorted(by_account.items()):
        if len(acct_evals) < 5:
            continue
        p = [e["predicted_score"] for e in acct_evals]
        a = [e["engagement_score"] for e in acct_evals]
        sp = spearman_rank_correlation(p, a)
        tq = quartile_precision(p, a, "top")
        account_spearman.append(sp)
        print(f"  @{username:<19} {len(acct_evals):>4} {sp:>+10.3f} {tq:>7.1%}")

    avg_within = (
        sum(account_spearman) / len(account_spearman) if account_spearman else 0.0
    )
    print(f"\n  Average within-account Spearman: {avg_within:+.3f}")

    # --- Per-criterion correlation ---
    criteria = [
        "hook_strength",
        "specificity",
        "emotional_resonance",
        "novelty",
        "actionability",
    ]
    print(f"\nPER-CRITERION CORRELATION WITH ACTUAL ENGAGEMENT")
    print(f"  {'Criterion':<25} {'Spearman':>10} {'Pearson(log)':>14}")
    print(f"  {'-' * 25} {'-' * 10} {'-' * 14}")

    for criterion in criteria:
        c_vals = [e[criterion] for e in evals]
        sp = spearman_rank_correlation(c_vals, actual)
        pe = pearson_correlation(c_vals, actual_log)
        print(f"  {criterion:<25} {sp:>+10.3f} {pe:>+14.3f}")

    # --- Score distribution ---
    print(f"\nSCORE DISTRIBUTION (predicted)")
    buckets: dict[int, int] = defaultdict(int)
    for s in predicted:
        buckets[int(s)] += 1
    for score in sorted(buckets.keys()):
        bar = "#" * buckets[score]
        print(f"  {score:>2}: {bar} ({buckets[score]})")

    # --- Engagement distribution ---
    print(f"\nENGAGEMENT DISTRIBUTION (actual)")
    eng_buckets = {"0": 0, "1-5": 0, "6-20": 0, "21-50": 0, "51-200": 0, "200+": 0}
    for a in actual:
        if a == 0:
            eng_buckets["0"] += 1
        elif a <= 5:
            eng_buckets["1-5"] += 1
        elif a <= 20:
            eng_buckets["6-20"] += 1
        elif a <= 50:
            eng_buckets["21-50"] += 1
        elif a <= 200:
            eng_buckets["51-200"] += 1
        else:
            eng_buckets["200+"] += 1
    for bucket, count in eng_buckets.items():
        bar = "#" * min(count, 60)
        pct = count / len(actual) * 100 if actual else 0
        print(f"  {bucket:>6}: {bar} ({count}, {pct:.0f}%)")

    # --- Misranked examples (biggest prediction errors) ---
    print(f"\nBIGGEST PREDICTION ERRORS (high predicted, low actual)")
    errors = []
    for e in evals:
        errors.append({
            "username": e["username"],
            "predicted": e["predicted_score"],
            "actual": e["engagement_score"],
            "error": e["predicted_score"] - math.log1p(e["engagement_score"]) * 2,
            "text": e["text"][:80],
        })
    errors.sort(key=lambda x: x["error"], reverse=True)
    for err in errors[:5]:
        print(
            f"  @{err['username']}: predicted={err['predicted']:.0f} "
            f"actual_eng={err['actual']:.0f}"
        )
        print(f"    {err['text']}...")

    print(f"\nBIGGEST PREDICTION ERRORS (low predicted, high actual)")
    for err in errors[-5:]:
        print(
            f"  @{err['username']}: predicted={err['predicted']:.0f} "
            f"actual_eng={err['actual']:.0f}"
        )
        print(f"    {err['text']}...")

    # --- Save run ---
    if args.save_run:
        run_id = str(uuid.uuid4())[:8]
        db.insert_backtest_run(
            run_id=run_id,
            evaluator_version=args.version,
            model=evals[0]["model"] if evals else "unknown",
            num_tweets=len(evals),
            num_accounts=len(by_account),
            spearman_overall=sp_overall,
            spearman_within_account=avg_within,
            pearson_log=pe_log,
            top_quartile_precision=top_q,
            bottom_quartile_precision=bot_q,
        )
        print(f"\nSaved as backtest run '{run_id}'")

    db.close()


if __name__ == "__main__":
    main()
