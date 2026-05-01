#!/usr/bin/env python3
"""Seed content ideas from repeated Claude Code session error patterns."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.claude_error_idea_seeder import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_COUNT,
    ClaudeErrorIdeaCandidate,
    ClaudeErrorSeedResult,
    build_claude_error_idea_candidates,
    seed_claude_error_ideas,
)


def _shorten(text: str | None, width: int = 70) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def format_results_table(
    candidates: list[ClaudeErrorIdeaCandidate],
    seed_results: list[ClaudeErrorSeedResult] | None = None,
) -> str:
    lines = [
        f"candidates={len(candidates)}",
        f"{'Count':>5s}  {'Score':>5s}  {'Type':16s}  {'Last seen':20s}  Pattern",
        f"{'-' * 5:>5s}  {'-' * 5:>5s}  {'-' * 16:16s}  {'-' * 20:20s}  {'-' * 44}",
    ]
    if not candidates:
        lines.append("-----  -----  none              --------------------  no eligible error patterns")
    for candidate in candidates:
        lines.append(
            f"{candidate.occurrence_count:5d}  "
            f"{candidate.score:5.1f}  "
            f"{_shorten(candidate.failure_type, 16):16s}  "
            f"{_shorten(candidate.last_seen_at, 20):20s}  "
            f"{_shorten(candidate.normalized_phrase, 72)}"
        )

    if seed_results is not None:
        created = sum(1 for result in seed_results if result.status == "created")
        proposed = sum(1 for result in seed_results if result.status == "proposed")
        skipped = sum(1 for result in seed_results if result.status == "skipped")
        lines.append("")
        lines.append(f"seed_results created={created} proposed={proposed} skipped={skipped}")
        for result in seed_results:
            idea_id = str(result.idea_id) if result.idea_id is not None else "-"
            lines.append(
                f"  {result.status:8s} id={idea_id:>4s} "
                f"count={result.occurrence_count:2d} score={result.score:4.1f} "
                f"{_shorten(result.normalized_phrase, 58)}: {result.reason}"
            )
    return "\n".join(lines)


def format_results_json(
    candidates: list[ClaudeErrorIdeaCandidate],
    seed_results: list[ClaudeErrorSeedResult] | None = None,
) -> str:
    payload: dict[str, object] = {
        "candidates": [candidate.to_dict() for candidate in candidates],
    }
    if seed_results is not None:
        payload["seed_results"] = [result.to_dict() for result in seed_results]
    return json.dumps(payload, indent=2, sort_keys=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for Claude messages (default: {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=DEFAULT_MIN_COUNT,
        help=f"Minimum occurrences required for a candidate (default: {DEFAULT_MIN_COUNT})",
    )
    parser.add_argument("--limit", type=int, default=10, help="Maximum candidates to return")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report seed candidates without writing content ideas to the database",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    now = datetime.now(timezone.utc)
    with script_context() as (_config, db):
        seed_results = seed_claude_error_ideas(
            db,
            days=args.days,
            min_count=args.min_count,
            limit=args.limit,
            dry_run=args.dry_run,
            now=now,
        )
        candidates = build_claude_error_idea_candidates(
            db,
            days=args.days,
            min_count=args.min_count,
            limit=args.limit,
            now=now,
        )

    output = (
        format_results_json(candidates, seed_results)
        if args.json
        else format_results_table(candidates, seed_results)
    )
    print(output)


if __name__ == "__main__":
    main()
