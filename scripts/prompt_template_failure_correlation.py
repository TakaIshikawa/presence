#!/usr/bin/env python3
"""Report prompt template failure correlation."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.prompt_template_failure_correlation import DEFAULT_FAILURE_RATE_THRESHOLD, DEFAULT_LIMIT, DEFAULT_MIN_ATTEMPTS, DEFAULT_SCORE_THRESHOLD, build_prompt_template_failure_correlation_report_from_db, format_prompt_template_failure_correlation_json, format_prompt_template_failure_correlation_text  # noqa: E402
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--format", choices=("json", "text"), default="json")
    p.add_argument("--min-attempts", type=_positive_int, default=DEFAULT_MIN_ATTEMPTS)
    p.add_argument("--failure-rate-threshold", type=float, default=DEFAULT_FAILURE_RATE_THRESHOLD)
    p.add_argument("--score-threshold", type=float, default=DEFAULT_SCORE_THRESHOLD)
    p.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        a = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {"min_attempts": a.min_attempts, "failure_rate_threshold": a.failure_rate_threshold, "score_threshold": a.score_threshold, "limit": a.limit}
    try:
        if a.db:
            with sqlite3.connect(a.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_prompt_template_failure_correlation_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_prompt_template_failure_correlation_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_prompt_template_failure_correlation_text(report) if a.format == "text" else format_prompt_template_failure_correlation_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
