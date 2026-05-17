#!/usr/bin/env python3
"""Report stale reused newsletter CTAs."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_cta_freshness import (  # noqa: E402
    DEFAULT_MIN_REUSE,
    DEFAULT_STALE_DAYS,
    build_newsletter_cta_freshness_report,
    format_newsletter_cta_freshness_json,
    format_newsletter_cta_freshness_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stale-days", type=int, default=DEFAULT_STALE_DAYS)
    parser.add_argument("--min-reuse", type=int, default=DEFAULT_MIN_REUSE)
    parser.add_argument("--format", choices=("json", "text"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_newsletter_cta_freshness_report(
                db,
                stale_days=args.stale_days,
                min_reuse=args.min_reuse,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_newsletter_cta_freshness_json(report)
        if args.format == "json"
        else format_newsletter_cta_freshness_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
