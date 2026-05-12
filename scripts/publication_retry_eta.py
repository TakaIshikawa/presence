#!/usr/bin/env python3
"""Report publication retry ETA buckets."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_retry_eta import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MAX_ATTEMPTS,
    build_publication_retry_eta_report,
    format_publication_retry_eta_json,
    format_publication_retry_eta_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--max-attempts", type=_positive_int, default=DEFAULT_MAX_ATTEMPTS)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_publication_retry_eta_report(
                db,
                limit=args.limit,
                max_attempts=args.max_attempts,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_publication_retry_eta_json(report)
        if args.format == "json"
        else format_publication_retry_eta_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
