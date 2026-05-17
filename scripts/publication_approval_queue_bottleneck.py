#!/usr/bin/env python3
"""Report publication approval queue bottlenecks."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_approval_queue_bottleneck import (  # noqa: E402
    DEFAULT_CRITICAL_DAYS,
    DEFAULT_WARNING_DAYS,
    build_publication_approval_queue_bottleneck_report,
    format_publication_approval_queue_bottleneck_json,
    format_publication_approval_queue_bottleneck_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warning-days", type=int, default=DEFAULT_WARNING_DAYS)
    parser.add_argument("--critical-days", type=int, default=DEFAULT_CRITICAL_DAYS)
    parser.add_argument("--format", choices=("json", "text"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_publication_approval_queue_bottleneck_report(
                db,
                warning_days=args.warning_days,
                critical_days=args.critical_days,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_publication_approval_queue_bottleneck_json(report)
        if args.format == "json"
        else format_publication_approval_queue_bottleneck_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
