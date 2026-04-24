#!/usr/bin/env python3
"""Generate a persona guard drift report."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.persona_drift_report import (  # noqa: E402
    PersonaDriftReporter,
    format_json_report,
    format_text_report,
)
from runner import script_context  # noqa: E402


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize persona guard outcomes over a recent date range."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to look back (default: 7)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output stable JSON instead of concise text.",
    )
    parser.add_argument(
        "--limit-failures",
        type=int,
        default=5,
        help="Maximum failed examples to include (default: 5)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv or [])
    with script_context() as (config, db):
        report = PersonaDriftReporter(db).build_report(
            days=args.days,
            limit_failures=args.limit_failures,
        )

    if args.json:
        print(format_json_report(report))
    else:
        print(format_text_report(report))


if __name__ == "__main__":
    main(sys.argv[1:])
