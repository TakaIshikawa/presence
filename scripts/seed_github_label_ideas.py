#!/usr/bin/env python3
"""Seed content ideas from recent labeled GitHub activity."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.github_label_idea_seeder import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LABELS,
    DEFAULT_LIMIT,
    format_github_label_idea_results_json,
    format_github_label_idea_results_text,
    seed_github_label_ideas,
)


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _labels(value: str) -> list[str]:
    labels = [item.strip() for item in value.split(",") if item.strip()]
    if not labels:
        raise argparse.ArgumentTypeError("labels must include at least one value")
    return labels


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--labels",
        type=_labels,
        default=list(DEFAULT_LABELS),
        help=(
            "Comma-separated labels to include "
            f"(default: {', '.join(DEFAULT_LABELS)})."
        ),
    )
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for GitHub activity (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum eligible candidates to process (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Return candidates without inserting content_ideas rows.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            results = seed_github_label_ideas(
                db,
                labels=args.labels,
                days=args.days,
                limit=args.limit,
                dry_run=args.dry_run,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_github_label_idea_results_json(results))
    else:
        print(format_github_label_idea_results_text(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
