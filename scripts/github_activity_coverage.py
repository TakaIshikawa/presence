#!/usr/bin/env python3
"""Report ingested GitHub activity that has not sourced generated content."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.github_activity_coverage import (
    SUPPORTED_ACTIVITY_TYPES,
    format_github_activity_coverage_text,
    uncovered_github_activity_report,
)
from runner import script_context


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", help="Only include activity from this repo_name")
    parser.add_argument(
        "--activity-type",
        choices=sorted(SUPPORTED_ACTIVITY_TYPES),
        help="Only include one GitHub activity type",
    )
    parser.add_argument("--state", help="Only include activity with this state")
    parser.add_argument(
        "--days",
        type=int,
        help="Only include activity updated within this many days",
    )
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        try:
            report = uncovered_github_activity_report(
                db,
                repo=args.repo,
                activity_type=args.activity_type,
                state=args.state,
                days=args.days,
            )
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(format_github_activity_coverage_text(report))


if __name__ == "__main__":
    main()
