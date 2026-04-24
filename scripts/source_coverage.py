#!/usr/bin/env python3
"""Report ingested source material not yet used by generated content."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.source_coverage import format_source_coverage, summarize_source_coverage
from runner import script_context


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Identify ingested commits, Claude messages, and GitHub activity "
            "that have not been used as generated_content source material."
        )
    )
    parser.add_argument("--days", type=int, default=30, help="Lookback window in days (default: 30)")
    parser.add_argument("--repo", help="Filter GitHub commits/activity by repository name")
    parser.add_argument("--json", action="store_true", help="Emit deterministic JSON output")
    parser.add_argument("--limit", type=int, default=10, help="Uncovered items to show per source type (default: 10)")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        report = summarize_source_coverage(
            db,
            days=args.days,
            repo=args.repo,
            limit=args.limit,
        )

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
    else:
        print(format_source_coverage(report))


if __name__ == "__main__":
    main()
