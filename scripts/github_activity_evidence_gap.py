#!/usr/bin/env python3
"""Report generated GitHub content missing supporting evidence."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.github_activity_evidence_gap import (  # noqa: E402
    build_github_activity_evidence_gap_report,
    format_github_activity_evidence_gap_json,
    format_github_activity_evidence_gap_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--format", choices=("json", "text"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_github_activity_evidence_gap_report(db)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_github_activity_evidence_gap_json(report)
        if args.format == "json"
        else format_github_activity_evidence_gap_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
