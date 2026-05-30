#!/usr/bin/env python3
"""Report LinkedIn engagement identity gaps."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from _focused_report_cli import positive_int, run  # noqa: E402
from evaluation.linkedin_engagement_identity_gaps import DEFAULT_LIMIT, build_linkedin_engagement_identity_gaps_report_from_db, format_linkedin_engagement_identity_gaps_json, format_linkedin_engagement_identity_gaps_text  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--limit", type=positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    return run(args, build_linkedin_engagement_identity_gaps_report_from_db, format_linkedin_engagement_identity_gaps_json, format_linkedin_engagement_identity_gaps_text, {"limit": args.limit})


if __name__ == "__main__":
    raise SystemExit(main())
