#!/usr/bin/env python3
"""Report content feedback revision adoption."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from _focused_report_cli import non_negative_int, positive_int, run  # noqa: E402
from evaluation.content_feedback_revision_adoption import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_AGE_HOURS,
    build_content_feedback_revision_adoption_report_from_db,
    format_content_feedback_revision_adoption_json,
    format_content_feedback_revision_adoption_text,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--min-age-hours", type=non_negative_int, default=DEFAULT_MIN_AGE_HOURS)
    parser.add_argument("--limit", type=positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    return run(
        args,
        build_content_feedback_revision_adoption_report_from_db,
        format_content_feedback_revision_adoption_json,
        format_content_feedback_revision_adoption_text,
        {"min_age_hours": args.min_age_hours, "limit": args.limit},
    )


if __name__ == "__main__":
    raise SystemExit(main())
