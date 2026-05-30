#!/usr/bin/env python3
"""Report engagement prediction component completeness."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from _focused_report_cli import non_negative_int, positive_int, run  # noqa: E402
from evaluation.engagement_prediction_component_completeness import DEFAULT_LIMIT, DEFAULT_MAX_BACKFILL_AGE_HOURS, build_engagement_prediction_component_completeness_report_from_db, format_engagement_prediction_component_completeness_json, format_engagement_prediction_component_completeness_text  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--format", choices=("json", "text"), default="json")
    p.add_argument("--max-backfill-age-hours", type=non_negative_int, default=DEFAULT_MAX_BACKFILL_AGE_HOURS)
    p.add_argument("--limit", type=positive_int, default=DEFAULT_LIMIT)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    return run(args, build_engagement_prediction_component_completeness_report_from_db, format_engagement_prediction_component_completeness_json, format_engagement_prediction_component_completeness_text, {"max_backfill_age_hours": args.max_backfill_age_hours, "limit": args.limit})


if __name__ == "__main__":
    raise SystemExit(main())
