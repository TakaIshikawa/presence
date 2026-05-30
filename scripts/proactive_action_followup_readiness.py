#!/usr/bin/env python3
"""Report proactive action follow-up readiness."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from _focused_report_cli import non_negative_int, positive_int, run  # noqa: E402
from evaluation.proactive_action_followup_readiness import DEFAULT_LIMIT, DEFAULT_WINDOW_HOURS, build_proactive_action_followup_readiness_report_from_db, format_proactive_action_followup_readiness_json, format_proactive_action_followup_readiness_text  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--format", choices=("json", "text"), default="json")
    p.add_argument("--window-hours", type=non_negative_int, default=DEFAULT_WINDOW_HOURS)
    p.add_argument("--limit", type=positive_int, default=DEFAULT_LIMIT)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    return run(args, build_proactive_action_followup_readiness_report_from_db, format_proactive_action_followup_readiness_json, format_proactive_action_followup_readiness_text, {"window_hours": args.window_hours, "limit": args.limit})


if __name__ == "__main__":
    raise SystemExit(main())
