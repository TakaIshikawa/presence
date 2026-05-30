#!/usr/bin/env python3
"""Report profile metric following spikes."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from _focused_report_cli import non_negative_float, non_negative_int, positive_int, run  # noqa: E402
from evaluation.profile_metric_following_spikes import DEFAULT_FOLLOWING_DELTA_THRESHOLD, DEFAULT_LIMIT, DEFAULT_RATIO_THRESHOLD, build_profile_metric_following_spikes_report_from_db, format_profile_metric_following_spikes_json, format_profile_metric_following_spikes_text  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--format", choices=("json", "text"), default="json")
    p.add_argument("--following-delta-threshold", type=non_negative_int, default=DEFAULT_FOLLOWING_DELTA_THRESHOLD)
    p.add_argument("--ratio-threshold", type=non_negative_float, default=DEFAULT_RATIO_THRESHOLD)
    p.add_argument("--limit", type=positive_int, default=DEFAULT_LIMIT)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    return run(args, build_profile_metric_following_spikes_report_from_db, format_profile_metric_following_spikes_json, format_profile_metric_following_spikes_text, {"following_delta_threshold": args.following_delta_threshold, "ratio_threshold": args.ratio_threshold, "limit": args.limit})


if __name__ == "__main__":
    raise SystemExit(main())
