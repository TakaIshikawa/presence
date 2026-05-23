#!/usr/bin/env python3
"""Report eval batch score outliers."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from _focused_report_cli import non_negative_float, positive_int, run  # noqa: E402
from evaluation.eval_batch_score_outliers import DEFAULT_LIMIT, DEFAULT_MIN_BATCH_SIZE, DEFAULT_Z_THRESHOLD, build_eval_batch_score_outliers_report_from_db, format_eval_batch_score_outliers_json, format_eval_batch_score_outliers_text  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--format", choices=("json", "text"), default="json")
    p.add_argument("--z-threshold", type=non_negative_float, default=DEFAULT_Z_THRESHOLD)
    p.add_argument("--min-batch-size", type=positive_int, default=DEFAULT_MIN_BATCH_SIZE)
    p.add_argument("--limit", type=positive_int, default=DEFAULT_LIMIT)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    return run(args, build_eval_batch_score_outliers_report_from_db, format_eval_batch_score_outliers_json, format_eval_batch_score_outliers_text, {"z_threshold": args.z_threshold, "min_batch_size": args.min_batch_size, "limit": args.limit})


if __name__ == "__main__":
    raise SystemExit(main())
