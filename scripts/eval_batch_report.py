#!/usr/bin/env python3
"""Compare recorded dry-run evaluation batches."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.eval_batch_report import (  # noqa: E402
    build_eval_batch_report,
    format_json_report,
    format_text_report,
)
from runner import script_context  # noqa: E402

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--baseline-batch-id",
        type=int,
        help="Baseline eval batch ID",
    )
    parser.add_argument(
        "--compare-batch-id",
        type=int,
        action="append",
        default=[],
        help="Comparison eval batch ID; repeat for multiple batches",
    )
    parser.add_argument(
        "--label",
        help="Optional label included in the report metadata",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Only include batches created in the last N days (default: 30)",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        report = build_eval_batch_report(
            db,
            args.baseline_batch_id,
            args.compare_batch_id,
            label=args.label,
            days=args.days,
        )
        if args.format == "json":
            print(format_json_report(report))
        else:
            print(format_text_report(report))


if __name__ == "__main__":
    main()
