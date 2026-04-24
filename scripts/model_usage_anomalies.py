#!/usr/bin/env python3
"""Report model operations with unusual token or cost usage."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.model_usage_anomalies import (
    build_model_usage_anomaly_report,
    format_model_usage_anomaly_report,
)
from runner import script_context


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days in the current and baseline windows (default: 7)",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=3,
        help="Minimum current and baseline samples before anomaly scoring (default: 3)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=2.0,
        help="Ratio or z-score threshold for anomalies (default: 2.0)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of text",
    )
    parser.add_argument(
        "--operation",
        action="append",
        default=[],
        help="Limit report to an operation_name. Repeat for multiple operations.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        report = build_model_usage_anomaly_report(
            db,
            days=args.days,
            min_samples=args.min_samples,
            threshold=args.threshold,
            operations=args.operation,
        )

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(format_model_usage_anomaly_report(report))


if __name__ == "__main__":
    main()
