#!/usr/bin/env python3
"""Report drift in published content mix across recent and baseline windows."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_mix_drift import (  # noqa: E402
    build_publication_mix_drift_report,
    format_publication_mix_drift_json,
    format_publication_mix_drift_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--recent-days",
        type=int,
        default=7,
        help="Number of days in the recent comparison window (default: 7)",
    )
    parser.add_argument(
        "--baseline-days",
        type=int,
        default=7,
        help="Number of days in the preceding baseline window (default: 7)",
    )
    parser.add_argument(
        "--drift-warning-points",
        type=float,
        default=20.0,
        help="Warn when share drift crosses this many percentage points (default: 20)",
    )
    parser.add_argument(
        "--format",
        choices=["json", "text"],
        default="text",
        help="Output format (default: text)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        with script_context() as (_config, db):
            report = build_publication_mix_drift_report(
                db,
                recent_days=args.recent_days,
                baseline_days=args.baseline_days,
                drift_warning_points=args.drift_warning_points,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_publication_mix_drift_json(report))
    else:
        print(format_publication_mix_drift_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
