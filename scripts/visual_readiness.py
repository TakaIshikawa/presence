#!/usr/bin/env python3
"""Report readiness of generated visual post assets."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from synthesis.visual_readiness import (
    build_visual_readiness_report,
    format_visual_readiness_report,
)


logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to look back for generated visual posts (default: 7)",
    )
    parser.add_argument(
        "--content-id",
        type=int,
        help="Inspect a single generated_content id instead of the lookback window",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON instead of a human-readable report",
    )
    parser.add_argument(
        "--missing-only",
        action="store_true",
        help="Only include assets that are not ready",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        report = build_visual_readiness_report(
            db,
            days=args.days,
            content_id=args.content_id,
            missing_only=args.missing_only,
        )

    if args.json:
        print(json.dumps(report.as_dict(), indent=2))
    else:
        print(format_visual_readiness_report(report))

    update_monitoring("visual_readiness")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
