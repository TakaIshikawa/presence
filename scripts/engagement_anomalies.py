#!/usr/bin/env python3
"""Report engagement anomalies by platform and content format."""

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.anomaly_detector import EngagementAnomalyDetector
from runner import script_context

logger = logging.getLogger(__name__)


def format_report(anomalies, days: int, platform: str | None) -> str:
    """Format anomalies as human-readable text."""
    platform_label = platform or "all platforms"
    lines = [
        "=" * 70,
        "ENGAGEMENT ANOMALIES",
        "=" * 70,
        f"Window: last {days} days | Platform: {platform_label}",
        "",
    ]

    if not anomalies:
        lines.append("No engagement anomalies found.")
        return "\n".join(lines)

    for index, anomaly in enumerate(anomalies, 1):
        preview = anomaly.content_preview
        if len(preview) > 80:
            preview = preview[:77] + "..."
        direction = anomaly.direction.upper()
        lines.append(
            f"{index}. {direction} [{anomaly.platform} / {anomaly.content_format}] "
            f"content_id={anomaly.content_id}"
        )
        lines.append(
            f"   Score {anomaly.engagement_score:.1f} vs median "
            f"{anomaly.baseline_median:.1f} "
            f"(delta {anomaly.score_delta:+.1f}, z {anomaly.robust_z_score:+.2f}, "
            f"n={anomaly.baseline_sample_count})"
        )
        lines.append(f"   {preview}")
        lines.append("")

    return "\n".join(lines).rstrip()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Identify posts whose engagement is anomalous versus recent platform/format baselines."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to look back (default: 30)",
    )
    parser.add_argument(
        "--platform",
        choices=["x", "bluesky"],
        help="Limit analysis to one platform",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON",
    )
    parser.add_argument(
        "--min-score-delta",
        type=float,
        default=5.0,
        help="Minimum absolute score delta from baseline median (default: 5.0)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        detector = EngagementAnomalyDetector(db)
        anomalies = detector.detect_anomalies(
            days=args.days,
            platform=args.platform,
            min_score_delta=args.min_score_delta,
        )

    if args.json:
        print(json.dumps([anomaly.to_dict() for anomaly in anomalies], indent=2))
    else:
        print(format_report(anomalies, days=args.days, platform=args.platform))


if __name__ == "__main__":
    main()
