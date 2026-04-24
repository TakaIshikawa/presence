#!/usr/bin/env python3
"""Quarantine unhealthy curated sources."""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_quarantine import quarantine_curated_sources
from runner import script_context


def _print_report(report: dict) -> None:
    mode = "APPLY" if report["applied"] else "DRY RUN"
    print(f"CURATED SOURCE QUARANTINE ({mode})")
    print(
        f"Thresholds: failures >= {report['failure_threshold']}; "
        f"stale >= {report['stale_days']} days"
    )
    if report["source_type"]:
        print(f"Source type: {report['source_type']}")
    print(
        "Counts: "
        f"healthy={report['counts']['healthy']} "
        f"watch={report['counts']['watch']} "
        f"quarantine={report['counts']['quarantine']}"
    )
    print(
        f"Planned pauses: {report['planned_pauses']}; "
        f"updated: {report['updated']}"
    )
    print()

    if not report["sources"]:
        print("No curated sources found.")
        return

    print(
        f"{'Class':<12} {'Source':<28} {'Type':<12} {'Failures':<8} "
        f"{'Status':<10} Reason"
    )
    print("-" * 104)
    for source in report["sources"]:
        display = (
            f"@{source['identifier']}"
            if source["source_type"] == "x_account"
            else source["identifier"]
        )
        print(
            f"{source['classification']:<12} {display:<28} "
            f"{source['source_type']:<12} {source['consecutive_failures']!s:<8} "
            f"{source['status']:<10} {source['reason']}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Classify and optionally pause unhealthy curated sources."
    )
    parser.add_argument(
        "--failure-threshold",
        type=int,
        default=3,
        help="Consecutive failures required for quarantine (default: 3).",
    )
    parser.add_argument(
        "--stale-days",
        type=int,
        default=30,
        help="Days since last successful fetch required for quarantine (default: 30).",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Report planned changes without modifying the database (default).",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="Pause quarantined active sources.",
    )
    parser.add_argument(
        "--source-type",
        choices=["x_account", "blog", "newsletter"],
        help="Limit quarantine checks to one source type.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON.",
    )
    args = parser.parse_args()

    with script_context() as (_config, db):
        report = quarantine_curated_sources(
            db,
            failure_threshold=args.failure_threshold,
            stale_days=args.stale_days,
            source_type=args.source_type,
            apply=args.apply,
        )

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        _print_report(report)


if __name__ == "__main__":
    main()
