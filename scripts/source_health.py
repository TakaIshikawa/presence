#!/usr/bin/env python3
"""Check and enforce curated source health."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_health import (  # noqa: E402
    find_sources_to_pause,
    pause_failing_sources,
    restore_sources,
    source_failure_threshold_from_config,
)
from runner import script_context, update_monitoring  # noqa: E402

logger = logging.getLogger(__name__)


def _source_label(row: dict) -> str:
    identifier = row.get("identifier") or ""
    return f"@{identifier}" if row.get("source_type") == "x_account" else identifier


def _decision_dicts(decisions) -> list[dict]:
    return [decision.as_dict() for decision in decisions]


def _split_restore_targets(targets: list[str]) -> tuple[list[int], list[str]]:
    source_ids = []
    identifiers = []
    for target in targets:
        if target.isdigit():
            source_ids.append(int(target))
        else:
            identifiers.append(target)
    return source_ids, identifiers


def _print_pause_candidates(decisions, *, action: str, dry_run: bool) -> None:
    if not decisions:
        print("No curated sources meet the auto-pause criteria.")
        return

    verb = "would be paused" if dry_run else action
    print(f"Curated sources that {verb}:")
    print(f"{'ID':<5} {'Source':<28} {'Type':<12} {'Failures':<9} {'Threshold':<9} Last Error")
    print("-" * 100)
    for decision in decisions:
        error = (decision.last_error or "").replace("\n", " ")
        if len(error) > 70:
            error = error[:67] + "..."
        print(
            f"{decision.id:<5} {_source_label(decision.as_dict()):<28} "
            f"{decision.source_type:<12} {decision.consecutive_failures:<9} "
            f"{decision.threshold:<9} {error}"
        )
        print(f"{'':<5} last_failure_at={decision.last_failure_at or 'unknown'}")


def _print_restored(rows: list[dict], *, dry_run: bool) -> None:
    if not rows:
        print("No paused curated sources matched the restore target.")
        return

    verb = "would be restored" if dry_run else "restored"
    print(f"Curated sources {verb}:")
    print(f"{'ID':<5} {'Source':<28} {'Type':<12} {'Failures':<9} Last Error")
    print("-" * 88)
    for row in rows:
        error = (row.get("last_error") or "").replace("\n", " ")
        if len(error) > 60:
            error = error[:57] + "..."
        print(
            f"{row.get('id'):<5} {_source_label(row):<28} "
            f"{row.get('source_type') or '':<12} "
            f"{row.get('consecutive_failures') or 0:<9} {error}"
        )


def _add_common_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show matching sources without changing rows",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage curated source health")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check", help="List sources that meet pause criteria")
    _add_common_options(check)

    pause = subparsers.add_parser("pause", help="Pause sources that meet pause criteria")
    _add_common_options(pause)

    restore = subparsers.add_parser("restore", help="Reactivate paused sources")
    restore.add_argument(
        "targets",
        nargs="*",
        help="Source IDs or identifiers to restore",
    )
    restore.add_argument(
        "--id",
        dest="source_ids",
        type=int,
        action="append",
        default=[],
        help="Paused curated_sources.id to restore",
    )
    restore.add_argument(
        "--identifier",
        dest="identifiers",
        action="append",
        default=[],
        help="Paused curated_sources.identifier to restore",
    )
    _add_common_options(restore)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (config, db):
        threshold = source_failure_threshold_from_config(config)

        if args.command == "check":
            decisions = find_sources_to_pause(db, threshold)
            if args.json:
                print(
                    json.dumps(
                        {
                            "command": "check",
                            "dry_run": args.dry_run,
                            "threshold": threshold,
                            "candidates": _decision_dicts(decisions),
                        },
                        indent=2,
                    )
                )
            else:
                _print_pause_candidates(decisions, action="would be paused", dry_run=True)
            return

        if args.command == "pause":
            decisions = pause_failing_sources(db, threshold, dry_run=args.dry_run)
            if args.json:
                print(
                    json.dumps(
                        {
                            "command": "pause",
                            "dry_run": args.dry_run,
                            "threshold": threshold,
                            "paused": _decision_dicts(decisions),
                        },
                        indent=2,
                    )
                )
            else:
                _print_pause_candidates(
                    decisions,
                    action="paused",
                    dry_run=args.dry_run,
                )
            if decisions and not args.dry_run:
                update_monitoring("source_health")
            return

        target_ids, target_identifiers = _split_restore_targets(args.targets)
        source_ids = [*args.source_ids, *target_ids]
        identifiers = [*args.identifiers, *target_identifiers]
        if not source_ids and not identifiers:
            parser.error("restore requires at least one ID or identifier")
        rows = restore_sources(
            db,
            source_ids=source_ids,
            identifiers=identifiers,
            dry_run=args.dry_run,
        )
        if args.json:
            print(
                json.dumps(
                    {
                        "command": "restore",
                        "dry_run": args.dry_run,
                        "restored": rows,
                    },
                    indent=2,
                )
            )
        else:
            _print_restored(rows, dry_run=args.dry_run)
        if rows and not args.dry_run:
            update_monitoring("source_health")


if __name__ == "__main__":
    main()
