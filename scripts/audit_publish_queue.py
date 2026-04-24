#!/usr/bin/env python3
"""Audit queued publish items for platform scheduling collisions."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.posting_schedule import embargo_windows_from_config
from output.publish_caps import daily_platform_limits_from_config
from output.publish_queue_audit import (
    DEFAULT_COLLISION_WINDOW_MINUTES,
    PublishQueueAuditResult,
    audit_publish_queue,
)
from runner import script_context


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--window-minutes",
        type=int,
        default=DEFAULT_COLLISION_WINDOW_MINUTES,
        help=f"Collision window in minutes (default: {DEFAULT_COLLISION_WINDOW_MINUTES})",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON",
    )
    parser.add_argument(
        "--apply-holds",
        action="store_true",
        help="Mark affected queued rows as held with audit hold reasons",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report collisions without writing changes (default)",
    )
    return parser.parse_args(argv)


def format_audit_result(result: PublishQueueAuditResult, *, applied: bool) -> str:
    if not result.collision_groups:
        return "No publish queue scheduling collisions found."

    lines = [
        f"Found {result.collision_count} publish queue scheduling collision group(s).",
    ]
    for group in result.collision_groups:
        lines.append(
            f"- {group.platform}: queue IDs {', '.join(str(i) for i in group.queue_ids)} "
            f"from {group.start_at.isoformat()} to {group.end_at.isoformat()}"
        )
        for queue_id in group.deferred_queue_ids:
            reason = result.hold_reasons.get(queue_id)
            if reason:
                lines.append(f"  hold {queue_id}: {reason}")

    if applied:
        lines.append(f"Applied holds to {len(result.applied_holds)} queued item(s).")
    else:
        lines.append("Dry run: no queue rows were changed.")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.window_minutes <= 0:
        print("error: --window-minutes must be positive", file=sys.stderr)
        return 2

    apply_holds = bool(args.apply_holds and not args.dry_run)
    with script_context() as (config, db):
        result = audit_publish_queue(
            db,
            window_minutes=args.window_minutes,
            daily_platform_limits=daily_platform_limits_from_config(config),
            embargo_windows=embargo_windows_from_config(config),
            apply_holds=apply_holds,
        )

    if args.json:
        print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        print(format_audit_result(result, applied=apply_holds))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
