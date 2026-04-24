#!/usr/bin/env python3
"""Report whether queued content is ready to publish."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publish_readiness import check_publish_readiness  # noqa: E402
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--platform",
        choices=("x", "bluesky", "all"),
        help="Only report queue rows for this queued platform",
    )
    parser.add_argument(
        "--queue-id",
        type=int,
        help="Only report one publish_queue id",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit stable machine-readable JSON",
    )
    parser.add_argument(
        "--allow-restricted-knowledge",
        action="store_true",
        help="Downgrade restricted-knowledge license blocks to warnings",
    )
    return parser.parse_args(argv)


def _format_text(results: list) -> str:
    if not results:
        return "No queued publish items found."

    lines: list[str] = []
    for result in results:
        due = ""
        if result.due is True:
            due = " due"
        elif result.due is False:
            due = " future"
        lines.append(
            f"Queue {result.queue_id} content {result.content_id} "
            f"[{result.platform}]{due}: {result.status}"
        )
        for reason in result.reasons:
            lines.append(f"- {reason.severity}: {reason.code}: {reason.message}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    now_iso = datetime.now(timezone.utc).isoformat()

    with script_context() as (config, db):
        results = check_publish_readiness(
            db,
            config=config,
            platform=args.platform,
            queue_id=args.queue_id,
            now_iso=now_iso,
            allow_restricted_knowledge=args.allow_restricted_knowledge,
        )

    if args.json:
        print(
            json.dumps(
                [result.as_dict() for result in results],
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(_format_text(results))

    return 1 if any(result.blocked for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
