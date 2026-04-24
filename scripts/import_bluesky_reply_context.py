#!/usr/bin/env python3
"""Import Bluesky parent/root thread context for queued replies."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.bluesky_thread_context import (  # noqa: E402
    BlueskyThreadContextError,
    build_reply_context_update,
    metadata_is_incomplete,
    parse_platform_metadata,
)
from output.bluesky_client import BlueskyClient  # noqa: E402
from runner import script_context, update_monitoring  # noqa: E402

logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Enrich queued Bluesky replies with parent/root thread context."
    )
    parser.add_argument(
        "--reply-id",
        type=int,
        help="Only import context for one reply_queue row.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum candidate Bluesky reply rows to inspect.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report intended updates without mutating reply_queue.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON.",
    )
    return parser


def _candidate_rows(db: Any, *, reply_id: int | None, limit: int) -> list[dict[str, Any]]:
    if limit <= 0:
        raise ValueError("--limit must be positive")

    params: list[Any] = []
    filters = ["platform = 'bluesky'"]
    if reply_id is not None:
        filters.append("id = ?")
        params.append(reply_id)
    params.append(limit)

    cursor = db.conn.execute(
        f"""SELECT *
            FROM reply_queue
            WHERE {' AND '.join(filters)}
            ORDER BY detected_at ASC, id ASC
            LIMIT ?""",
        params,
    )
    return [dict(row) for row in cursor.fetchall()]


def import_context(
    db: Any,
    client: Any,
    *,
    reply_id: int | None = None,
    limit: int = 50,
    dry_run: bool = False,
    default_handle: str | None = None,
) -> dict[str, Any]:
    """Import missing thread context for queued Bluesky replies."""
    candidates = _candidate_rows(db, reply_id=reply_id, limit=limit)
    result: dict[str, Any] = {
        "dry_run": dry_run,
        "inspected": len(candidates),
        "updated": 0,
        "skipped_complete": 0,
        "errors": [],
        "rows": [],
    }

    for row in candidates:
        row_id = row["id"]
        if not metadata_is_incomplete(row.get("platform_metadata")):
            result["skipped_complete"] += 1
            result["rows"].append({"id": row_id, "status": "skipped_complete"})
            continue

        try:
            update = build_reply_context_update(
                row,
                client=client,
                default_handle=default_handle,
            )
        except (BlueskyThreadContextError, Exception) as e:
            result["errors"].append({"id": row_id, "error": str(e)})
            result["rows"].append({"id": row_id, "status": "error", "error": str(e)})
            continue

        old_metadata = parse_platform_metadata(row.get("platform_metadata"))
        changed_keys = sorted(
            key
            for key, value in update.metadata.items()
            if value and value != old_metadata.get(key)
        )
        if not dry_run:
            db.conn.execute(
                "UPDATE reply_queue SET platform_metadata = ? WHERE id = ?",
                (update.platform_metadata, row_id),
            )
            db.conn.commit()
            result["updated"] += 1
            status = "updated"
        else:
            status = "would_update"

        result["rows"].append(
            {
                "id": row_id,
                "status": status,
                "inbound_tweet_id": row.get("inbound_tweet_id"),
                "changed_keys": changed_keys,
            }
        )

    return result


def _format_text(result: dict[str, Any]) -> str:
    lines = [
        "Bluesky reply context import",
        f"Inspected: {result['inspected']}",
        f"Updated: {result['updated']}",
        f"Skipped complete: {result['skipped_complete']}",
        f"Errors: {len(result['errors'])}",
    ]
    for item in result["rows"]:
        line = f"  #{item['id']}: {item['status']}"
        if item.get("error"):
            line += f" ({item['error']})"
        lines.append(line)
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    args = _build_parser().parse_args(argv)

    with script_context() as (config, db):
        bluesky_config = getattr(config, "bluesky", None)
        if not bluesky_config or not getattr(bluesky_config, "enabled", False):
            logger.info("Bluesky is disabled")
            return 0

        client = BlueskyClient(
            bluesky_config.handle,
            bluesky_config.app_password,
        )
        result = import_context(
            db,
            client,
            reply_id=args.reply_id,
            limit=args.limit,
            dry_run=args.dry_run,
            default_handle=bluesky_config.handle,
        )

    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(_format_text(result))

    if not args.dry_run:
        update_monitoring("import-bluesky-reply-context")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
