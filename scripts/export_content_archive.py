#!/usr/bin/env python3
"""Export generated content archive rows as JSONL."""

from __future__ import annotations

import argparse
import base64
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, TextIO

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context


SUPPORTED_PLATFORMS = {"all", "x", "bluesky"}


def _decode_json_field(value: Any) -> Any:
    """Return parsed JSON when possible while preserving malformed values."""
    if value in (None, ""):
        return []
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _json_safe(value: Any) -> Any:
    """Recursively convert SQLite values into JSON-safe structures."""
    if isinstance(value, bytes):
        return {
            "encoding": "base64",
            "data": base64.b64encode(value).decode("ascii"),
        }
    if isinstance(value, dict):
        return {str(key): _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_json_safe(inner) for inner in value]
    if isinstance(value, tuple):
        return [_json_safe(inner) for inner in value]
    return value


def _row_to_dict(row: Any) -> dict[str, Any]:
    data = dict(row)
    data["source_commits"] = _decode_json_field(data.get("source_commits"))
    data["source_messages"] = _decode_json_field(data.get("source_messages"))
    return _json_safe(data)


def _normalize_now(now: datetime | None) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now


def _content_filters(
    days: int,
    content_type: str | None,
    platform: str,
    now: datetime | None,
) -> tuple[str, list[Any]]:
    if days <= 0:
        raise ValueError("days must be positive")
    if platform not in SUPPORTED_PLATFORMS:
        raise ValueError(f"platform must be one of: {', '.join(sorted(SUPPORTED_PLATFORMS))}")

    cutoff = (_normalize_now(now) - timedelta(days=days)).isoformat()
    filters = [
        """(
            gc.created_at >= ?
            OR gc.published_at >= ?
            OR EXISTS (
                SELECT 1 FROM content_publications cp
                WHERE cp.content_id = gc.id
                  AND (
                      cp.published_at >= ?
                      OR cp.updated_at >= ?
                      OR cp.last_error_at >= ?
                  )
            )
            OR EXISTS (
                SELECT 1 FROM post_engagement pe
                WHERE pe.content_id = gc.id AND pe.fetched_at >= ?
            )
            OR EXISTS (
                SELECT 1 FROM bluesky_engagement be
                WHERE be.content_id = gc.id AND be.fetched_at >= ?
            )
        )"""
    ]
    params: list[Any] = [cutoff] * 7

    if content_type:
        filters.append("gc.content_type = ?")
        params.append(content_type)

    if platform != "all":
        platform_filter = [
            """EXISTS (
                SELECT 1 FROM content_publications cp
                WHERE cp.content_id = gc.id AND cp.platform = ?
            )"""
        ]
        params.append(platform)
        if platform == "x":
            platform_filter.append("gc.tweet_id IS NOT NULL")
            platform_filter.append(
                """EXISTS (
                    SELECT 1 FROM post_engagement pe
                    WHERE pe.content_id = gc.id
                )"""
            )
        elif platform == "bluesky":
            platform_filter.append("gc.bluesky_uri IS NOT NULL")
            platform_filter.append(
                """EXISTS (
                    SELECT 1 FROM bluesky_engagement be
                    WHERE be.content_id = gc.id
                )"""
            )
        filters.append("(" + " OR ".join(platform_filter) + ")")

    return " AND ".join(filters), params


def list_content_ids(
    db: Any,
    days: int = 30,
    content_type: str | None = None,
    platform: str = "all",
    now: datetime | None = None,
) -> list[int]:
    """List archive candidate content IDs."""
    where, params = _content_filters(days, content_type, platform, now)
    rows = db.conn.execute(
        f"""SELECT gc.id
            FROM generated_content gc
            WHERE {where}
            ORDER BY gc.id ASC""",
        tuple(params),
    ).fetchall()
    return [int(row["id"]) for row in rows]


def get_content_topics(db: Any, content_id: int) -> list[dict[str, Any]]:
    rows = db.conn.execute(
        """SELECT *
           FROM content_topics
           WHERE content_id = ?
           ORDER BY created_at ASC, id ASC""",
        (content_id,),
    ).fetchall()
    return [_json_safe(dict(row)) for row in rows]


def get_planned_topics(db: Any, content_id: int) -> list[dict[str, Any]]:
    rows = db.conn.execute(
        """SELECT pt.*,
                  cc.id AS linked_campaign_id,
                  cc.name AS campaign_name,
                  cc.goal AS campaign_goal,
                  cc.start_date AS campaign_start_date,
                  cc.end_date AS campaign_end_date,
                  cc.daily_limit AS campaign_daily_limit,
                  cc.weekly_limit AS campaign_weekly_limit,
                  cc.status AS campaign_status,
                  cc.created_at AS campaign_created_at
           FROM planned_topics pt
           LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id
           WHERE pt.content_id = ?
           ORDER BY pt.target_date ASC, pt.created_at ASC, pt.id ASC""",
        (content_id,),
    ).fetchall()

    planned_topics = []
    for row in rows:
        data = dict(row)
        campaign = None
        if data.get("linked_campaign_id") is not None:
            campaign = {
                "id": data.pop("linked_campaign_id"),
                "name": data.pop("campaign_name"),
                "goal": data.pop("campaign_goal"),
                "start_date": data.pop("campaign_start_date"),
                "end_date": data.pop("campaign_end_date"),
                "daily_limit": data.pop("campaign_daily_limit"),
                "weekly_limit": data.pop("campaign_weekly_limit"),
                "status": data.pop("campaign_status"),
                "created_at": data.pop("campaign_created_at"),
            }
        else:
            for key in (
                "linked_campaign_id",
                "campaign_name",
                "campaign_goal",
                "campaign_start_date",
                "campaign_end_date",
                "campaign_daily_limit",
                "campaign_weekly_limit",
                "campaign_status",
                "campaign_created_at",
            ):
                data.pop(key, None)
        data["campaign"] = campaign
        planned_topics.append(_json_safe(data))
    return planned_topics


def build_archive_record(db: Any, content_id: int, platform: str = "all") -> dict[str, Any]:
    """Build one archive object for a generated content item."""
    row = db.conn.execute(
        "SELECT * FROM generated_content WHERE id = ?",
        (content_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"Content ID {content_id} not found")

    publications = db.get_latest_publication_states(content_id)
    snapshots = db.get_engagement_snapshots_for_content(content_id)
    variants = db.list_content_variants(content_id)
    if platform != "all":
        publications = [pub for pub in publications if pub.get("platform") == platform]
        snapshots = [snapshot for snapshot in snapshots if snapshot.get("platform") == platform]
        variants = [variant for variant in variants if variant.get("platform") == platform]

    return _json_safe(
        {
            "content": _row_to_dict(row),
            "publications": publications,
            "engagement_snapshots": snapshots,
            "knowledge_links": db.get_content_lineage(content_id),
            "variants": variants,
            "topics": get_content_topics(db, content_id),
            "planned_topics": get_planned_topics(db, content_id),
            "pipeline_runs": db.get_pipeline_runs_for_content(content_id),
        }
    )


def iter_archive_records(
    db: Any,
    days: int = 30,
    content_type: str | None = None,
    platform: str = "all",
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Return archive records in deterministic order."""
    return [
        build_archive_record(db, content_id, platform=platform)
        for content_id in list_content_ids(db, days, content_type, platform, now)
    ]


def write_jsonl(records: list[dict[str, Any]], output: TextIO) -> int:
    """Write archive records as one compact JSON object per line."""
    count = 0
    for record in records:
        output.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        count += 1
    return count


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to include based on generated, published, publication, or engagement timestamps.",
    )
    parser.add_argument("--content-type", help="Filter by generated_content.content_type")
    parser.add_argument(
        "--platform",
        default="all",
        choices=sorted(SUPPORTED_PLATFORMS),
        help="Filter to content and nested rows for a platform.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output JSONL file path. Use '-' for stdout.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        records = iter_archive_records(
            db,
            days=args.days,
            content_type=args.content_type,
            platform=args.platform,
        )

    if args.output == "-":
        count = write_jsonl(records, sys.stdout)
    else:
        output_path = Path(args.output).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as handle:
            count = write_jsonl(records, handle)

    print(f"Exported {count} content archive records to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
