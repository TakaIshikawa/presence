"""Import Instagram post insight exports."""
from __future__ import annotations

import csv
import io
import json
import re
from pathlib import Path
from typing import Any

from evaluation._batch_report_utils import dump_json, text


SCHEMA = """CREATE TABLE IF NOT EXISTS instagram_post_insights (
account_handle TEXT,
media_id TEXT NOT NULL,
permalink TEXT,
media_type TEXT,
posted_at TEXT,
impressions INTEGER,
reach INTEGER,
likes INTEGER,
comments INTEGER,
saves INTEGER,
shares INTEGER,
captured_at TEXT NOT NULL,
PRIMARY KEY (media_id, captured_at)
)"""


def parse_instagram_post_insights(raw: str) -> list[dict[str, Any]]:
    rows = []
    for item in _records(raw, ("insights", "posts", "items", "data")):
        media_id = text(item.get("media_id") or item.get("id") or item.get("post_id"))
        captured_at = text(item.get("captured_at") or item.get("collected_at") or item.get("snapshot_at"))
        if not media_id or not captured_at:
            raise ValueError("media_id and captured_at are required")
        metrics = item.get("insights") if isinstance(item.get("insights"), dict) else {}
        rows.append(
            {
                "account_handle": _handle(item.get("account_handle") or item.get("handle") or item.get("username")) or None,
                "media_id": media_id,
                "permalink": text(item.get("permalink") or item.get("url") or item.get("post_url")) or None,
                "media_type": text(item.get("media_type") or item.get("type")) or None,
                "posted_at": text(item.get("posted_at") or item.get("created_at") or item.get("timestamp")) or None,
                "impressions": _int(_metric(item, metrics, "impressions", "impression_count")),
                "reach": _int(_metric(item, metrics, "reach", "reach_count")),
                "likes": _int(_metric(item, metrics, "likes", "like_count")),
                "comments": _int(_metric(item, metrics, "comments", "comment_count")),
                "saves": _int(_metric(item, metrics, "saves", "save_count")),
                "shares": _int(_metric(item, metrics, "shares", "share_count")),
                "captured_at": captured_at,
            }
        )
    rows.sort(key=lambda row: (row["media_id"], row["captured_at"]))
    return rows


def upsert_instagram_post_insights(conn, rows: list[dict[str, Any]], dry_run: bool = False) -> dict[str, Any]:
    if dry_run:
        return {
            "artifact_type": "instagram_post_insight_import",
            "dry_run": True,
            "parsed_count": len(rows),
            "upserted_count": 0,
        }
    conn.execute(SCHEMA)
    for row in rows:
        conn.execute(
            """INSERT INTO instagram_post_insights VALUES
            (:account_handle,:media_id,:permalink,:media_type,:posted_at,:impressions,:reach,:likes,:comments,:saves,:shares,:captured_at)
            ON CONFLICT(media_id,captured_at) DO UPDATE SET
            account_handle=excluded.account_handle,
            permalink=excluded.permalink,
            media_type=excluded.media_type,
            posted_at=excluded.posted_at,
            impressions=excluded.impressions,
            reach=excluded.reach,
            likes=excluded.likes,
            comments=excluded.comments,
            saves=excluded.saves,
            shares=excluded.shares""",
            row,
        )
    conn.commit()
    return {
        "artifact_type": "instagram_post_insight_import",
        "dry_run": False,
        "parsed_count": len(rows),
        "upserted_count": len(rows),
    }


def import_instagram_post_insights(conn, path: str | Path, dry_run: bool = False) -> dict[str, Any]:
    return upsert_instagram_post_insights(conn, parse_instagram_post_insights(Path(path).read_text()), dry_run=dry_run)


def format_instagram_post_insight_import_json(summary: dict[str, Any]) -> str:
    return dump_json(summary)


def format_instagram_post_insight_import_text(summary: dict[str, Any]) -> str:
    return (
        "Instagram Post Insight Import\n"
        f"parsed={summary['parsed_count']} upserted={summary['upserted_count']} dry_run={summary['dry_run']}"
    )


def _records(raw: str, keys: tuple[str, ...]) -> list[dict[str, Any]]:
    raw = raw.strip()
    if not raw:
        return []
    if raw[0] in "[{":
        decoded = json.loads(raw)
        if isinstance(decoded, dict):
            for key in keys:
                if isinstance(decoded.get(key), list):
                    return decoded[key]
            return [decoded]
        return decoded
    if "," in raw.splitlines()[0]:
        return list(csv.DictReader(io.StringIO(raw)))
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def _metric(item: dict[str, Any], metrics: dict[str, Any], *names: str) -> Any:
    for name in names:
        if item.get(name) not in (None, ""):
            return item.get(name)
        if metrics.get(name) not in (None, ""):
            return metrics.get(name)
    return 0


def _int(value: Any) -> int:
    raw = text(value)
    if not raw:
        return 0
    cleaned = re.sub(r"[,\s_]", "", raw)
    try:
        return int(float(cleaned))
    except ValueError:
        return 0


def _handle(value: Any) -> str:
    return text(value).lstrip("@").strip()
