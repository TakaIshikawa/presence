"""Manifest exporter for generated visual assets."""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from synthesis.alt_text_guard import validate_alt_text


@dataclass(frozen=True)
class VisualManifestFilters:
    """Filters for visual asset manifest rows."""

    since_days: int | None = None
    content_id: int | None = None
    missing_alt_only: bool = False


def _normalize_now(now: datetime | None) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now


def _decode_json_list(value: Any) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return value
    if not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _has_text(value: str | None) -> bool:
    return bool((value or "").strip())


def _read_png_dimensions(path: Path) -> dict[str, int] | None:
    with path.open("rb") as handle:
        header = handle.read(24)
    if len(header) >= 24 and header[:8] == b"\x89PNG\r\n\x1a\n":
        width, height = struct.unpack(">II", header[16:24])
        return {"width": width, "height": height}
    return None


def _read_jpeg_dimensions(path: Path) -> dict[str, int] | None:
    with path.open("rb") as handle:
        if handle.read(2) != b"\xff\xd8":
            return None
        while True:
            byte = handle.read(1)
            marker = b""
            while byte == b"\xff":
                marker = handle.read(1)
                if marker != b"\xff":
                    break
                byte = marker
            if not byte or not marker:
                return None
            marker_code = marker[0]
            if marker_code in {0xD8, 0xD9}:
                continue
            length_bytes = handle.read(2)
            if len(length_bytes) != 2:
                return None
            segment_length = struct.unpack(">H", length_bytes)[0]
            if segment_length < 2:
                return None
            if marker_code in {
                0xC0,
                0xC1,
                0xC2,
                0xC3,
                0xC5,
                0xC6,
                0xC7,
                0xC9,
                0xCA,
                0xCB,
                0xCD,
                0xCE,
                0xCF,
            }:
                data = handle.read(5)
                if len(data) != 5:
                    return None
                height, width = struct.unpack(">HH", data[1:5])
                return {"width": width, "height": height}
            handle.seek(segment_length - 2, 1)


def image_dimensions(image_path: str | None) -> dict[str, int] | None:
    """Return image dimensions from local file headers when available."""
    if not _has_text(image_path):
        return None

    path = Path(str(image_path)).expanduser()
    if not path.exists() or not path.is_file():
        return None

    try:
        dimensions = _read_png_dimensions(path)
        if dimensions:
            return dimensions
        return _read_jpeg_dimensions(path)
    except OSError:
        return None


def _publication_state(row: dict[str, Any]) -> dict[str, Any]:
    platform_states = []
    raw_states = row.get("publication_states") or ""
    if raw_states:
        for part in str(raw_states).split("|"):
            if not part:
                continue
            platform, _, status = part.partition(":")
            platform_states.append({"platform": platform, "status": status or None})

    queue_status = row.get("queue_status")
    legacy_code = int(row.get("published") or 0)
    if any(state.get("status") == "published" for state in platform_states):
        status = "published"
    elif queue_status == "held":
        status = "held"
    elif queue_status in {"queued", "failed", "cancelled", "published"}:
        status = queue_status
    elif platform_states:
        status = platform_states[0].get("status") or "generated"
    elif legacy_code == 1:
        status = "published"
    elif legacy_code == -1:
        status = "failed"
    else:
        status = "generated"

    return {
        "status": status,
        "published": status == "published" or legacy_code == 1,
        "published_at": row.get("publication_published_at") or row.get("published_at"),
        "published_url": row.get("published_url"),
        "tweet_id": row.get("tweet_id"),
        "bluesky_uri": row.get("bluesky_uri"),
        "platforms": platform_states,
        "queue_status": queue_status,
    }


def _row_to_entry(row: Any) -> dict[str, Any]:
    data = dict(row)
    alt_validation = validate_alt_text(
        data.get("image_alt_text"),
        image_prompt=data.get("image_prompt"),
        image_path=data.get("image_path"),
        content_type=data.get("content_type"),
    )
    dimensions = image_dimensions(data.get("image_path"))

    return {
        "content_id": data["id"],
        "content_type": data["content_type"],
        "created_at": data["created_at"],
        "content_format": data.get("content_format"),
        "image_path": data.get("image_path"),
        "image_prompt": data.get("image_prompt"),
        "prompt_present": _has_text(data.get("image_prompt")),
        "image_alt_text": data.get("image_alt_text"),
        "alt_text_status": alt_validation.status,
        "alt_text_usable": alt_validation.passed,
        "alt_text_issues": [issue.as_dict() for issue in alt_validation.issues],
        "dimensions": dimensions,
        "source_content_ids": {
            "source_commits": _decode_json_list(data.get("source_commits")),
            "source_messages": _decode_json_list(data.get("source_messages")),
            "source_activity_ids": _decode_json_list(data.get("source_activity_ids")),
            "repurposed_from": data.get("repurposed_from"),
        },
        "publication": _publication_state(data),
    }


def list_visual_manifest_entries(
    db: Any,
    filters: VisualManifestFilters | None = None,
    *,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """List one manifest entry per generated content row with an image path."""
    filters = filters or VisualManifestFilters()
    clauses = ["gc.image_path IS NOT NULL", "TRIM(gc.image_path) != ''"]
    params: list[Any] = []

    if filters.since_days is not None:
        if filters.since_days <= 0:
            raise ValueError("since_days must be positive")
        cutoff = (_normalize_now(now) - timedelta(days=filters.since_days)).isoformat()
        clauses.append("gc.created_at >= ?")
        params.append(cutoff)

    if filters.content_id is not None:
        clauses.append("gc.id = ?")
        params.append(filters.content_id)

    where = " AND ".join(clauses)
    rows = db.conn.execute(
        f"""WITH latest_queue AS (
                SELECT *
                FROM (
                    SELECT pq.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY pq.content_id
                               ORDER BY pq.scheduled_at DESC, pq.id DESC
                           ) AS rn
                    FROM publish_queue pq
                )
                WHERE rn = 1
            ),
            publication_rollup AS (
                SELECT
                    cp.content_id,
                    GROUP_CONCAT(cp.platform || ':' || cp.status, '|') AS publication_states,
                    MAX(cp.published_at) AS publication_published_at
                FROM content_publications cp
                GROUP BY cp.content_id
            )
            SELECT gc.*,
                   lq.status AS queue_status,
                   pr.publication_states,
                   pr.publication_published_at
            FROM generated_content gc
            LEFT JOIN latest_queue lq ON lq.content_id = gc.id
            LEFT JOIN publication_rollup pr ON pr.content_id = gc.id
            WHERE {where}
            ORDER BY gc.created_at DESC, gc.id DESC""",
        tuple(params),
    ).fetchall()

    entries = [_row_to_entry(row) for row in rows]
    if filters.missing_alt_only:
        entries = [entry for entry in entries if not entry["alt_text_usable"]]
    return entries


def manifest_to_json(entries: list[dict[str, Any]]) -> str:
    """Format manifest entries as JSON."""
    return json.dumps(entries, indent=2)


def _shorten(value: Any, width: int) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        text = "yes" if value else "no"
    else:
        text = str(value).replace("\n", " ")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _dimension_text(dimensions: dict[str, int] | None) -> str:
    if not dimensions:
        return "-"
    return f"{dimensions['width']}x{dimensions['height']}"


def manifest_to_table(entries: list[dict[str, Any]]) -> str:
    """Format manifest entries as a readable table."""
    if not entries:
        return "No visual assets found."

    columns = [
        ("content_id", "CID", 5),
        ("content_type", "TYPE", 10),
        ("created_at", "CREATED", 19),
        ("publication", "PUB", 10),
        ("alt_text_status", "ALT", 14),
        ("prompt_present", "PROMPT", 6),
        ("dimensions", "SIZE", 10),
        ("image_path", "IMAGE_PATH", 42),
    ]
    lines = [
        "  ".join(label.ljust(width) for _, label, width in columns),
        "  ".join("-" * width for _, _, width in columns),
    ]
    for entry in entries:
        row = {
            "content_id": entry["content_id"],
            "content_type": entry["content_type"],
            "created_at": entry["created_at"],
            "publication": entry["publication"]["status"],
            "alt_text_status": entry["alt_text_status"],
            "prompt_present": entry["prompt_present"],
            "dimensions": _dimension_text(entry["dimensions"]),
            "image_path": entry["image_path"],
        }
        lines.append(
            "  ".join(
                _shorten(row[key], width).ljust(width)
                for key, _, width in columns
            )
        )
    return "\n".join(lines)
