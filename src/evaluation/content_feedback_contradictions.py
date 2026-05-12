"""Find generated content with contradictory durable feedback signals."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 50
OPPOSING_TYPES = {"reject", "revise"}


def build_content_feedback_contradictions_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    tag: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only report for content with opposing feedback signals."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _aware(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    normalized_tag = _normalize_tag(tag) if tag else None
    rows = _feedback_rows(conn, schema, cutoff, generated_at)
    groups = _contradiction_groups(rows, normalized_tag)[:limit]

    return {
        "artifact_type": "content_feedback_contradictions",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "limit": limit,
            "tag": normalized_tag,
            "lookback_start": cutoff.isoformat(),
            "lookback_end": generated_at.isoformat(),
        },
        "totals": {
            "feedback_rows_scanned": len(rows),
            "contradictory_content_count": len(groups),
            "malformed_tag_rows": sum(1 for row in rows if row["tags_malformed"]),
        },
        "contradictions": groups,
        "empty_state": {
            "is_empty": not groups,
            "schema_present": "content_feedback" in schema,
            "message": "No contradictory content feedback found." if not groups else None,
        },
    }


def format_content_feedback_contradictions_json(report: dict[str, Any]) -> str:
    """Render report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_feedback_contradictions_text(report: dict[str, Any]) -> str:
    """Render report as compact terminal text."""
    lines = [
        "Content Feedback Contradictions",
        f"Generated: {report['generated_at']}",
        (
            f"Window: {report['filters']['days']} days "
            f"limit={report['filters']['limit']} tag={report['filters']['tag'] or 'all'}"
        ),
        (
            "Totals: "
            f"feedback_rows={report['totals']['feedback_rows_scanned']} "
            f"contradictions={report['totals']['contradictory_content_count']}"
        ),
    ]
    if report["totals"]["malformed_tag_rows"]:
        lines.append(f"Malformed tag rows: {report['totals']['malformed_tag_rows']}")
    if not report["contradictions"]:
        lines.extend(["", report["empty_state"]["message"]])
        return "\n".join(lines)

    lines.extend(["", "Contradictions:"])
    for group in report["contradictions"]:
        tags = ", ".join(group["conflicting_tags"]) or "-"
        notes = " | ".join(group["sample_notes"]) or "-"
        lines.append(
            f"- content_id={group['content_id']} latest={group['latest_feedback_at']} "
            f"types={_format_counts(group['feedback_type_counts'])} tags={tags} notes={notes}"
        )
    return "\n".join(lines)


def _feedback_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    if "content_feedback" not in schema:
        return []
    columns = schema["content_feedback"]
    tags_expr = "cf.tags" if "tags" in columns else "NULL"
    notes_expr = "cf.notes" if "notes" in columns else "NULL"
    created_expr = "cf.created_at" if "created_at" in columns else "NULL"
    rows = conn.execute(
        f"""SELECT cf.id, cf.content_id, cf.feedback_type,
                  {notes_expr} AS notes,
                  {tags_expr} AS tags,
                  {created_expr} AS created_at
           FROM content_feedback cf
           ORDER BY {created_expr} ASC, cf.id ASC"""
    ).fetchall()

    normalized = []
    for row in rows:
        created_at = _parse_timestamp(row["created_at"]) or now
        if created_at < cutoff or created_at > now:
            continue
        tags, malformed = _parse_tags(row["tags"])
        normalized.append(
            {
                "id": int(row["id"]),
                "content_id": int(row["content_id"]),
                "feedback_type": str(row["feedback_type"] or "unknown"),
                "notes": str(row["notes"] or "").strip(),
                "tags": tags,
                "tags_malformed": malformed,
                "created_at": created_at.isoformat(),
            }
        )
    return normalized


def _contradiction_groups(rows: list[dict[str, Any]], tag: str | None) -> list[dict[str, Any]]:
    by_content: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_content[row["content_id"]].append(row)

    groups = []
    for content_id, content_rows in by_content.items():
        counts = Counter(row["feedback_type"] for row in content_rows)
        has_type_conflict = counts.get("prefer", 0) > 0 and any(
            counts.get(feedback_type, 0) > 0 for feedback_type in OPPOSING_TYPES
        )
        tags_by_type: dict[str, set[str]] = defaultdict(set)
        tag_counts = Counter()
        for row in content_rows:
            for item in row["tags"]:
                tags_by_type[item].add(row["feedback_type"])
                tag_counts[item] += 1
        conflicting_tags = sorted(
            item
            for item, types in tags_by_type.items()
            if "prefer" in types and types.intersection(OPPOSING_TYPES)
        )
        if tag and tag not in conflicting_tags and not any(tag in row["tags"] for row in content_rows):
            continue
        if tag and tag in {item for row in content_rows for item in row["tags"]} and tag not in conflicting_tags:
            continue
        if not has_type_conflict and not conflicting_tags:
            continue

        latest = max(row["created_at"] for row in content_rows)
        groups.append(
            {
                "content_id": content_id,
                "feedback_type_counts": dict(sorted(counts.items())),
                "conflicting_tags": conflicting_tags,
                "normalized_tag_summary": [
                    {"tag": item, "count": tag_counts[item], "feedback_types": sorted(tags_by_type[item])}
                    for item in sorted(tag_counts)
                ],
                "latest_feedback_at": latest,
                "sample_notes": _sample_notes(content_rows),
            }
        )
    groups.sort(
        key=lambda item: (
            item["latest_feedback_at"],
            sum(item["feedback_type_counts"].values()),
            item["content_id"],
        ),
        reverse=True,
    )
    return groups


def _parse_tags(value: Any) -> tuple[list[str], bool]:
    if value in (None, ""):
        return [], False
    parsed = value
    malformed = False
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            parsed = []
            malformed = True
    if not isinstance(parsed, list):
        return [], True
    tags = sorted({_normalize_tag(item) for item in parsed if _normalize_tag(item)})
    return tags, malformed


def _sample_notes(rows: list[dict[str, Any]]) -> list[str]:
    notes = []
    for row in sorted(rows, key=lambda item: (item["created_at"], item["id"]), reverse=True):
        note = " ".join(row["notes"].split())
        if note and note not in notes:
            notes.append(note[:160])
        if len(notes) == 3:
            break
    return notes


def _normalize_tag(value: Any) -> str:
    return str(value or "").strip().lower().replace(" ", "_")


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {
        row["name"]: {column["name"] for column in conn.execute(f"PRAGMA table_info({row['name']})")}
        for row in rows
    }


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_counts(counts: dict[str, int]) -> str:
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))
