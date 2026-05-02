"""Report generated visual assets that need cleanup or remediation."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


DEFAULT_DAYS = 30
CATEGORIES = (
    "missing_file_reference",
    "unpublished_stale_visual",
    "queued_without_alt_text",
    "published_asset",
)
ACTIVE_QUEUE_STATUSES = {"queued", "held"}
PUBLISHED_STATUSES = {"published", "success", "succeeded"}


def build_visual_asset_orphans_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    check_files: bool = False,
    limit: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return generated visual asset rows grouped by cleanup category."""

    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    columns = schema.get("generated_content", set())
    missing_tables = [] if "generated_content" in schema else ["generated_content"]
    if not columns or "id" not in columns or "image_path" not in columns:
        return _empty_report(
            generated_at,
            days=days,
            check_files=check_files,
            limit=limit,
            missing_tables=missing_tables,
            missing_columns=_missing_columns(columns),
        )

    queue_states = _queue_states(conn, schema)
    publication_states = _publication_states(conn, schema)
    rows = _load_visual_rows(conn, columns)
    findings = [
        finding
        for row in rows
        if (
            finding := _classify_row(
                row,
                days=days,
                now=generated_at,
                check_files=check_files,
                queue_statuses=queue_states.get(int(row["id"]), []),
                publication_statuses=publication_states.get(int(row["id"]), []),
            )
        )
        is not None
    ]
    findings.sort(key=lambda item: (item["category"], -(item["age_days"] or -1), item["content_id"]))
    total_findings = len(findings)
    if limit is not None:
        findings = findings[:limit]

    counts = {category: 0 for category in CATEGORIES}
    for finding in findings:
        counts[finding["category"]] += 1

    return {
        "artifact_type": "visual_asset_orphans",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "check_files": check_files,
            "limit": limit,
        },
        "counts": {
            "visual_rows_scanned": len(rows),
            "findings": len(findings),
            "findings_before_limit": total_findings,
            "by_category": counts,
        },
        "missing_tables": missing_tables,
        "missing_columns": _missing_columns(columns),
        "rows": findings,
    }


def format_visual_asset_orphans_json(report: dict[str, Any]) -> str:
    """Render the visual asset orphan report as deterministic JSON."""

    return json.dumps(report, indent=2, sort_keys=True)


def format_visual_asset_orphans_text(report: dict[str, Any]) -> str:
    """Render the visual asset orphan report as human-readable text."""

    filters = report["filters"]
    counts = report["counts"]
    by_category = counts["by_category"]
    lines = [
        "Visual Asset Orphans",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days={filters['days']} "
            f"check_files={int(filters['check_files'])} "
            f"limit={filters['limit'] or 'none'}"
        ),
        (
            f"Rows: scanned={counts['visual_rows_scanned']} "
            f"findings={counts['findings']} "
            f"before_limit={counts['findings_before_limit']} "
            f"missing_file={by_category.get('missing_file_reference', 0)} "
            f"stale={by_category.get('unpublished_stale_visual', 0)} "
            f"queued_no_alt={by_category.get('queued_without_alt_text', 0)} "
            f"published={by_category.get('published_asset', 0)}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        for table, columns in report["missing_columns"].items():
            lines.append(f"Missing columns on {table}: {', '.join(columns)}")

    if not report["rows"]:
        lines.append("")
        lines.append("No visual asset orphan findings found.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Findings:")
    for row in report["rows"]:
        lines.append(
            f"  - content_id={row['content_id']} category={row['category']} "
            f"age_days={row['age_days'] if row['age_days'] is not None else 'n/a'} "
            f"publication={row['publication_status']} queue={row['queue_status']} "
            f"image_path={row['image_path']} action={row['recommended_action']}"
        )
    return "\n".join(lines)


def _load_visual_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select_columns = [
        "id",
        _column_expr(columns, "content_type"),
        _column_expr(columns, "image_path"),
        _column_expr(columns, "image_prompt"),
        _column_expr(columns, "image_alt_text"),
        _column_expr(columns, "published", "0"),
        _column_expr(columns, "published_url"),
        _column_expr(columns, "published_at"),
        _column_expr(columns, "created_at"),
    ]
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {", ".join(select_columns)}
                FROM generated_content
                WHERE image_path IS NOT NULL
                  AND TRIM(image_path) != ''
                ORDER BY id ASC"""
        ).fetchall()
    ]


def _classify_row(
    row: dict[str, Any],
    *,
    days: int,
    now: datetime,
    check_files: bool,
    queue_statuses: list[str],
    publication_statuses: list[str],
) -> dict[str, Any] | None:
    content_id = int(row["id"])
    created = _parse_datetime(row.get("created_at"))
    age_days = (now - created).days if created is not None else None
    publication_status = _publication_status(row, publication_statuses)
    queue_status = _combined_status(queue_statuses)
    image_path = str(row.get("image_path") or "").strip()
    alt_text = str(row.get("image_alt_text") or "").strip()

    category: str | None
    action: str
    file_exists: bool | None = None
    if check_files and _is_filesystem_path(image_path):
        file_exists = Path(image_path).exists()
        if not file_exists:
            category = "missing_file_reference"
            action = "Remove or regenerate the missing visual asset reference."
        else:
            category = None
            action = ""
    else:
        category = None
        action = ""

    if category is None and publication_status == "published":
        category = "published_asset"
        action = "Keep as published record or archive according to retention policy."
    elif category is None and _has_active_queue(queue_statuses) and not alt_text:
        category = "queued_without_alt_text"
        action = "Add alt text before publishing the queued visual."
    elif category is None and publication_status != "published" and age_days is not None and age_days >= days:
        category = "unpublished_stale_visual"
        action = "Refresh, reschedule, or delete the stale unpublished visual."

    if category is None:
        return None

    return {
        "content_id": content_id,
        "content_type": row.get("content_type"),
        "image_path": image_path,
        "image_prompt": _clean(row.get("image_prompt")),
        "image_alt_text": _clean(row.get("image_alt_text")),
        "created_at": row.get("created_at"),
        "age_days": age_days,
        "publication_status": publication_status,
        "queue_status": queue_status,
        "file_exists": file_exists,
        "category": category,
        "recommended_action": action,
    }


def _queue_states(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[int, list[str]]:
    columns = schema.get("publish_queue")
    if not columns or not {"content_id", "status"}.issubset(columns):
        return {}
    states: dict[int, list[str]] = {}
    for row in conn.execute(
        """SELECT content_id, status
           FROM publish_queue
           WHERE content_id IS NOT NULL
           ORDER BY content_id ASC, id ASC"""
    ).fetchall():
        content_id = _int_value(row["content_id"])
        status = _status(row["status"])
        if content_id is not None and status:
            states.setdefault(content_id, []).append(status)
    return states


def _publication_states(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[int, list[str]]:
    columns = schema.get("content_publications")
    if not columns or not {"content_id", "status"}.issubset(columns):
        return {}
    states: dict[int, list[str]] = {}
    for row in conn.execute(
        """SELECT content_id, status
           FROM content_publications
           WHERE content_id IS NOT NULL
           ORDER BY content_id ASC, id ASC"""
    ).fetchall():
        content_id = _int_value(row["content_id"])
        status = _status(row["status"])
        if content_id is not None and status:
            states.setdefault(content_id, []).append(status)
    return states


def _publication_status(row: dict[str, Any], publication_statuses: list[str]) -> str:
    if any(status in PUBLISHED_STATUSES for status in publication_statuses) or _legacy_published(row):
        return "published"
    if publication_statuses:
        return _combined_status(publication_statuses)
    return "unpublished"


def _legacy_published(row: dict[str, Any]) -> bool:
    published = row.get("published")
    if isinstance(published, str):
        if published.strip().lower() in {"1", "true", "yes", "published"}:
            return True
    elif published:
        return True
    return bool(row.get("published_url") or row.get("published_at"))


def _combined_status(statuses: list[str]) -> str:
    unique = sorted(dict.fromkeys(statuses))
    if not unique:
        return "none"
    if len(unique) == 1:
        return unique[0]
    return "mixed:" + ",".join(unique)


def _has_active_queue(statuses: list[str]) -> bool:
    return any(status in ACTIVE_QUEUE_STATUSES for status in statuses)


def _is_filesystem_path(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme.lower() not in {"http", "https", "s3", "gs"}


def _missing_columns(columns: set[str]) -> dict[str, list[str]]:
    required = {"id", "image_path"}
    missing = sorted(required - columns)
    return {"generated_content": missing} if missing else {}


def _empty_report(
    generated_at: datetime,
    *,
    days: int,
    check_files: bool,
    limit: int | None,
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    return {
        "artifact_type": "visual_asset_orphans",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "check_files": check_files, "limit": limit},
        "counts": {
            "visual_rows_scanned": 0,
            "findings": 0,
            "findings_before_limit": 0,
            "by_category": {category: 0 for category in CATEGORIES},
        },
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
        "rows": [],
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3 connection or database wrapper with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        schema[table] = {
            str(info["name"] if isinstance(info, sqlite3.Row) else info[1])
            for info in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")
        }
    return schema


def _column_expr(columns: set[str], column: str, fallback: str = "NULL") -> str:
    return column if column in columns else f"{fallback} AS {column}"


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return _ensure_utc(datetime.fromisoformat(text))
    except ValueError:
        return None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _int_value(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _status(value: Any) -> str | None:
    cleaned = _clean(value)
    return cleaned.lower() if cleaned else None


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
