"""Report generated content rows without durable source attribution."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 30
SOURCE_COLUMNS = ("source_commits", "source_messages", "source_activity_ids")
BUCKETS = ("no_sources", "stale_unpublished_no_sources", "published_no_sources")


def build_generated_source_attribution_gaps_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    content_type: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return source-less generated_content rows grouped by attribution bucket."""

    if days <= 0:
        raise ValueError("days must be positive")
    if content_type is not None and not content_type.strip():
        raise ValueError("content_type must not be blank")

    conn = _connection(db_or_conn)
    conn.row_factory = sqlite3.Row
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    stale_cutoff = generated_at - timedelta(days=days)
    schema = _schema(conn)
    columns = schema.get("generated_content", set())
    warnings: list[str] = []
    rows = _load_generated_content(conn, columns, content_type=content_type)

    gaps = [
        gap
        for row in rows
        if (
            gap := _gap_row(
                row,
                columns=columns,
                stale_cutoff=stale_cutoff,
                report_warnings=warnings,
            )
        )
        is not None
    ]
    counts_by_bucket = {bucket: 0 for bucket in BUCKETS}
    for gap in gaps:
        counts_by_bucket[gap["bucket"]] = counts_by_bucket.get(gap["bucket"], 0) + 1

    return {
        "artifact_type": "generated_source_attribution_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "content_type": content_type,
        },
        "stale_cutoff": stale_cutoff.isoformat(),
        "missing_tables": [] if "generated_content" in schema else ["generated_content"],
        "missing_columns": {
            "generated_content": sorted(_required_columns() - columns)
        }
        if columns and (_required_columns() - columns)
        else {},
        "counts": {
            "rows_scanned": len(rows),
            "attribution_gaps": len(gaps),
            "by_bucket": counts_by_bucket,
        },
        "rows": sorted(gaps, key=lambda item: (item["bucket"], item["created_at"] or "", item["content_id"])),
        "warnings": sorted(dict.fromkeys(warnings)),
    }


def format_generated_source_attribution_gaps_json(report: dict[str, Any]) -> str:
    """Render the attribution gap report as deterministic JSON."""

    return json.dumps(report, indent=2, sort_keys=True)


def format_generated_source_attribution_gaps_text(report: dict[str, Any]) -> str:
    """Render the attribution gap report as human-readable text."""

    filters = report["filters"]
    counts = report["counts"]
    by_bucket = counts["by_bucket"]
    lines = [
        "Generated Source Attribution Gaps",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days={filters['days']} "
            f"content_type={filters['content_type'] or 'all'}"
        ),
        f"Stale cutoff: {report['stale_cutoff']}",
        (
            f"Rows: scanned={counts['rows_scanned']} "
            f"gaps={counts['attribution_gaps']} "
            f"no_sources={by_bucket.get('no_sources', 0)} "
            f"stale_unpublished={by_bucket.get('stale_unpublished_no_sources', 0)} "
            f"published={by_bucket.get('published_no_sources', 0)}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        for table, columns in report["missing_columns"].items():
            lines.append(f"Missing columns on {table}: {', '.join(columns)}")
    if report["warnings"]:
        lines.append("Warnings:")
        lines.extend(f"  - {warning}" for warning in report["warnings"])

    if not report["rows"]:
        lines.append("")
        lines.append("No generated content source attribution gaps found.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Gaps:")
    for row in report["rows"]:
        lines.append(
            f"  - content_id={row['content_id']} type={row['content_type'] or 'n/a'} "
            f"bucket={row['bucket']} published={row['published']} "
            f"created_at={row['created_at'] or 'n/a'} action={row['recommended_action']}"
        )
    return "\n".join(lines)


def _load_generated_content(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    content_type: str | None,
) -> list[dict[str, Any]]:
    if not columns or "id" not in columns:
        return []

    select_columns = [
        "id",
        _column_expr(columns, "content_type"),
        _column_expr(columns, "published", "0"),
        _column_expr(columns, "published_url"),
        _column_expr(columns, "published_at"),
        _column_expr(columns, "created_at"),
        _column_expr(columns, "source_commits"),
        _column_expr(columns, "source_messages"),
        _column_expr(columns, "source_activity_ids"),
    ]
    where: list[str] = []
    params: list[Any] = []
    if content_type and "content_type" in columns:
        where.append("content_type = ?")
        params.append(content_type)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM generated_content
            {where_sql}
            ORDER BY id ASC""",
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows]


def _gap_row(
    row: dict[str, Any],
    *,
    columns: set[str],
    stale_cutoff: datetime,
    report_warnings: list[str],
) -> dict[str, Any] | None:
    content_id = int(row["id"])
    source_counts: dict[str, int] = {}
    for field in SOURCE_COLUMNS:
        values = _source_list(row.get(field), field, content_id, columns, report_warnings)
        source_counts[field] = len(values)
    if sum(source_counts.values()) > 0:
        return None

    published = _is_published(row)
    created_at = row.get("created_at")
    created = _parse_datetime(created_at)
    if published:
        bucket = "published_no_sources"
        action = "Backfill durable source attribution or unpublish until provenance is restored."
    elif created is not None and created < stale_cutoff:
        bucket = "stale_unpublished_no_sources"
        action = "Refresh or discard stale unpublished content before reuse."
    else:
        bucket = "no_sources"
        action = "Attach source commits, messages, or GitHub activity before reuse or publication."

    return {
        "content_id": content_id,
        "content_type": row.get("content_type"),
        "published": published,
        "published_url": row.get("published_url"),
        "created_at": created_at,
        "source_counts": source_counts,
        "bucket": bucket,
        "recommended_action": action,
    }


def _source_list(
    value: Any,
    field: str,
    content_id: int,
    columns: set[str],
    report_warnings: list[str],
) -> list[Any]:
    if field not in columns:
        return []
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError) as exc:
        report_warnings.append(f"generated_content {content_id} has malformed {field}: {exc}")
        return []
    if not isinstance(parsed, list):
        report_warnings.append(
            f"generated_content {content_id} has non-list {field}: {type(parsed).__name__}"
        )
        return []
    return parsed


def _is_published(row: dict[str, Any]) -> bool:
    published = row.get("published")
    if isinstance(published, str):
        return published.strip().lower() in {"1", "true", "yes", "published"}
    return bool(published or row.get("published_url") or row.get("published_at"))


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    tables = {str(row["name"] if isinstance(row, sqlite3.Row) else row[0]) for row in rows}
    return {
        table: {
            str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
            for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")
        }
        for table in tables
    }


def _required_columns() -> set[str]:
    return {
        "id",
        "content_type",
        "published",
        "published_url",
        "created_at",
        *SOURCE_COLUMNS,
    }


def _column_expr(columns: set[str], column: str, fallback: str = "NULL") -> str:
    return column if column in columns else f"{fallback} AS {column}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3 connection or database wrapper with .conn")
    return conn


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


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
