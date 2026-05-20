"""Report generated content with missing or stale claim-check coverage."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 25
DEFAULT_UNSUPPORTED_THRESHOLD = 0

_ISSUE_RANK = {
    "missing_claim_check": 0,
    "unsupported_claims": 1,
    "stale_claim_check": 2,
}


def build_content_claim_check_coverage_report(
    rows: list[dict[str, Any]],
    *,
    unsupported_threshold: int = DEFAULT_UNSUPPORTED_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Build a deterministic claim-check coverage report from joined rows."""
    if unsupported_threshold < 0:
        raise ValueError("unsupported_threshold must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    content_rows = _collapse_content_rows(rows)
    in_scope = [row for row in content_rows if _is_published_or_queued(row)]
    issue_items = _issue_items(in_scope, unsupported_threshold=unsupported_threshold)
    issue_items.sort(key=_issue_sort_key)

    counts = {issue_type: 0 for issue_type in _ISSUE_RANK}
    for item in issue_items:
        counts[item["issue_type"]] += 1

    return {
        "artifact_type": "content_claim_check_coverage",
        "generated_at": generated_at.isoformat(),
        "missing_tables": list(missing_tables),
        "thresholds": {
            "unsupported_threshold": unsupported_threshold,
            "limit": limit,
        },
        "summary": {
            "content_rows_scanned": len(content_rows),
            "eligible_content_count": len(in_scope),
            "issue_count": len(issue_items),
            "missing_claim_check_count": counts["missing_claim_check"],
            "unsupported_claims_count": counts["unsupported_claims"],
            "stale_claim_check_count": counts["stale_claim_check"],
        },
        "issue_items": issue_items[:limit],
    }


def build_content_claim_check_coverage_report_from_db(
    db_or_conn: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = ("generated_content", "content_claim_checks")
    missing_tables = tuple(table for table in required if table not in schema)
    if missing_tables:
        return build_content_claim_check_coverage_report(
            [],
            missing_tables=missing_tables,
            **kwargs,
        )
    rows = _load_rows(conn, schema)
    return build_content_claim_check_coverage_report(
        rows,
        missing_tables=missing_tables,
        **kwargs,
    )


def format_content_claim_check_coverage_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_claim_check_coverage_text(report: dict[str, Any]) -> str:
    thresholds = report["thresholds"]
    summary = report["summary"]
    lines = [
        "Content Claim Check Coverage",
        f"Generated: {report['generated_at']}",
        (
            f"Thresholds: unsupported_threshold={thresholds['unsupported_threshold']} "
            f"limit={thresholds['limit']}"
        ),
        (
            f"Totals: scanned={summary['content_rows_scanned']} "
            f"eligible={summary['eligible_content_count']} issues={summary['issue_count']} "
            f"missing={summary['missing_claim_check_count']} "
            f"unsupported={summary['unsupported_claims_count']} "
            f"stale={summary['stale_claim_check_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["issue_items"]:
        lines.extend(["", "Issue items:"])
        for item in report["issue_items"]:
            lines.append(
                f"- type={item['issue_type']} content_id={item['content_id']} "
                f"status={item['status']} unsupported={item['unsupported_count']} "
                f"content_updated_at={item['content_updated_at'] or '-'} "
                f"claim_check_updated_at={item['claim_check_updated_at'] or '-'}"
            )
    elif not report["missing_tables"]:
        lines.append("No content claim-check coverage issues found.")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    gc = schema["generated_content"]
    cc = schema["content_claim_checks"]
    selected = [
        _column_expr(gc, "id", "gc", "content_id", default="gc.rowid"),
        _column_expr(gc, "content_type", "gc", "content_type", default="'unknown'"),
        _column_expr(gc, "status", "gc", "status", default="NULL"),
        _column_expr(gc, "published", "gc", "published", default="NULL"),
        _column_expr(gc, "created_at", "gc", "content_created_at", default="NULL"),
        _column_expr(gc, "updated_at", "gc", "content_updated_at", default="NULL"),
        _column_expr(cc, "content_id", "cc", "claim_check_content_id", default="NULL"),
        _column_expr(cc, "supported_count", "cc", "supported_count", default="0"),
        _column_expr(cc, "unsupported_count", "cc", "unsupported_count", default="0"),
        _column_expr(cc, "created_at", "cc", "claim_check_created_at", default="NULL"),
        _column_expr(cc, "updated_at", "cc", "claim_check_updated_at", default="NULL"),
    ]
    join = "cc.content_id = gc.id" if "content_id" in cc and "id" in gc else "0"
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(selected)}
                FROM generated_content gc
                LEFT JOIN content_claim_checks cc ON {join}
                ORDER BY gc.id ASC"""
        ).fetchall()
    ]


def _collapse_content_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    collapsed: dict[int, dict[str, Any]] = {}
    for raw in rows:
        row = _normalize_row(raw)
        existing = collapsed.get(row["content_id"])
        if existing is None or _claim_sort_value(row) > _claim_sort_value(existing):
            collapsed[row["content_id"]] = row
    return [collapsed[key] for key in sorted(collapsed)]


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "content_id": _int(row.get("content_id")),
        "content_type": str(row.get("content_type") or "unknown"),
        "status": _status(row),
        "published": _maybe_int(row.get("published")),
        "content_created_at": row.get("content_created_at"),
        "content_updated_at": row.get("content_updated_at") or row.get("content_created_at"),
        "has_claim_check": row.get("claim_check_content_id") is not None,
        "supported_count": _int(row.get("supported_count")),
        "unsupported_count": _int(row.get("unsupported_count")),
        "claim_check_created_at": row.get("claim_check_created_at"),
        "claim_check_updated_at": row.get("claim_check_updated_at") or row.get("claim_check_created_at"),
    }


def _issue_items(rows: list[dict[str, Any]], *, unsupported_threshold: int) -> list[dict[str, Any]]:
    issues = []
    for row in rows:
        if not row["has_claim_check"]:
            issues.append(_issue(row, "missing_claim_check"))
            continue
        if row["unsupported_count"] > unsupported_threshold:
            issues.append(_issue(row, "unsupported_claims"))
        if _is_stale(row):
            issues.append(_issue(row, "stale_claim_check"))
    return issues


def _issue(row: dict[str, Any], issue_type: str) -> dict[str, Any]:
    return {
        "issue_type": issue_type,
        "content_id": row["content_id"],
        "content_type": row["content_type"],
        "status": row["status"],
        "supported_count": row["supported_count"],
        "unsupported_count": row["unsupported_count"],
        "content_created_at": row["content_created_at"],
        "content_updated_at": row["content_updated_at"],
        "claim_check_created_at": row["claim_check_created_at"],
        "claim_check_updated_at": row["claim_check_updated_at"],
    }


def _is_published_or_queued(row: dict[str, Any]) -> bool:
    status = str(row.get("status") or "").strip().lower()
    if status in {"published", "queued"}:
        return True
    return row.get("published") == 1


def _is_stale(row: dict[str, Any]) -> bool:
    content_updated = _parse_datetime(row.get("content_updated_at"))
    claim_updated = _parse_datetime(row.get("claim_check_updated_at"))
    return bool(content_updated and claim_updated and claim_updated < content_updated)


def _status(row: dict[str, Any]) -> str:
    raw = str(row.get("status") or "").strip().lower()
    if raw:
        return raw
    return "published" if _maybe_int(row.get("published")) == 1 else "unknown"


def _issue_sort_key(item: dict[str, Any]) -> tuple[int, int]:
    return (_ISSUE_RANK.get(item["issue_type"], 99), item["content_id"])


def _claim_sort_value(row: dict[str, Any]) -> tuple[float, int]:
    parsed = _parse_datetime(row.get("claim_check_updated_at"))
    timestamp = parsed.timestamp() if parsed else float("-inf")
    return (timestamp, row["unsupported_count"])


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], name: str, alias: str, out: str, *, default: str) -> str:
    return f"{alias}.{name} AS {out}" if name in columns else f"{default} AS {out}"


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _maybe_int(value: Any) -> int | None:
    if value is None:
        return None
    return _int(value)
