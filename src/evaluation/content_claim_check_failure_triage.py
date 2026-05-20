"""Triage failed or unresolved content claim checks."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
CLAIM_CHECK_COLUMNS = {
    "content_id",
    "supported_count",
    "unsupported_count",
    "annotation_text",
    "created_at",
    "updated_at",
}
GENERATED_CONTENT_COLUMNS = {"id", "content_type", "published", "published_at", "status"}


def build_content_claim_check_failure_triage_report(
    claim_rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic triage report for claim-check failures."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    findings: list[dict[str, Any]] = []
    unsupported_rows = 0
    published_unresolved = 0
    issue_counts: Counter[str] = Counter()

    for raw in claim_rows:
        row = _normalize_row(raw)
        if row["effective_checked_at_dt"] is not None and row["effective_checked_at_dt"] < cutoff:
            continue
        row_issues = _row_issues(row)
        if row["unsupported_count"] > 0:
            unsupported_rows += 1
        if row["unsupported_count"] > 0 and row["published"]:
            published_unresolved += 1
        for issue_type, details in row_issues:
            issue_counts[issue_type] += 1
            findings.append(_finding(row, issue_type, details))

    findings.sort(key=lambda item: (_issue_rank(item["issue_type"]), item["content_id"], item["issue_type"]))
    shown = findings[:limit]
    return {
        "artifact_type": "content_claim_check_failure_triage",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "limit": limit,
            "lookback_start": cutoff.isoformat(),
            "lookback_end": generated_at.isoformat(),
        },
        "summary": {
            "checked_row_count": len(claim_rows),
            "unsupported_row_count": unsupported_rows,
            "published_unresolved_row_count": published_unresolved,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": dict(sorted(issue_counts.items())),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {
            table: sorted(columns)
            for table, columns in sorted((missing_columns or {}).items())
            if columns
        },
        "empty_state": {
            "is_empty": not claim_rows,
            "reason": "missing_schema" if missing_tables or missing_columns else "no_claim_checks" if not claim_rows else None,
        },
    }


def build_content_claim_check_failure_triage_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = sorted(table for table in ("content_claim_checks", "generated_content") if table not in schema)
    missing_columns: dict[str, list[str]] = {}
    if "content_claim_checks" in schema:
        missing = sorted(CLAIM_CHECK_COLUMNS - schema["content_claim_checks"])
        if missing:
            missing_columns["content_claim_checks"] = missing
    if "generated_content" in schema:
        missing = sorted(GENERATED_CONTENT_COLUMNS - schema["generated_content"])
        if missing:
            missing_columns["generated_content"] = missing
    can_load = "content_claim_checks" in schema and "content_id" in schema["content_claim_checks"]
    return build_content_claim_check_failure_triage_report(
        _load_rows(conn, schema) if can_load else [],
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_content_claim_check_failure_triage_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_claim_check_failure_triage_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Content Claim Check Failure Triage",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} limit={report['filters']['limit']}",
        (
            f"Totals: checks={summary['checked_row_count']} unsupported={summary['unsupported_row_count']} "
            f"published_unresolved={summary['published_unresolved_row_count']} findings={summary['finding_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report["missing_columns"].items())
        ]
        lines.append("Missing columns: " + "; ".join(missing))
    if not report["findings"]:
        lines.append("No content claim-check failure triage findings found.")
        return "\n".join(lines)
    lines.append("Findings:")
    for finding in report["findings"]:
        lines.append(
            f"  - content_id={finding['content_id']} type={finding['issue_type']} "
            f"content_type={finding['content_type']} published={finding['published']} "
            f"supported={finding['supported_count']} unsupported={finding['unsupported_count']} "
            f"details={finding['details']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    ccc = schema["content_claim_checks"]
    gc = schema.get("generated_content", set())
    has_gc_join = "generated_content" in schema and "id" in gc
    join = "LEFT JOIN generated_content gc ON gc.id = ccc.content_id" if has_gc_join else ""
    updated_expr = _expr(ccc, "updated_at", fallback="NULL", alias="ccc")
    created_expr = _expr(ccc, "created_at", fallback="NULL", alias="ccc")
    effective_expr = f"COALESCE({updated_expr}, {created_expr})"
    select = [
        _expr(ccc, "id", fallback="ccc.rowid", alias="ccc") + " AS claim_check_id",
        "ccc.content_id AS content_id",
        _expr(ccc, "supported_count", fallback="NULL", alias="ccc") + " AS supported_count",
        _expr(ccc, "unsupported_count", fallback="NULL", alias="ccc") + " AS unsupported_count",
        _expr(ccc, "annotation_text", fallback="NULL", alias="ccc") + " AS annotation_text",
        created_expr + " AS claim_check_created_at",
        updated_expr + " AS claim_check_updated_at",
        effective_expr + " AS effective_checked_at",
        (_expr(gc, "id", fallback="NULL", alias="gc") if has_gc_join else "NULL") + " AS generated_content_id",
        (_expr(gc, "content_type", fallback="'unknown'", alias="gc") if has_gc_join else "'unknown'") + " AS content_type",
        (_expr(gc, "published", fallback="NULL", alias="gc") if has_gc_join else "NULL") + " AS published",
        (_expr(gc, "published_at", fallback="NULL", alias="gc") if has_gc_join else "NULL") + " AS published_at",
        (_expr(gc, "status", fallback="NULL", alias="gc") if has_gc_join else "NULL") + " AS status",
    ]
    order = f"{effective_expr}, ccc.content_id, ccc.rowid"
    query = f"SELECT {', '.join(select)} FROM content_claim_checks ccc {join} ORDER BY {order}"
    return [dict(row) for row in conn.execute(query).fetchall()]


def _row_issues(row: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    issues: list[tuple[str, dict[str, Any]]] = []
    if row["unsupported_count"] > 0:
        issues.append(("unsupported_claims", {"unsupported_count": row["unsupported_count"]}))
    if row["supported_count"] < 0 or row["unsupported_count"] < 0:
        issues.append(
            (
                "negative_claim_count",
                {"supported_count": row["supported_count"], "unsupported_count": row["unsupported_count"]},
            )
        )
    if row["unsupported_count"] > 0 and not row["annotation_text"]:
        issues.append(("missing_annotation", {"unsupported_count": row["unsupported_count"]}))
    if not row["generated_content_id"]:
        issues.append(("orphan_claim_check", {"content_id": row["content_id"]}))
    if row["unsupported_count"] > 0 and row["published"]:
        issues.append(("published_with_unsupported_claims", {"unsupported_count": row["unsupported_count"]}))
    return issues


def _finding(row: dict[str, Any], issue_type: str, details: dict[str, Any]) -> dict[str, Any]:
    return {
        "claim_check_id": row["claim_check_id"],
        "content_id": row["content_id"],
        "content_type": row["content_type"],
        "issue_type": issue_type,
        "supported_count": row["supported_count"],
        "unsupported_count": row["unsupported_count"],
        "annotation_text": row["annotation_text"],
        "published": row["published"],
        "claim_check_updated_at": row["claim_check_updated_at"],
        "details": details,
    }


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    published = _truthy(row.get("published")) or bool(row.get("published_at")) or _text(row.get("status")).lower() in {"published", "live"}
    return {
        "claim_check_id": _text(row.get("claim_check_id")) or _text(row.get("content_id")),
        "content_id": _text(row.get("content_id")),
        "content_type": _text(row.get("content_type")) or "unknown",
        "supported_count": _int(row.get("supported_count")),
        "unsupported_count": _int(row.get("unsupported_count")),
        "annotation_text": _text(row.get("annotation_text")),
        "generated_content_id": _text(row.get("generated_content_id")),
        "published": published,
        "claim_check_created_at": row.get("claim_check_created_at"),
        "claim_check_updated_at": row.get("claim_check_updated_at"),
        "effective_checked_at_dt": _parse_dt(row.get("effective_checked_at")),
    }


def _issue_rank(issue_type: str) -> int:
    ranks = {
        "published_with_unsupported_claims": 0,
        "unsupported_claims": 1,
        "negative_claim_count": 2,
        "missing_annotation": 3,
        "orphan_claim_check": 4,
    }
    return ranks.get(issue_type, 99)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY name").fetchall()
    return {row["name"]: {info["name"] for info in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _expr(columns: set[str], *names: str, fallback: str, alias: str | None = None) -> str:
    prefix = f"{alias}." if alias else ""
    for name in names:
        if name in columns:
            return f"{prefix}{name}"
    return fallback


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "published"}
    return bool(value)


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
