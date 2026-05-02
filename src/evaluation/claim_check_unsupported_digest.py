"""Digest unsupported claim-check findings for generated content."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 50
ANNOTATION_EXCERPT_LENGTH = 220

_UNSUPPORTED_RE = re.compile(
    r"\b(unsupported|needs[- ]evidence|needs evidence|unverified|unverifiable|"
    r"not found in sources|no evidence|missing evidence)\b",
    re.IGNORECASE,
)
_SEVERITY_RANK = {"critical": 0, "high": 1, "medium": 2}


@dataclass(frozen=True)
class ClaimCheckUnsupportedDigestRow:
    """One generated content item with unsupported claim-check evidence."""

    content_id: int
    content_type: str
    severity: str
    unsupported_count: int
    supported_count: int
    annotation_excerpt: str
    content_created_at: str | None
    claim_check_created_at: str | None
    claim_check_updated_at: str | None
    suggested_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaimCheckUnsupportedDigest:
    """Unsupported claim-check digest with schema metadata."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    buckets: dict[str, dict[str, int]]
    rows: tuple[ClaimCheckUnsupportedDigestRow, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    @property
    def has_issues(self) -> bool:
        return bool(self.rows)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claim_check_unsupported_digest",
            "buckets": {
                content_type: dict(sorted(severities.items()))
                for content_type, severities in sorted(self.buckets.items())
            },
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "has_issues": self.has_issues,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


def build_claim_check_unsupported_digest(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaimCheckUnsupportedDigest:
    """Return a deterministic digest of unsupported claim-check results."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "cutoff": cutoff.isoformat(),
        "limit": limit,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or _missing_join_columns(missing_columns):
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = [
        _build_row(row)
        for row in _load_rows(conn, schema=schema, cutoff=cutoff)
        if _is_actionable(row)
    ]
    rows.sort(key=_sort_key)
    rows = rows[:limit]

    bucket_counter: Counter[tuple[str, str]] = Counter(
        (row.content_type, row.severity) for row in rows
    )
    buckets: dict[str, dict[str, int]] = {}
    for (content_type, severity), count in sorted(bucket_counter.items()):
        buckets.setdefault(content_type, {})[severity] = count

    severity_counts = Counter(row.severity for row in rows)
    return ClaimCheckUnsupportedDigest(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "row_count": len(rows),
            "by_severity": dict(sorted(severity_counts.items())),
        },
        buckets=buckets,
        rows=tuple(rows),
        missing_tables=(),
        missing_columns=missing_columns,
    )


def format_claim_check_unsupported_digest_json(
    report: ClaimCheckUnsupportedDigest,
) -> str:
    """Serialize the unsupported claim-check digest as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claim_check_unsupported_digest_text(
    report: ClaimCheckUnsupportedDigest,
) -> str:
    """Render the unsupported claim-check digest for operators."""
    lines = [
        "Unsupported Claim-check Digest",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['days']} days "
            f"cutoff={report.filters['cutoff']} limit={report.filters['limit']}"
        ),
        f"Totals: rows={report.totals['row_count']}",
    ]
    if report.buckets:
        bucket_text = [
            (
                f"{content_type}("
                f"{', '.join(f'{severity}={count}' for severity, count in severities.items())})"
            )
            for content_type, severities in sorted(report.buckets.items())
        ]
        lines.append(f"Buckets: {'; '.join(bucket_text)}")
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        ]
        lines.append(f"Missing columns: {'; '.join(missing)}")
    lines.append("")

    if not report.rows:
        lines.append("No unsupported claim-check findings found.")
        return "\n".join(lines)

    lines.append("Findings:")
    for row in report.rows:
        lines.append(
            f"  - content_id={row.content_id} type={row.content_type} "
            f"severity={row.severity} unsupported={row.unsupported_count} "
            f"supported={row.supported_count} updated_at={row.claim_check_updated_at or '-'}"
        )
        lines.append(f"      action: {row.suggested_action}")
        lines.append(f"      annotation: {row.annotation_excerpt or '-'}")
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    *,
    schema: dict[str, set[str]],
    cutoff: datetime,
) -> list[dict[str, Any]]:
    gc_columns = schema["generated_content"]
    ccc_columns = schema["content_claim_checks"]
    updated_expr = _column_expr(ccc_columns, "updated_at", fallback="NULL", alias="ccc")
    created_expr = _column_expr(ccc_columns, "created_at", fallback="NULL", alias="ccc")
    effective_checked_at = (
        f"COALESCE({updated_expr}, {created_expr}, "
        f"{_column_expr(gc_columns, 'created_at', fallback='NULL', alias='gc')})"
    )
    rows = conn.execute(
        f"""SELECT
               ccc.content_id AS content_id,
               {_column_expr(gc_columns, "content_type", "'unknown'", alias="gc")} AS content_type,
               {_column_expr(gc_columns, "created_at", "NULL", alias="gc")} AS content_created_at,
               {_column_expr(ccc_columns, "supported_count", "0", alias="ccc")} AS supported_count,
               {_column_expr(ccc_columns, "unsupported_count", "0", alias="ccc")} AS unsupported_count,
               {_column_expr(ccc_columns, "annotation_text", "NULL", alias="ccc")} AS annotation_text,
               {created_expr} AS claim_check_created_at,
               {updated_expr} AS claim_check_updated_at,
               {effective_checked_at} AS effective_checked_at
           FROM content_claim_checks ccc
           INNER JOIN generated_content gc ON gc.id = ccc.content_id
           WHERE datetime({effective_checked_at}) >= datetime(?)
           ORDER BY ccc.content_id ASC""",
        (cutoff.isoformat(),),
    ).fetchall()
    return [dict(row) for row in rows]


def _build_row(row: dict[str, Any]) -> ClaimCheckUnsupportedDigestRow:
    unsupported_count = _int(row.get("unsupported_count"))
    supported_count = _int(row.get("supported_count"))
    severity = _severity(
        unsupported_count=unsupported_count,
        annotation_text=row.get("annotation_text"),
    )
    return ClaimCheckUnsupportedDigestRow(
        content_id=int(row["content_id"]),
        content_type=str(row.get("content_type") or "unknown"),
        severity=severity,
        unsupported_count=unsupported_count,
        supported_count=supported_count,
        annotation_excerpt=_excerpt(row.get("annotation_text")),
        content_created_at=row.get("content_created_at"),
        claim_check_created_at=row.get("claim_check_created_at"),
        claim_check_updated_at=row.get("claim_check_updated_at"),
        suggested_action=_suggested_action(severity),
    )


def _is_actionable(row: dict[str, Any]) -> bool:
    return _int(row.get("unsupported_count")) > 0 or bool(
        _UNSUPPORTED_RE.search(str(row.get("annotation_text") or ""))
    )


def _severity(*, unsupported_count: int, annotation_text: Any) -> str:
    if unsupported_count >= 3:
        return "critical"
    if unsupported_count >= 1:
        return "high"
    if _UNSUPPORTED_RE.search(str(annotation_text or "")):
        return "medium"
    return "medium"


def _suggested_action(severity: str) -> str:
    if severity == "critical":
        return "block publication, remove or substantiate unsupported claims, then rerun claim check"
    if severity == "high":
        return "revise unsupported claims or add evidence before publication or reuse"
    return "review annotation wording and add evidence or rerun claim check"


def _sort_key(row: ClaimCheckUnsupportedDigestRow) -> tuple[Any, ...]:
    updated = _parse_datetime(row.claim_check_updated_at or row.claim_check_created_at)
    updated_ts = updated.timestamp() if updated else float("-inf")
    return (_SEVERITY_RANK.get(row.severity, 99), -updated_ts, row.content_id)


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    expected = {
        "generated_content": {"id", "content_type", "created_at"},
        "content_claim_checks": {
            "content_id",
            "supported_count",
            "unsupported_count",
            "annotation_text",
            "created_at",
            "updated_at",
        },
    }
    missing_tables = tuple(table for table in expected if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in expected.items()
        if table in schema and columns - schema[table]
    }
    return missing_tables, missing_columns


def _missing_join_columns(missing_columns: dict[str, tuple[str, ...]]) -> bool:
    return "id" in missing_columns.get("generated_content", ()) or (
        "content_id" in missing_columns.get("content_claim_checks", ())
    )


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> ClaimCheckUnsupportedDigest:
    return ClaimCheckUnsupportedDigest(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={"row_count": 0, "by_severity": {}},
        buckets={},
        rows=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str = "NULL",
    *,
    alias: str,
) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _excerpt(value: Any, *, limit: int = ANNOTATION_EXCERPT_LENGTH) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."
