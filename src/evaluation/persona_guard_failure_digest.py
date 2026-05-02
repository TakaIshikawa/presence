"""Digest failed and borderline persona guard rows for operator review."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 50
CONTENT_EXCERPT_LENGTH = 220
PASSING_STATUSES = {"pass", "passed"}


@dataclass(frozen=True)
class PersonaGuardFailureDigestRow:
    """One checked persona guard row that needs review."""

    content_id: int
    content_type: str
    status: str
    passed: bool
    score: float
    reasons: Any
    metrics: Any
    content_excerpt: str
    content_created_at: str | None
    guard_created_at: str | None
    guard_updated_at: str | None
    checked_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PersonaGuardFailureDigest:
    """Persona guard failure digest with summary buckets."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    rows: tuple[PersonaGuardFailureDigestRow, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    @property
    def has_issues(self) -> bool:
        return bool(self.rows)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "persona_guard_failure_digest",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "has_issues": self.has_issues,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": _stable_totals(self.totals),
        }


def build_persona_guard_failure_digest(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_score: float | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> PersonaGuardFailureDigest:
    """Return checked persona guard rows that failed, look suspicious, or score low."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if min_score is not None and not 0 <= min_score <= 1:
        raise ValueError("min_score must be between 0 and 1")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "cutoff": cutoff.isoformat(),
        "limit": limit,
        "min_score": min_score,
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
        if _is_actionable(row, min_score=min_score)
    ]
    rows.sort(key=_sort_key)
    rows = rows[:limit]

    status_counts = Counter(row.status for row in rows)
    content_type_counts = Counter(row.content_type for row in rows)
    failed_count = sum(not row.passed for row in rows)
    borderline_count = sum(row.passed for row in rows)

    return PersonaGuardFailureDigest(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "row_count": len(rows),
            "failed_count": failed_count,
            "borderline_count": borderline_count,
            "by_status": dict(sorted(status_counts.items())),
            "by_content_type": dict(sorted(content_type_counts.items())),
        },
        rows=tuple(rows),
        missing_tables=(),
        missing_columns=missing_columns,
    )


def format_persona_guard_failure_digest_json(
    report: PersonaGuardFailureDigest,
) -> str:
    """Serialize the persona guard failure digest as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_persona_guard_failure_digest_text(
    report: PersonaGuardFailureDigest,
) -> str:
    """Render the persona guard failure digest for operator review."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Persona Guard Failure Digest",
        f"Generated: {report.generated_at}",
        (
            f"Window: {filters['days']} days cutoff={filters['cutoff']} "
            f"min_score={filters['min_score']} limit={filters['limit']}"
        ),
        (
            f"Totals: rows={totals['row_count']} failed={totals['failed_count']} "
            f"borderline={totals['borderline_count']}"
        ),
        "By status: " + _format_counts(totals["by_status"]),
        "By content_type: " + _format_counts(totals["by_content_type"]),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        ]
        lines.append("Missing columns: " + "; ".join(missing))
    lines.append("")

    if not report.rows:
        lines.append("No checked persona guard failures or borderline rows found.")
        return "\n".join(lines)

    lines.append("Rows:")
    for row in report.rows:
        reason_text = _compact_value(row.reasons)
        lines.append(
            f"- content_id={row.content_id} type={row.content_type} "
            f"status={row.status} passed={row.passed} score={row.score:.3f} "
            f"checked_at={row.checked_at or '-'}"
        )
        lines.append(f"  reasons={reason_text or '-'}")
        lines.append(f"  excerpt={row.content_excerpt or '-'}")
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    *,
    schema: dict[str, set[str]],
    cutoff: datetime,
) -> list[dict[str, Any]]:
    gc_columns = schema["generated_content"]
    guard_columns = schema["content_persona_guard"]
    guard_created_expr = _column_expr(guard_columns, "created_at", "NULL", alias="cpg")
    guard_updated_expr = _column_expr(guard_columns, "updated_at", "NULL", alias="cpg")
    content_created_expr = _column_expr(gc_columns, "created_at", "NULL", alias="gc")
    checked_at_expr = f"COALESCE({guard_updated_expr}, {guard_created_expr}, {content_created_expr})"

    rows = conn.execute(
        f"""SELECT
               cpg.content_id AS content_id,
               {_column_expr(gc_columns, "content_type", "'unknown'", alias="gc")} AS content_type,
               {_column_expr(gc_columns, "content", "''", alias="gc")} AS content,
               {content_created_expr} AS content_created_at,
               {_column_expr(guard_columns, "checked", "0", alias="cpg")} AS checked,
               {_column_expr(guard_columns, "passed", "0", alias="cpg")} AS passed,
               {_column_expr(guard_columns, "status", "'unknown'", alias="cpg")} AS status,
               {_column_expr(guard_columns, "score", "0", alias="cpg")} AS score,
               {_column_expr(guard_columns, "reasons", "NULL", alias="cpg")} AS reasons,
               {_column_expr(guard_columns, "metrics", "NULL", alias="cpg")} AS metrics,
               {guard_created_expr} AS guard_created_at,
               {guard_updated_expr} AS guard_updated_at,
               {checked_at_expr} AS checked_at
           FROM content_persona_guard cpg
           INNER JOIN generated_content gc ON gc.id = cpg.content_id
           WHERE cpg.checked = 1
             AND datetime({checked_at_expr}) >= datetime(?)
           ORDER BY datetime({checked_at_expr}) DESC, cpg.content_id ASC""",
        (cutoff.isoformat(),),
    ).fetchall()
    return [dict(row) for row in rows]


def _build_row(row: dict[str, Any]) -> PersonaGuardFailureDigestRow:
    score = _float(row.get("score"))
    return PersonaGuardFailureDigestRow(
        content_id=int(row["content_id"]),
        content_type=str(row.get("content_type") or "unknown"),
        status=_normalize_status(row.get("status")),
        passed=bool(_int(row.get("passed"))),
        score=round(score, 3),
        reasons=_parse_json_value(row.get("reasons"), default=[]),
        metrics=_parse_json_value(row.get("metrics"), default={}),
        content_excerpt=_excerpt(row.get("content")),
        content_created_at=row.get("content_created_at"),
        guard_created_at=row.get("guard_created_at"),
        guard_updated_at=row.get("guard_updated_at"),
        checked_at=row.get("checked_at"),
    )


def _is_actionable(row: dict[str, Any], *, min_score: float | None) -> bool:
    if not bool(_int(row.get("checked"))):
        return False
    passed = bool(_int(row.get("passed")))
    status = _normalize_status(row.get("status"))
    score = _float(row.get("score"))
    return not passed or status not in PASSING_STATUSES or (
        min_score is not None and score < min_score
    )


def _sort_key(row: PersonaGuardFailureDigestRow) -> tuple[Any, ...]:
    checked_at = _parse_datetime(row.checked_at)
    checked_ts = checked_at.timestamp() if checked_at else float("-inf")
    passed_rank = 1 if row.passed else 0
    return (passed_rank, row.score, -checked_ts, row.content_type, row.content_id)


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    expected = {
        "generated_content": {"id", "content", "content_type", "created_at"},
        "content_persona_guard": {
            "content_id",
            "checked",
            "passed",
            "status",
            "score",
            "reasons",
            "metrics",
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
        "content_id" in missing_columns.get("content_persona_guard", ())
    )


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> PersonaGuardFailureDigest:
    return PersonaGuardFailureDigest(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "row_count": 0,
            "failed_count": 0,
            "borderline_count": 0,
            "by_status": {},
            "by_content_type": {},
        },
        rows=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    except sqlite3.Error:
        return {}
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


def _parse_json_value(value: Any, *, default: Any) -> Any:
    if value is None or value == "":
        return default
    try:
        return json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return str(value)


def _excerpt(value: Any, *, limit: int = CONTENT_EXCERPT_LENGTH) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))


def _compact_value(value: Any) -> str:
    if isinstance(value, list):
        return "; ".join(str(item) for item in value)
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    return str(value or "")


def _stable_totals(totals: dict[str, Any]) -> dict[str, Any]:
    result = dict(sorted(totals.items()))
    for key in ("by_content_type", "by_status"):
        if isinstance(result.get(key), dict):
            result[key] = dict(sorted(result[key].items()))
    return result


def _normalize_status(value: Any) -> str:
    return str(value or "unknown").strip().lower() or "unknown"


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
