"""Summarize generated content blocked by quality gates."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_COUNT = 1
REPRESENTATIVE_ID_LIMIT = 5


@dataclass(frozen=True)
class QualityGateFailureTrendRow:
    """One grouped quality-gate failure trend."""

    gate: str
    reason_code: str
    content_type: str
    week: str
    failure_count: int
    affected_content_ids: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["affected_content_ids"] = list(self.affected_content_ids)
        return payload


@dataclass(frozen=True)
class QualityGateFailureTrendsReport:
    """Quality-gate failure trend report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    rows: tuple[QualityGateFailureTrendRow, ...]
    weekly_totals: dict[str, int]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "quality_gate_failure_trends",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "row_count": len(self.rows),
            "rows": [row.to_dict() for row in self.rows],
            "totals": _stable_totals(self.totals),
            "weekly_totals": dict(sorted(self.weekly_totals.items())),
        }


@dataclass(frozen=True)
class _FailureEvent:
    gate: str
    reason_code: str
    content_type: str
    content_id: int
    occurred_at: datetime


def build_quality_gate_failure_trends_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_count: int = DEFAULT_MIN_COUNT,
    now: datetime | None = None,
) -> QualityGateFailureTrendsReport:
    """Return grouped recent quality-gate failures for generated content."""
    days = _positive_int(days, "days")
    min_count = _positive_int(min_count, "min_count")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "cutoff": cutoff.isoformat(),
        "min_count": min_count,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if "generated_content" not in schema or (
        "id" in missing_columns.get("generated_content", ())
        or "content_type" in missing_columns.get("generated_content", ())
    ):
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    events: list[_FailureEvent] = []
    if "content_persona_guard" in schema and not _missing_persona_join_columns(missing_columns):
        events.extend(
            _load_persona_guard_events(
                conn,
                gc_columns=schema["generated_content"],
                guard_columns=schema["content_persona_guard"],
                cutoff=cutoff,
                window_end=generated_at,
            )
        )
    if "content_claim_checks" in schema and not _missing_claim_join_columns(missing_columns):
        events.extend(
            _load_claim_check_events(
                conn,
                gc_columns=schema["generated_content"],
                claim_columns=schema["content_claim_checks"],
                cutoff=cutoff,
                window_end=generated_at,
            )
        )

    return _build_report(
        events,
        generated_at=generated_at,
        filters=filters,
        min_count=min_count,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_quality_gate_failure_trends_json(
    report: QualityGateFailureTrendsReport,
) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_quality_gate_failure_trends_text(
    report: QualityGateFailureTrendsReport,
) -> str:
    """Render a compact operator-facing report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Quality Gate Failure Trends",
        f"Generated: {report.generated_at}",
        f"Window: days={filters['days']} cutoff={filters['cutoff']} min_count={filters['min_count']}",
        (
            f"Totals: failures={totals['failure_count']} rows={len(report.rows)} "
            f"content={totals['affected_content_count']}"
        ),
        "By gate: " + _format_counts(totals["by_gate"]),
        "By content_type: " + _format_counts(totals["by_content_type"]),
    ]
    if report.weekly_totals:
        lines.append("Weekly totals: " + _format_counts(report.weekly_totals))
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + missing)
    lines.append("")

    if not report.rows:
        lines.append("No quality-gate failures found for the selected window.")
        return "\n".join(lines)

    lines.append("Rows:")
    for row in report.rows:
        ids = ", ".join(str(content_id) for content_id in row.affected_content_ids)
        lines.append(
            f"- week={row.week} gate={row.gate} reason={row.reason_code} "
            f"type={row.content_type} count={row.failure_count} ids={ids or '-'}"
        )
    return "\n".join(lines)


def _load_persona_guard_events(
    conn: sqlite3.Connection,
    *,
    gc_columns: set[str],
    guard_columns: set[str],
    cutoff: datetime,
    window_end: datetime,
) -> list[_FailureEvent]:
    guard_created = _column_expr(guard_columns, "created_at", "NULL", alias="cpg")
    guard_updated = _column_expr(guard_columns, "updated_at", "NULL", alias="cpg")
    content_created = _column_expr(gc_columns, "created_at", "NULL", alias="gc")
    occurred_expr = f"COALESCE({guard_updated}, {guard_created}, {content_created})"
    rows = conn.execute(
        f"""SELECT
               cpg.content_id AS content_id,
               {_column_expr(gc_columns, "content_type", "'unknown'", alias="gc")} AS content_type,
               {_column_expr(guard_columns, "status", "'failed'", alias="cpg")} AS status,
               {_column_expr(guard_columns, "reasons", "NULL", alias="cpg")} AS reasons,
               {_column_expr(guard_columns, "metrics", "NULL", alias="cpg")} AS metrics,
               {occurred_expr} AS occurred_at
           FROM content_persona_guard cpg
           INNER JOIN generated_content gc ON gc.id = cpg.content_id
           WHERE {_column_expr(guard_columns, "checked", "1", alias="cpg")} = 1
             AND {_column_expr(guard_columns, "passed", "0", alias="cpg")} = 0
             AND datetime({occurred_expr}) >= datetime(?)
             AND datetime({occurred_expr}) <= datetime(?)
           ORDER BY datetime({occurred_expr}) ASC, cpg.content_id ASC""",
        (cutoff.isoformat(), window_end.isoformat()),
    ).fetchall()

    events: list[_FailureEvent] = []
    for row in rows:
        occurred_at = _parse_datetime(row["occurred_at"])
        if occurred_at is None:
            continue
        for reason in _reason_codes(row["reasons"], fallback=row["status"], metrics=row["metrics"]):
            events.append(
                _FailureEvent(
                    gate="persona_guard",
                    reason_code=reason,
                    content_type=_clean_label(row["content_type"], "unknown"),
                    content_id=int(row["content_id"]),
                    occurred_at=occurred_at,
                )
            )
    return events


def _load_claim_check_events(
    conn: sqlite3.Connection,
    *,
    gc_columns: set[str],
    claim_columns: set[str],
    cutoff: datetime,
    window_end: datetime,
) -> list[_FailureEvent]:
    claim_created = _column_expr(claim_columns, "created_at", "NULL", alias="ccc")
    claim_updated = _column_expr(claim_columns, "updated_at", "NULL", alias="ccc")
    content_created = _column_expr(gc_columns, "created_at", "NULL", alias="gc")
    occurred_expr = f"COALESCE({claim_updated}, {claim_created}, {content_created})"
    annotation_expr = _column_expr(claim_columns, "annotation_text", "NULL", alias="ccc")
    rows = conn.execute(
        f"""SELECT
               ccc.content_id AS content_id,
               {_column_expr(gc_columns, "content_type", "'unknown'", alias="gc")} AS content_type,
               {_column_expr(claim_columns, "unsupported_count", "0", alias="ccc")} AS unsupported_count,
               {annotation_expr} AS annotation_text,
               {occurred_expr} AS occurred_at
           FROM content_claim_checks ccc
           INNER JOIN generated_content gc ON gc.id = ccc.content_id
           WHERE {_column_expr(claim_columns, "unsupported_count", "0", alias="ccc")} > 0
             AND datetime({occurred_expr}) >= datetime(?)
             AND datetime({occurred_expr}) <= datetime(?)
           ORDER BY datetime({occurred_expr}) ASC, ccc.content_id ASC""",
        (cutoff.isoformat(), window_end.isoformat()),
    ).fetchall()

    events: list[_FailureEvent] = []
    for row in rows:
        occurred_at = _parse_datetime(row["occurred_at"])
        if occurred_at is None:
            continue
        reason = "unsupported_claims"
        if _clean_label(row["annotation_text"], ""):
            reason = "unsupported_claims"
        events.append(
            _FailureEvent(
                gate="claim_check",
                reason_code=reason,
                content_type=_clean_label(row["content_type"], "unknown"),
                content_id=int(row["content_id"]),
                occurred_at=occurred_at,
            )
        )
    return events


def _build_report(
    events: list[_FailureEvent],
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    min_count: int,
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> QualityGateFailureTrendsReport:
    grouped: dict[tuple[str, str, str, str], list[_FailureEvent]] = defaultdict(list)
    weekly_totals: Counter[str] = Counter()
    by_gate: Counter[str] = Counter()
    by_content_type: Counter[str] = Counter()
    affected_ids: set[int] = set()

    for event in events:
        week = _iso_week(event.occurred_at)
        grouped[(event.gate, event.reason_code, event.content_type, week)].append(event)
        weekly_totals[week] += 1
        by_gate[event.gate] += 1
        by_content_type[event.content_type] += 1
        affected_ids.add(event.content_id)

    rows = []
    for (gate, reason_code, content_type, week), group in grouped.items():
        if len(group) < min_count:
            continue
        ids = tuple(sorted({event.content_id for event in group})[:REPRESENTATIVE_ID_LIMIT])
        rows.append(
            QualityGateFailureTrendRow(
                gate=gate,
                reason_code=reason_code,
                content_type=content_type,
                week=week,
                failure_count=len(group),
                affected_content_ids=ids,
            )
        )
    rows.sort(
        key=lambda row: (
            row.week,
            -row.failure_count,
            row.gate,
            row.reason_code,
            row.content_type,
        )
    )

    return QualityGateFailureTrendsReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "failure_count": len(events),
            "affected_content_count": len(affected_ids),
            "by_gate": dict(sorted(by_gate.items())),
            "by_content_type": dict(sorted(by_content_type.items())),
        },
        rows=tuple(rows),
        weekly_totals=dict(sorted(weekly_totals.items())),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    expected = {
        "generated_content": {"id", "content_type"},
        "content_persona_guard": {"content_id", "checked", "passed", "status"},
        "content_claim_checks": {"content_id", "unsupported_count"},
    }
    missing_tables = tuple(table for table in expected if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in expected.items()
        if table in schema and columns - schema[table]
    }
    return missing_tables, missing_columns


def _missing_persona_join_columns(missing_columns: dict[str, tuple[str, ...]]) -> bool:
    return "content_id" in missing_columns.get("content_persona_guard", ())


def _missing_claim_join_columns(missing_columns: dict[str, tuple[str, ...]]) -> bool:
    return "content_id" in missing_columns.get("content_claim_checks", ())


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> QualityGateFailureTrendsReport:
    return QualityGateFailureTrendsReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "failure_count": 0,
            "affected_content_count": 0,
            "by_gate": {},
            "by_content_type": {},
        },
        rows=(),
        weekly_totals={},
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _reason_codes(value: Any, *, fallback: Any, metrics: Any) -> tuple[str, ...]:
    parsed = _parse_json(value)
    reasons: list[str] = []
    if isinstance(parsed, list):
        reasons.extend(_clean_label(item, "") for item in parsed)
    elif isinstance(parsed, dict):
        for key in ("code", "reason", "reason_code", "status"):
            if parsed.get(key):
                reasons.append(_clean_label(parsed[key], ""))
    elif parsed is not None:
        reasons.append(_clean_label(parsed, ""))

    metric_data = _parse_json(metrics)
    if isinstance(metric_data, dict):
        for key in ("code", "reason", "reason_code"):
            if metric_data.get(key):
                reasons.append(_clean_label(metric_data[key], ""))

    reasons = [reason for reason in reasons if reason]
    if not reasons:
        reasons = [_clean_label(fallback, "failed")]
    return tuple(dict.fromkeys(reasons))


def _parse_json(value: Any) -> Any:
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return str(value)


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


def _positive_int(value: int, name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be positive") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


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
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso_week(value: datetime) -> str:
    year, week, _weekday = value.isocalendar()
    return f"{year}-W{week:02d}"


def _clean_label(value: Any, default: str) -> str:
    text = str(value or "").strip().lower().replace(" ", "_")
    return text or default


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))


def _stable_totals(totals: dict[str, Any]) -> dict[str, Any]:
    result = dict(sorted(totals.items()))
    for key in ("by_gate", "by_content_type"):
        if isinstance(result.get(key), dict):
            result[key] = dict(sorted(result[key].items()))
    return result
