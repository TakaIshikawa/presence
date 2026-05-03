"""Audit model_usage accounting rows for incomplete or inconsistent attribution."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Iterable


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
ISSUE_ZERO_COST_WITH_TOKENS = "zero_cost_with_tokens"
ISSUE_TOKEN_TOTAL_MISMATCH = "token_total_mismatch"
ISSUE_BLANK_MODEL_NAME = "blank_model_name"
ISSUE_BLANK_OPERATION_NAME = "blank_operation_name"
ISSUE_MISSING_GENERATED_CONTENT = "missing_generated_content"
ISSUE_MISSING_PIPELINE_RUN = "missing_pipeline_run"
ISSUE_TYPES = (
    ISSUE_ZERO_COST_WITH_TOKENS,
    ISSUE_TOKEN_TOTAL_MISMATCH,
    ISSUE_BLANK_MODEL_NAME,
    ISSUE_BLANK_OPERATION_NAME,
    ISSUE_MISSING_GENERATED_CONTENT,
    ISSUE_MISSING_PIPELINE_RUN,
)


@dataclass(frozen=True)
class ModelUsageAccountingFinding:
    """One accounting issue on a model_usage row."""

    issue_type: str
    usage_id: int | None
    created_at: str | None
    operation_name: str | None
    model_name: str | None
    input_tokens: int
    output_tokens: int
    total_tokens: int
    estimated_cost: float
    content_id: int | None = None
    pipeline_run_id: int | None = None
    expected_total_tokens: int | None = None
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ModelUsageAccountingAuditReport:
    """Model usage accounting audit report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    findings: tuple[ModelUsageAccountingFinding, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def has_issues(self) -> bool:
        return bool(self.findings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "model_usage_accounting_audit",
            "findings": [finding.to_dict() for finding in self.findings],
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "has_issues": self.has_issues,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": _sorted_totals(self.totals),
        }


def build_model_usage_accounting_audit_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    operation: str | None = None,
    model: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ModelUsageAccountingAuditReport:
    """Load recent model_usage rows and report accounting inconsistencies."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "limit": limit,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": cutoff.isoformat(),
        "model": model,
        "operation": operation,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = () if "model_usage" in schema else ("model_usage",)
    missing_columns = _missing_columns(schema)
    rows = (
        _load_model_usage_rows(
            conn,
            schema,
            cutoff=cutoff,
            operation=operation,
            model=model,
        )
        if not missing_tables
        else []
    )
    findings = _audit_rows(rows, schema)[:limit]
    return ModelUsageAccountingAuditReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(rows, findings),
        findings=tuple(findings),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_model_usage_accounting_audit_json(
    report: ModelUsageAccountingAuditReport,
) -> str:
    """Serialize a model usage accounting audit as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_model_usage_accounting_audit_text(
    report: ModelUsageAccountingAuditReport,
) -> str:
    """Render a concise human-readable model usage accounting audit."""
    totals = report.totals
    filters = report.filters
    by_type = totals["by_issue_type"]
    lines = [
        "Model Usage Accounting Audit",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={filters['days']} limit={filters['limit']} "
            f"operation={filters.get('operation') or '*'} "
            f"model={filters.get('model') or '*'} "
            f"lookback_start={filters['lookback_start']}"
        ),
        (
            "Totals: "
            f"rows_scanned={totals['rows_scanned']} "
            f"rows_with_issues={totals['rows_with_issues']} "
            f"issue_count={totals['issue_count']} "
            + " ".join(f"{issue_type}={by_type.get(issue_type, 0)}" for issue_type in ISSUE_TYPES)
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
            if columns
        )
        if missing:
            lines.append("Missing columns: " + missing)

    if not report.findings:
        lines.extend(["", "No model usage accounting issues found."])
        return "\n".join(lines)

    lines.extend(["", "Findings:"])
    for finding in report.findings:
        lines.append(
            f"- type={finding.issue_type} usage_id={finding.usage_id or '-'} "
            f"operation={finding.operation_name or '-'} model={finding.model_name or '-'} "
            f"tokens={finding.input_tokens}+{finding.output_tokens}/{finding.total_tokens} "
            f"cost={finding.estimated_cost:g} content_id={finding.content_id or '-'} "
            f"pipeline_run_id={finding.pipeline_run_id or '-'} created_at={finding.created_at or '-'}"
        )
        lines.append(f"  reason={finding.reason}")
    return "\n".join(lines)


def _load_model_usage_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    operation: str | None,
    model: str | None,
) -> list[dict[str, Any]]:
    columns = schema["model_usage"]
    select_columns = [
        _column_expr(columns, "id", "mu", "id", default="NULL"),
        _column_expr(columns, "created_at", "mu", "created_at", default="NULL"),
        _column_expr(columns, "operation_name", "mu", "operation_name", default="NULL"),
        _column_expr(columns, "model_name", "mu", "model_name", default="NULL"),
        _column_expr(columns, "input_tokens", "mu", "input_tokens", default="0"),
        _column_expr(columns, "output_tokens", "mu", "output_tokens", default="0"),
        _column_expr(columns, "total_tokens", "mu", "total_tokens", default="0"),
        _column_expr(columns, "estimated_cost", "mu", "estimated_cost", default="0"),
        _column_expr(columns, "content_id", "mu", "content_id", default="NULL"),
        _column_expr(columns, "pipeline_run_id", "mu", "pipeline_run_id", default="NULL"),
        "NULL AS generated_content_id",
        "NULL AS pipeline_runs_id",
    ]
    joins: list[str] = []
    if "content_id" in columns and "generated_content" in schema and "id" in schema["generated_content"]:
        joins.append("LEFT JOIN generated_content gc ON gc.id = mu.content_id")
        select_columns[-2] = "gc.id AS generated_content_id"
    if "pipeline_run_id" in columns and "pipeline_runs" in schema and "id" in schema["pipeline_runs"]:
        joins.append("LEFT JOIN pipeline_runs pr ON pr.id = mu.pipeline_run_id")
        select_columns[-1] = "pr.id AS pipeline_runs_id"

    where: list[str] = []
    params: list[Any] = []
    if "created_at" in columns:
        where.append("datetime(mu.created_at) >= datetime(?)")
        params.append(_sqlite_ts(cutoff))
    if operation is not None:
        where.append("mu.operation_name = ?")
        params.append(operation)
    if model is not None:
        where.append("mu.model_name = ?")
        params.append(model)
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    order_clause = "datetime(mu.created_at) DESC" if "created_at" in columns else "mu.rowid DESC"
    if "id" in columns:
        order_clause += ", mu.id DESC"

    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM model_usage mu
            {' '.join(joins)}
            {where_clause}
            ORDER BY {order_clause}""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _audit_rows(
    rows: Iterable[dict[str, Any]],
    schema: dict[str, set[str]],
) -> list[ModelUsageAccountingFinding]:
    columns = schema.get("model_usage", set())
    findings: list[ModelUsageAccountingFinding] = []
    for row in rows:
        input_tokens = _int_value(row.get("input_tokens"))
        output_tokens = _int_value(row.get("output_tokens"))
        total_tokens = _int_value(row.get("total_tokens"))
        estimated_cost = _float_value(row.get("estimated_cost"))
        expected_total = input_tokens + output_tokens
        model_name = _clean_text(row.get("model_name"))
        operation_name = _clean_text(row.get("operation_name"))
        content_id = _int_or_none(row.get("content_id"))
        pipeline_run_id = _int_or_none(row.get("pipeline_run_id"))

        if total_tokens > 0 and estimated_cost == 0:
            findings.append(
                _finding(
                    ISSUE_ZERO_COST_WITH_TOKENS,
                    row,
                    input_tokens,
                    output_tokens,
                    total_tokens,
                    estimated_cost,
                    model_name,
                    operation_name,
                    expected_total,
                    "total_tokens is positive while estimated_cost is zero",
                )
            )
        if {"input_tokens", "output_tokens", "total_tokens"}.issubset(columns) and expected_total != total_tokens:
            findings.append(
                _finding(
                    ISSUE_TOKEN_TOTAL_MISMATCH,
                    row,
                    input_tokens,
                    output_tokens,
                    total_tokens,
                    estimated_cost,
                    model_name,
                    operation_name,
                    expected_total,
                    "input_tokens plus output_tokens does not equal total_tokens",
                )
            )
        if "model_name" in columns and model_name is None:
            findings.append(
                _finding(
                    ISSUE_BLANK_MODEL_NAME,
                    row,
                    input_tokens,
                    output_tokens,
                    total_tokens,
                    estimated_cost,
                    model_name,
                    operation_name,
                    expected_total,
                    "model_name is blank",
                )
            )
        if "operation_name" in columns and operation_name is None:
            findings.append(
                _finding(
                    ISSUE_BLANK_OPERATION_NAME,
                    row,
                    input_tokens,
                    output_tokens,
                    total_tokens,
                    estimated_cost,
                    model_name,
                    operation_name,
                    expected_total,
                    "operation_name is blank",
                )
            )
        if (
            "content_id" in columns
            and "generated_content" in schema
            and "id" in schema["generated_content"]
            and content_id is not None
            and row.get("generated_content_id") is None
        ):
            findings.append(
                _finding(
                    ISSUE_MISSING_GENERATED_CONTENT,
                    row,
                    input_tokens,
                    output_tokens,
                    total_tokens,
                    estimated_cost,
                    model_name,
                    operation_name,
                    expected_total,
                    "content_id does not resolve to generated_content",
                )
            )
        if (
            "pipeline_run_id" in columns
            and "pipeline_runs" in schema
            and "id" in schema["pipeline_runs"]
            and pipeline_run_id is not None
            and row.get("pipeline_runs_id") is None
        ):
            findings.append(
                _finding(
                    ISSUE_MISSING_PIPELINE_RUN,
                    row,
                    input_tokens,
                    output_tokens,
                    total_tokens,
                    estimated_cost,
                    model_name,
                    operation_name,
                    expected_total,
                    "pipeline_run_id does not resolve to pipeline_runs",
                )
            )
    findings.sort(key=_finding_sort_key)
    return findings


def _finding(
    issue_type: str,
    row: dict[str, Any],
    input_tokens: int,
    output_tokens: int,
    total_tokens: int,
    estimated_cost: float,
    model_name: str | None,
    operation_name: str | None,
    expected_total_tokens: int,
    reason: str,
) -> ModelUsageAccountingFinding:
    return ModelUsageAccountingFinding(
        issue_type=issue_type,
        usage_id=_int_or_none(row.get("id")),
        created_at=str(row["created_at"]) if row.get("created_at") is not None else None,
        operation_name=operation_name,
        model_name=model_name,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=total_tokens,
        estimated_cost=estimated_cost,
        content_id=_int_or_none(row.get("content_id")),
        pipeline_run_id=_int_or_none(row.get("pipeline_run_id")),
        expected_total_tokens=expected_total_tokens,
        reason=reason,
    )


def _totals(
    rows: list[dict[str, Any]],
    findings: Iterable[ModelUsageAccountingFinding],
) -> dict[str, Any]:
    finding_list = list(findings)
    rows_with_issues = {finding.usage_id for finding in finding_list}
    return {
        "by_issue_type": dict(Counter(finding.issue_type for finding in finding_list)),
        "by_model": dict(Counter(finding.model_name or "(blank)" for finding in finding_list)),
        "by_operation": dict(Counter(finding.operation_name or "(blank)" for finding in finding_list)),
        "issue_count": len(finding_list),
        "rows_scanned": len(rows),
        "rows_with_issues": len(rows_with_issues),
    }


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    required = {
        "model_usage": {
            "estimated_cost",
            "input_tokens",
            "model_name",
            "operation_name",
            "output_tokens",
            "total_tokens",
        },
    }
    return {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    schema: dict[str, set[str]] = {}
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[str(table)] = {info[1] for info in conn.execute(f"PRAGMA table_info({table})")}
    return schema


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _column_expr(
    columns: set[str],
    column: str,
    alias: str,
    output: str,
    *,
    default: str,
) -> str:
    if column in columns:
        return f"{alias}.{column} AS {output}"
    return f"{default} AS {output}"


def _sorted_totals(totals: dict[str, Any]) -> dict[str, Any]:
    sorted_totals: dict[str, Any] = {}
    for key, value in sorted(totals.items()):
        if isinstance(value, dict):
            sorted_totals[key] = {subkey: value[subkey] for subkey in sorted(value)}
        else:
            sorted_totals[key] = value
    return sorted_totals


def _finding_sort_key(finding: ModelUsageAccountingFinding) -> tuple[str, str, int, str]:
    return (
        finding.created_at or "",
        finding.operation_name or "",
        finding.usage_id or 0,
        finding.issue_type,
    )


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _sqlite_ts(value: datetime) -> str:
    return _ensure_utc(value).strftime("%Y-%m-%d %H:%M:%S")


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_value(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_value(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
