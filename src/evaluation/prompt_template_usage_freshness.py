"""Audit generated content and prompt templates for stale template usage."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_STALE_DAYS = 30
DEFAULT_LIMIT = 50


def build_prompt_template_usage_freshness_report(
    usage_rows: list[dict[str, Any]],
    template_rows: list[dict[str, Any]],
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a report separating content-level usage gaps and stale templates."""
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    lookback_start = generated_at - timedelta(days=lookback_days)
    stale_before = generated_at - timedelta(days=stale_days)

    recent_usage = []
    for row in usage_rows:
        created_at = _parse_dt(_first(row, "generated_at", "created_at", "candidate_at")) or generated_at
        if not lookback_start <= created_at <= generated_at:
            continue
        recent_usage.append({**row, "created_at_dt": created_at})

    usage_findings = _usage_findings(recent_usage)
    last_usage = _last_usage_by_template(usage_rows)
    stale_templates = _stale_template_findings(template_rows, last_usage, stale_before)
    usage_findings.sort(key=lambda row: (row["content_id"] or 0, row["reason_codes"]))
    stale_templates.sort(key=lambda row: (row["last_used_at"] or "", row["prompt_template_id"] or row["prompt_template"]))
    usage_findings = usage_findings[:limit]
    stale_templates = stale_templates[:limit]

    usage_reason_counts = Counter(reason for row in usage_findings for reason in row["reason_codes"])
    return {
        "artifact_type": "prompt_template_usage_freshness",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "lookback_days": lookback_days,
            "stale_days": stale_days,
            "limit": limit,
            "lookback_start": lookback_start.isoformat(),
            "stale_before": stale_before.isoformat(),
        },
        "summary": {
            "recent_content_count": len(recent_usage),
            "content_usage_gap_count": len(usage_findings),
            "stale_template_count": len(stale_templates),
            "inactive_template_usage_count": usage_reason_counts.get("inactive_template", 0),
            "missing_prompt_template_id_count": usage_reason_counts.get("missing_prompt_template_id", 0),
            "missing_prompt_version_id_count": usage_reason_counts.get("missing_prompt_version_id", 0),
        },
        "content_usage_gaps": usage_findings,
        "stale_unused_templates": stale_templates,
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
    }


def build_prompt_template_usage_freshness_report_from_db(
    db_or_conn: Any,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    usage_rows = [] if gaps["missing_tables"] else _load_usage_rows(conn, schema)
    template_rows = _load_template_rows(conn, schema)
    return build_prompt_template_usage_freshness_report(
        usage_rows,
        template_rows,
        lookback_days=lookback_days,
        stale_days=stale_days,
        limit=limit,
        now=now,
        schema_gaps=gaps,
    )


def format_prompt_template_usage_freshness_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_prompt_template_usage_freshness_text(report: dict[str, Any]) -> str:
    lines = [
        "Prompt Template Usage Freshness",
        f"Generated: {report['generated_at']}",
        f"Lookback: {report['filters']['lookback_days']} days",
        f"Stale threshold: {report['filters']['stale_days']} days",
        (
            "Totals: "
            f"recent_content={report['summary']['recent_content_count']} "
            f"content_gaps={report['summary']['content_usage_gap_count']} "
            f"stale_templates={report['summary']['stale_template_count']}"
        ),
    ]
    if report["schema_gaps"].get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["schema_gaps"]["missing_tables"]))
    if report["content_usage_gaps"]:
        lines.extend(["", "Content usage gaps:"])
        for row in report["content_usage_gaps"]:
            lines.append(
                f"- content={row['content_id'] or '-'} template={row['prompt_template_id'] or row['prompt_template'] or '-'} "
                f"version={row['prompt_version_id'] or row['prompt_version'] or '-'} reasons={','.join(row['reason_codes'])}"
            )
    if report["stale_unused_templates"]:
        lines.extend(["", "Stale unused templates:"])
        for row in report["stale_unused_templates"]:
            lines.append(
                f"- template={row['prompt_template_id'] or row['prompt_template']} "
                f"version={row['prompt_version_id'] or row['prompt_version'] or '-'} last_used_at={row['last_used_at'] or '-'}"
            )
    if not report["content_usage_gaps"] and not report["stale_unused_templates"]:
        lines.append("No prompt template usage freshness issues found.")
    return "\n".join(lines)


def _usage_findings(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    findings = []
    for row in rows:
        reasons: list[str] = []
        if not _has_identifier(_first(row, "prompt_template_id", "prompt_template")):
            reasons.append("missing_prompt_template_id")
        if not _has_identifier(_first(row, "prompt_version_id", "prompt_version")):
            reasons.append("missing_prompt_version_id")
        if not _template_active(row):
            reasons.append("inactive_template")
        if not reasons:
            continue
        findings.append(
            {
                "content_id": _int_or_none(_first(row, "content_id", "id")),
                "content_type": _optional_text(_first(row, "content_type")),
                "prompt_template_id": _optional_text(_first(row, "prompt_template_id")),
                "prompt_template": _optional_text(_first(row, "prompt_template", "template_name", "prompt_type")),
                "prompt_version_id": _optional_text(_first(row, "prompt_version_id")),
                "prompt_version": _optional_text(_first(row, "prompt_version", "version")),
                "template_status": _optional_text(_first(row, "template_status", "status")),
                "template_active": _template_active(row),
                "generated_at": row["created_at_dt"].isoformat(),
                "reason_codes": reasons,
            }
        )
    return findings


def _stale_template_findings(
    template_rows: list[dict[str, Any]],
    last_usage: dict[tuple[str | None, str | None], datetime],
    stale_before: datetime,
) -> list[dict[str, Any]]:
    findings = []
    for row in template_rows:
        if not _template_active(row):
            continue
        key = (_optional_text(_first(row, "prompt_template_id", "id")), _optional_text(_first(row, "prompt_version_id", "version_id")))
        alt_key = (_optional_text(_first(row, "prompt_template", "template_name", "prompt_type")), _optional_text(_first(row, "prompt_version", "version")))
        used_at = last_usage.get(key) or last_usage.get(alt_key)
        created_at = _parse_dt(_first(row, "created_at", "updated_at"))
        reference_at = used_at or created_at
        if reference_at is not None and reference_at > stale_before:
            continue
        findings.append(
            {
                "prompt_template_id": key[0],
                "prompt_template": _optional_text(_first(row, "prompt_template", "template_name", "prompt_type")),
                "prompt_version_id": key[1],
                "prompt_version": _optional_text(_first(row, "prompt_version", "version")),
                "last_used_at": _iso(used_at),
                "created_at": _iso(created_at),
                "reason_codes": ["stale_unused_template"],
            }
        )
    return findings


def _last_usage_by_template(rows: list[dict[str, Any]]) -> dict[tuple[str | None, str | None], datetime]:
    indexed: dict[tuple[str | None, str | None], datetime] = {}
    for row in rows:
        used_at = _parse_dt(_first(row, "generated_at", "created_at", "candidate_at"))
        if used_at is None:
            continue
        keys = [
            (_optional_text(_first(row, "prompt_template_id")), _optional_text(_first(row, "prompt_version_id"))),
            (_optional_text(_first(row, "prompt_template_id")), None),
            (_optional_text(_first(row, "prompt_template", "template_name", "prompt_type")), _optional_text(_first(row, "prompt_version", "version"))),
            (_optional_text(_first(row, "prompt_template", "template_name", "prompt_type")), None),
        ]
        for key in keys:
            if not key[0]:
                continue
            current = indexed.get(key)
            if current is None or used_at > current:
                indexed[key] = used_at
    return indexed


def _load_usage_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    gc = schema["generated_content"]
    selected = [
        _expr(gc, "id", "NULL", "gc", alias="content_id"),
        _expr(gc, "content_type", "NULL", "gc", alias="content_type"),
        _expr(gc, "created_at", "NULL", "gc", alias="generated_at"),
        _expr(gc, "prompt_template_id", "NULL", "gc", alias="prompt_template_id"),
        _expr(gc, "prompt_template", "NULL", "gc", alias="prompt_template"),
        _expr(gc, "prompt_version_id", "NULL", "gc", alias="prompt_version_id"),
        _expr(gc, "prompt_version", "NULL", "gc", alias="prompt_version"),
    ]
    joins = ""
    if "engagement_predictions" in schema and {"content_id", "prompt_type"}.issubset(schema["engagement_predictions"]):
        ep = schema["engagement_predictions"]
        joins += " LEFT JOIN engagement_predictions ep ON ep.content_id = gc.id"
        selected[4] = f"COALESCE({_raw_expr(gc, 'prompt_template', 'NULL', 'gc')}, ep.prompt_type) AS prompt_template"
        selected[6] = f"COALESCE({_raw_expr(gc, 'prompt_version', 'NULL', 'gc')}, {_raw_expr(ep, 'prompt_version', 'NULL', 'ep')}) AS prompt_version"
    can_join_templates = "prompt_templates" in schema and "id" in schema["prompt_templates"] and "prompt_template_id" in gc
    if can_join_templates:
        pt = schema["prompt_templates"]
        joins += " LEFT JOIN prompt_templates pt ON pt.id = gc.prompt_template_id"
        selected.extend(
            [
                _expr(pt, "active", "1", "pt", alias="template_active"),
                _expr(pt, "status", "'active'", "pt", alias="template_status"),
                f"COALESCE({_raw_expr(gc, 'prompt_template', 'NULL', 'gc')}, {_raw_expr(pt, 'name', 'NULL', 'pt')}, {_raw_expr(pt, 'prompt_type', 'NULL', 'pt')}) AS template_name",
            ]
        )
    else:
        selected.extend(["1 AS template_active", "'active' AS template_status", "NULL AS template_name"])
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM generated_content gc{joins} ORDER BY gc.id ASC").fetchall()]


def _load_template_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    rows = []
    if "prompt_templates" in schema:
        pt = schema["prompt_templates"]
        selected = [
            _expr(pt, "id", "NULL", "pt", alias="prompt_template_id"),
            _expr(pt, "name", "NULL", "pt", alias="prompt_template"),
            _expr(pt, "prompt_type", "NULL", "pt", alias="prompt_type"),
            _expr(pt, "active", "1", "pt", alias="template_active"),
            _expr(pt, "status", "'active'", "pt", alias="template_status"),
            _expr(pt, "created_at", "NULL", "pt", alias="created_at"),
            "NULL AS prompt_version_id",
            "NULL AS prompt_version",
        ]
        rows.extend(dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM prompt_templates pt").fetchall())
    if "prompt_versions" in schema:
        pv = schema["prompt_versions"]
        selected = [
            _expr(pv, "prompt_template_id", "NULL", "pv", alias="prompt_template_id"),
            _expr(pv, "template_id", "NULL", "pv", alias="template_id"),
            _expr(pv, "prompt_type", "NULL", "pv", alias="prompt_template"),
            _expr(pv, "id", "NULL", "pv", alias="prompt_version_id"),
            _expr(pv, "version", "NULL", "pv", alias="prompt_version"),
            _expr(pv, "active", "1", "pv", alias="template_active"),
            _expr(pv, "status", "'active'", "pv", alias="template_status"),
            _expr(pv, "created_at", "NULL", "pv", alias="created_at"),
        ]
        version_rows = [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM prompt_versions pv").fetchall()]
        for row in version_rows:
            if row.get("prompt_template_id") is None:
                row["prompt_template_id"] = row.get("template_id")
            rows.append(row)
    return rows


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "generated_content" not in schema:
        return {"missing_tables": ["generated_content"], "missing_columns": {}}
    missing = sorted({"id"} - schema["generated_content"])
    return {"missing_tables": [], "missing_columns": {"generated_content": missing} if missing else {}}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _expr(columns: set[str], column: str, default: str, table_alias: str, *, alias: str | None = None) -> str:
    return f"{_raw_expr(columns, column, default, table_alias)} AS {alias or column}"


def _raw_expr(columns: set[str], column: str, default: str, table_alias: str) -> str:
    return f"{table_alias}.{column}" if column in columns else default


def _template_active(row: dict[str, Any]) -> bool:
    status = _text(_first(row, "template_status", "status")).lower()
    active = _first(row, "template_active", "active")
    if status in {"inactive", "paused", "rejected", "archived", "disabled"}:
        return False
    if active is None:
        return True
    return _text(active).lower() not in {"0", "false", "no", "inactive"}


def _has_identifier(value: Any) -> bool:
    return bool(_text(value))


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _int_or_none(value: Any) -> int | None:
    try:
        return None if value in (None, "") else int(value)
    except (TypeError, ValueError):
        return None


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
