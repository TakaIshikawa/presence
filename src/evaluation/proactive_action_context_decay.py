"""Report pending proactive actions with stale or missing context."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Iterable


DEFAULT_MAX_CONTEXT_AGE_DAYS = 14
DEFAULT_LIMIT = 100
TIMESTAMP_KEYS = ("fetched_at", "enriched_at", "updated_at", "observed_at")
CONTEXT_REQUIRED_ACTION_TYPES = {"reply", "quote_tweet"}


def build_proactive_action_context_decay_report(
    rows: list[dict[str, Any]],
    *,
    max_context_age_days: int = DEFAULT_MAX_CONTEXT_AGE_DAYS,
    status: str | Iterable[str] = "pending",
    action_type: str | Iterable[str] = "all",
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
) -> dict[str, Any]:
    if max_context_age_days < 0:
        raise ValueError("max_context_age_days must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    statuses = _normalize_filter(status)
    action_types = _normalize_filter(action_type)
    findings: list[dict[str, Any]] = []
    for row in rows:
        row_status = _clean(row.get("status"), "unknown").lower()
        row_action_type = _clean(row.get("action_type"), "unknown").lower()
        if statuses and "all" not in statuses and row_status not in statuses:
            continue
        if action_types and "all" not in action_types and row_action_type not in action_types:
            continue
        reference = _parse_dt(row.get("reviewed_at")) or _parse_dt(row.get("created_at")) or generated_at
        context_state = _json_state(row.get("relationship_context"))
        metadata_state = _json_state(row.get("platform_metadata"))
        context_ts = _latest_timestamp(context_state.get("value"))
        metadata_ts = _latest_timestamp(metadata_state.get("value"))
        context_age = _age_days(context_ts, reference)
        metadata_age = _age_days(metadata_ts, reference)
        missing_context = row_action_type in CONTEXT_REQUIRED_ACTION_TYPES and not context_state.get("has_value")
        invalid_json = bool(context_state.get("invalid") or metadata_state.get("invalid"))
        stale_context = context_age is not None and context_age > max_context_age_days
        stale_metadata = metadata_age is not None and metadata_age > max_context_age_days
        missing_timestamp = (
            (context_state.get("has_value") and not context_state.get("invalid") and context_ts is None)
            or (metadata_state.get("has_value") and not metadata_state.get("invalid") and metadata_ts is None)
        )
        if not (missing_context or invalid_json or stale_context or stale_metadata or missing_timestamp):
            continue
        findings.append(
            {
                "proactive_action_id": _int_or_none(row.get("proactive_action_id") or row.get("id")),
                "action_type": row_action_type,
                "target_author_handle": _clean(row.get("target_author_handle")) or None,
                "status": row_status,
                "context_age_days": context_age,
                "metadata_age_days": metadata_age,
                "missing_context": missing_context,
                "severity": _severity(missing_context, invalid_json, stale_context, stale_metadata, missing_timestamp),
                "issue_types": _issue_types(missing_context, invalid_json, stale_context, stale_metadata, missing_timestamp),
            }
        )
    findings.sort(key=lambda item: (_severity_rank(item["severity"]), item["proactive_action_id"] or 0))
    shown = findings[:limit]
    return {
        "artifact_type": "proactive_action_context_decay",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "max_context_age_days": max_context_age_days,
            "status": list(statuses),
            "action_type": list(action_types),
            "limit": limit,
        },
        "summary": {"rows_scanned": len(rows), "decayed_count": len(findings), "shown_count": len(shown)},
        "decayed_actions": shown,
        "missing_tables": sorted(missing_tables or []),
    }


def build_proactive_action_context_decay_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing = [] if "proactive_actions" in schema else ["proactive_actions"]
    rows = _load_rows(conn, schema["proactive_actions"]) if not missing else []
    return build_proactive_action_context_decay_report(rows, missing_tables=missing, **kwargs)


def format_proactive_action_context_decay_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_proactive_action_context_decay_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Proactive Action Context Decay",
        f"Generated: {report['generated_at']}",
        f"Max context age days: {report['filters']['max_context_age_days']}",
        f"Totals: scanned={summary['rows_scanned']} decayed={summary['decayed_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["decayed_actions"]:
        lines.append("No proactive action context decay found.")
        return "\n".join(lines)
    lines.extend(["", "proactive_action_id | action_type | target_author_handle | status | context_age_days | metadata_age_days | missing_context | severity"])
    for item in report["decayed_actions"]:
        lines.append(
            f"{item['proactive_action_id'] or '-'} | {item['action_type']} | {item['target_author_handle'] or '-'} | "
            f"{item['status']} | {item['context_age_days'] if item['context_age_days'] is not None else '-'} | "
            f"{item['metadata_age_days'] if item['metadata_age_days'] is not None else '-'} | {item['missing_context']} | {item['severity']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    wanted = [
        "id", "status", "action_type", "target_author_handle", "relationship_context",
        "platform_metadata", "created_at", "reviewed_at",
    ]
    select = [_expr(columns, col, "proactive_action_id" if col == "id" else col, "NULL") for col in wanted]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM proactive_actions ORDER BY rowid ASC")]


def _json_state(value: Any) -> dict[str, Any]:
    if value in (None, ""):
        return {"has_value": False, "invalid": False, "value": None}
    if isinstance(value, (dict, list)):
        return {"has_value": True, "invalid": False, "value": value}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {"has_value": True, "invalid": True, "value": None}
    return {"has_value": True, "invalid": False, "value": parsed}


def _latest_timestamp(value: Any) -> datetime | None:
    timestamps: list[datetime] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key in TIMESTAMP_KEYS:
                parsed = _parse_dt(item)
                if parsed:
                    timestamps.append(parsed)
            if isinstance(item, (dict, list)):
                nested = _latest_timestamp(item)
                if nested:
                    timestamps.append(nested)
    elif isinstance(value, list):
        for item in value:
            nested = _latest_timestamp(item)
            if nested:
                timestamps.append(nested)
    return max(timestamps) if timestamps else None


def _age_days(timestamp: datetime | None, reference: datetime) -> int | None:
    if timestamp is None:
        return None
    return max(0, int((reference - timestamp).total_seconds() // 86400))


def _severity(missing_context: bool, invalid_json: bool, stale_context: bool, stale_metadata: bool, missing_timestamp: bool) -> str:
    if missing_context or invalid_json:
        return "critical"
    if stale_context and stale_metadata:
        return "high"
    if stale_context or stale_metadata:
        return "medium"
    if missing_timestamp:
        return "low"
    return "info"


def _issue_types(missing_context: bool, invalid_json: bool, stale_context: bool, stale_metadata: bool, missing_timestamp: bool) -> list[str]:
    issues = []
    if missing_context:
        issues.append("missing_relationship_context")
    if invalid_json:
        issues.append("invalid_json")
    if stale_context:
        issues.append("stale_relationship_context")
    if stale_metadata:
        issues.append("stale_platform_metadata")
    if missing_timestamp:
        issues.append("missing_context_timestamp")
    return issues


def _severity_rank(value: str) -> int:
    return {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}.get(value, 5)


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{column} AS {output}" if column in columns else f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _normalize_filter(value: str | Iterable[str]) -> tuple[str, ...]:
    values = value.split(",") if isinstance(value, str) else list(value)
    normalized = tuple(item for item in (_clean(part).lower() for part in values) if item)
    return normalized or ("all",)


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    for candidate in (text, text.replace(" ", "T", 1)):
        try:
            return _utc(datetime.fromisoformat(candidate))
        except ValueError:
            continue
    return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
