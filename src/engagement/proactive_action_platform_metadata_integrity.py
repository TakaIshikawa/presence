"""Audit proactive action platform metadata integrity."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Iterable


DEFAULT_ACTION_TYPE = "all"
DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
DEFAULT_STATUS = "all"
REQUIRED_COLUMNS = {"id", "action_type", "status", "platform_metadata"}
OPTIONAL_COLUMNS = ("target_tweet_id", "posted_tweet_id", "posted_platform_id", "created_at", "target_url", "posted_at")
REQUIRES_TARGET_METADATA = {"approved", "posted"}
STALE_TARGET_STATUSES = {"pending", "approved"}
TARGET_ID_KEYS = ("target_tweet_id", "target_id", "tweet_id", "post_id", "platform_target_id")
URL_KEYS = ("url", "target_url", "post_url", "platform_url")
CID_KEYS = ("cid", "target_cid", "content_id")
POSTED_KEYS = ("posted_tweet_id", "posted_platform_id", "platform_post_id", "posted_id", "post_id")
UNAVAILABLE_KEYS = ("deleted", "is_deleted", "unavailable", "is_unavailable", "target_deleted", "target_unavailable")
UNAVAILABLE_STATUSES = {"deleted", "unavailable", "not_found", "gone", "removed"}


def build_proactive_action_platform_metadata_integrity_report_from_db(
    db_or_conn: Any,
    *,
    status: str | Iterable[str] = DEFAULT_STATUS,
    action_type: str | Iterable[str] = DEFAULT_ACTION_TYPE,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a deterministic proactive action platform metadata report."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    status_filter = _normalize_filter(status)
    action_type_filter = _normalize_filter(action_type)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    columns = schema.get("proactive_actions")
    if columns is None:
        return _report(generated_at, status_filter, action_type_filter, days, limit, [], 0, ["proactive_actions"], {})
    missing = sorted(REQUIRED_COLUMNS - columns)
    if missing:
        return _report(generated_at, status_filter, action_type_filter, days, limit, [], 0, [], {"proactive_actions": missing})

    rows = _load_rows(conn, columns, generated_at - timedelta(days=days))
    matched = [
        _normalize_row(row)
        for row in rows
        if _matches(_normal_status(row.get("status")), status_filter)
        and _matches(_normal_text(row.get("action_type")) or "unknown", action_type_filter)
    ]
    findings = [finding for row in matched for finding in _findings(row)]
    findings.sort(key=_finding_sort_key)
    return _report(generated_at, status_filter, action_type_filter, days, limit, findings, len(matched), [], {})


def format_proactive_action_platform_metadata_integrity_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_proactive_action_platform_metadata_integrity_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    summary = report["summary"]
    lines = [
        "Proactive Action Platform Metadata Integrity",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"status={','.join(report['filters']['status'])} "
            f"action_type={','.join(report['filters']['action_type'])} "
            f"days={report['filters']['days']} limit={report['filters']['limit']}"
        ),
        f"Totals: actions={summary['action_count']} findings={summary['finding_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "action_id | status | action_type | gap_type | detail"])
    for finding in report["findings"]:
        lines.append(
            f"{finding['action_id']} | {finding['status']} | {finding['action_type']} | "
            f"{finding['gap_type']} | {finding.get('detail') or '-'}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str], cutoff: datetime) -> list[dict[str, Any]]:
    selected = ["id AS action_id", "action_type", "status", "platform_metadata"]
    for column in OPTIONAL_COLUMNS:
        selected.append(f"{column}" if column in columns else f"NULL AS {column}")
    where = "WHERE datetime(created_at) >= datetime(?)" if "created_at" in columns else ""
    params = (cutoff.isoformat(),) if where else ()
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM proactive_actions
            {where}
            ORDER BY datetime(COALESCE(created_at, '1970-01-01')) ASC, id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "status": _normal_status(row.get("status")),
        "action_type": _normal_text(row.get("action_type")) or "unknown",
    }


def _findings(row: dict[str, Any]) -> list[dict[str, Any]]:
    metadata, error = _metadata_object(row.get("platform_metadata"))
    if error:
        return [_finding(row, "malformed_platform_metadata", detail=error)]

    findings: list[dict[str, Any]] = []
    if row["status"] in REQUIRES_TARGET_METADATA:
        if not _metadata_value(metadata, ("platform", "network", "service")):
            findings.append(_finding(row, "missing_platform"))
        if not (_metadata_value(metadata, URL_KEYS) or _clean(row.get("target_url"))):
            findings.append(_finding(row, "missing_url"))
        if not _metadata_value(metadata, CID_KEYS):
            findings.append(_finding(row, "missing_cid"))

    row_target_id = _clean(row.get("target_tweet_id"))
    metadata_target_id = _metadata_value(metadata, TARGET_ID_KEYS)
    if row_target_id and metadata_target_id and row_target_id != metadata_target_id:
        findings.append(_finding(row, "target_tweet_id_mismatch", row_target_tweet_id=row_target_id, metadata_target_tweet_id=metadata_target_id))

    if row["status"] == "posted" and not (_clean(row.get("posted_tweet_id")) or _clean(row.get("posted_platform_id")) or _metadata_value(metadata, POSTED_KEYS)):
        findings.append(_finding(row, "posted_missing_identifier"))

    if row["status"] in STALE_TARGET_STATUSES and _metadata_target_unavailable(metadata):
        findings.append(_finding(row, "stale_unavailable_target", detail="metadata marks target deleted or unavailable"))
    return findings


def _metadata_object(raw: Any) -> tuple[dict[str, Any], str | None]:
    if raw is None or _clean(raw) == "":
        return {}, None
    if isinstance(raw, dict):
        return raw, None
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError) as exc:
        return {}, f"platform_metadata is not valid JSON: {exc}"
    if not isinstance(parsed, dict):
        return {}, "platform_metadata must be a JSON object"
    return parsed, None


def _metadata_value(metadata: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = metadata.get(key)
        if isinstance(value, dict):
            nested = _metadata_value(value, keys)
            if nested:
                return nested
        elif _clean(value):
            return _clean(value)
    return ""


def _metadata_target_unavailable(metadata: dict[str, Any]) -> bool:
    for key in UNAVAILABLE_KEYS:
        value = metadata.get(key)
        if isinstance(value, bool) and value:
            return True
        if _clean(value).lower() in {"1", "true", "yes"}:
            return True
    for key in ("target_status", "availability", "status"):
        if _clean(metadata.get(key)).lower() in UNAVAILABLE_STATUSES:
            return True
    return False


def _finding(row: dict[str, Any], gap_type: str, **extra: Any) -> dict[str, Any]:
    return {
        "action_id": row.get("action_id"),
        "status": row.get("status"),
        "action_type": row.get("action_type"),
        "target_tweet_id": row.get("target_tweet_id"),
        "posted_tweet_id": row.get("posted_tweet_id"),
        "created_at": row.get("created_at"),
        "gap_type": gap_type,
        **extra,
    }


def _report(
    generated_at: datetime,
    status_filter: tuple[str, ...],
    action_type_filter: tuple[str, ...],
    days: int,
    limit: int,
    findings: list[dict[str, Any]],
    action_count: int,
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    shown = findings[:limit]
    counts = Counter(finding["gap_type"] for finding in findings)
    return {
        "artifact_type": "proactive_action_platform_metadata_integrity",
        "generated_at": generated_at.isoformat(),
        "filters": {"status": list(status_filter), "action_type": list(action_type_filter), "days": days, "limit": limit},
        "summary": {
            "action_count": action_count,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_gap_type": dict(sorted(counts.items())),
        },
        "groups": [{"gap_type": gap_type, "finding_count": count} for gap_type, count in sorted(counts.items())],
        "findings": shown,
        "missing_tables": sorted(missing_tables),
        "missing_columns": {table: sorted(columns) for table, columns in sorted(missing_columns.items()) if columns},
        "empty_state": {
            "is_empty": not findings,
            "message": "No proactive action platform metadata integrity gaps found." if not findings else None,
        },
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _normalize_filter(value: str | Iterable[str]) -> tuple[str, ...]:
    parts = value.split(",") if isinstance(value, str) else [str(item) for item in value]
    normalized = tuple(sorted({_normal_text(part) for part in parts if _normal_text(part)}))
    return normalized or ("all",)


def _matches(value: str, allowed: tuple[str, ...]) -> bool:
    return allowed == ("all",) or value in allowed


def _normal_status(value: Any) -> str:
    text = _normal_text(value) or "pending"
    return "posted" if text in {"published", "sent", "completed"} else text


def _normal_text(value: Any) -> str:
    return _clean(value).lower()


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _finding_sort_key(finding: dict[str, Any]) -> tuple[int, tuple[int, Any]]:
    return (_gap_rank(finding["gap_type"]), _int_or_text(finding.get("action_id")))


def _gap_rank(gap_type: str) -> int:
    return {
        "malformed_platform_metadata": 0,
        "missing_platform": 1,
        "missing_url": 2,
        "missing_cid": 3,
        "target_tweet_id_mismatch": 4,
        "posted_missing_identifier": 5,
        "stale_unavailable_target": 6,
    }.get(gap_type, 99)


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_text(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, "" if value is None else str(value))


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
