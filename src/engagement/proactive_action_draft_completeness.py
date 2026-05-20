"""Report proactive action draft completeness gaps."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Iterable


DEFAULT_STATUS = "all"
DEFAULT_ACTION_TYPE = "all"
DEFAULT_DISCOVERY_SOURCE = "all"

REQUIRED_COLUMNS = {
    "id",
    "action_type",
    "target_tweet_text",
    "target_author_handle",
    "discovery_source",
    "draft_text",
    "status",
    "posted_tweet_id",
    "created_at",
}

DRAFT_ACTION_TYPES = {"reply", "quote_tweet", "quote"}
DRAFT_STATUSES = {"pending", "approved"}

ISSUE_ORDER = {
    "missing_draft_text": 0,
    "missing_target_text": 1,
    "missing_target_author": 2,
    "posted_without_platform_id": 3,
}


def build_proactive_action_draft_completeness_report(
    action_rows: list[dict[str, Any]],
    *,
    status: str | Iterable[str] = DEFAULT_STATUS,
    action_type: str | Iterable[str] = DEFAULT_ACTION_TYPE,
    discovery_source: str | Iterable[str] = DEFAULT_DISCOVERY_SOURCE,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic draft completeness report from action rows."""
    generated_at = _utc(now or datetime.now(timezone.utc))
    status_filter = _normalize_filter(status)
    action_type_filter = _normalize_filter(action_type)
    discovery_source_filter = _normalize_filter(discovery_source)
    rows = [
        _normalize_row(row)
        for row in action_rows
        if _matches(_normal_status(row.get("status")), status_filter)
        and _matches(_normal_text(row.get("action_type")) or "unknown", action_type_filter)
        and _matches(_normal_text(row.get("discovery_source")) or "unknown", discovery_source_filter)
    ]
    findings = [
        finding
        for row in rows
        for finding in _findings_for_action(row)
    ]
    findings.sort(key=_finding_sort_key)
    issue_counts = Counter(finding["issue_type"] for finding in findings)
    return {
        "artifact_type": "proactive_action_draft_completeness",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "status": list(status_filter),
            "action_type": list(action_type_filter),
            "discovery_source": list(discovery_source_filter),
        },
        "totals": {
            "rows_scanned": len(action_rows),
            "actions_matched": len(rows),
            "finding_count": len(findings),
            "missing_draft_text": issue_counts.get("missing_draft_text", 0),
            "missing_target_text": issue_counts.get("missing_target_text", 0),
            "missing_target_author": issue_counts.get("missing_target_author", 0),
            "posted_without_platform_id": issue_counts.get("posted_without_platform_id", 0),
            "by_action_type": _counts(rows, "action_type"),
            "by_discovery_source": _counts(rows, "discovery_source"),
            "by_status": _counts(rows, "status"),
        },
        "groups": _groups(rows, findings),
        "findings": findings,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {
            table: sorted(columns)
            for table, columns in sorted((missing_columns or {}).items())
            if columns
        },
    }


def build_proactive_action_draft_completeness_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "proactive_actions" not in schema:
        return build_proactive_action_draft_completeness_report(
            [],
            missing_tables=["proactive_actions"],
            **kwargs,
        )
    missing = sorted(REQUIRED_COLUMNS - schema["proactive_actions"])
    if missing:
        return build_proactive_action_draft_completeness_report(
            [],
            missing_columns={"proactive_actions": missing},
            **kwargs,
        )
    return build_proactive_action_draft_completeness_report(_load_action_rows(conn), **kwargs)


def format_proactive_action_draft_completeness_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_proactive_action_draft_completeness_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Proactive Action Draft Completeness",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"status={','.join(report['filters']['status'])} "
            f"action_type={','.join(report['filters']['action_type'])} "
            f"discovery_source={','.join(report['filters']['discovery_source'])}"
        ),
        (
            "Totals: "
            f"rows_scanned={totals['rows_scanned']} "
            f"actions_matched={totals['actions_matched']} "
            f"findings={totals['finding_count']} "
            f"missing_draft_text={totals['missing_draft_text']} "
            f"missing_target_text={totals['missing_target_text']} "
            f"missing_target_author={totals['missing_target_author']} "
            f"posted_without_platform_id={totals['posted_without_platform_id']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append(
            "Missing columns: "
            + ", ".join(
                f"{table}.{column}"
                for table, columns in report["missing_columns"].items()
                for column in columns
            )
        )
    if not report["findings"]:
        lines.append("No proactive action draft completeness gaps found.")
        return "\n".join(lines)

    lines.append("Groups:")
    for group in report["groups"]:
        lines.append(
            f"  - status={group['status']} action_type={group['action_type']} "
            f"discovery_source={group['discovery_source']} count={group['action_count']} "
            f"findings={group['finding_count']}"
        )
    lines.append("Findings:")
    for finding in report["findings"]:
        lines.append(
            f"  - {finding['issue_type']} action_id={finding['action_id']} "
            f"status={finding['status']} action_type={finding['action_type']} "
            f"source={finding['discovery_source']} author={finding['target_author_handle'] or '-'}"
        )
    return "\n".join(lines)


def _load_action_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT id, action_type, target_tweet_text, target_author_handle,
                  discovery_source, draft_text, status, posted_tweet_id, created_at
           FROM proactive_actions
           ORDER BY datetime(COALESCE(created_at, '1970-01-01')) ASC, id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id"),
        "action_type": _normal_text(row.get("action_type")) or "unknown",
        "target_tweet_text": _normal_text(row.get("target_tweet_text")),
        "target_author_handle": _normal_text(row.get("target_author_handle")),
        "discovery_source": _normal_text(row.get("discovery_source")) or "unknown",
        "draft_text": _normal_text(row.get("draft_text")),
        "status": _normal_status(row.get("status")),
        "posted_tweet_id": _normal_text(row.get("posted_tweet_id")),
        "created_at": row.get("created_at"),
    }


def _findings_for_action(row: dict[str, Any]) -> list[dict[str, Any]]:
    base = {
        "action_id": row.get("id"),
        "status": row["status"],
        "action_type": row["action_type"],
        "discovery_source": row["discovery_source"],
        "target_author_handle": row["target_author_handle"],
        "created_at": row.get("created_at"),
    }
    findings: list[dict[str, Any]] = []
    if row["status"] in DRAFT_STATUSES and row["action_type"] in DRAFT_ACTION_TYPES and not row["draft_text"]:
        findings.append({**base, "issue_type": "missing_draft_text"})
    if not row["target_tweet_text"]:
        findings.append({**base, "issue_type": "missing_target_text"})
    if not row["target_author_handle"]:
        findings.append({**base, "issue_type": "missing_target_author"})
    if row["status"] == "posted" and not row["posted_tweet_id"]:
        findings.append({**base, "issue_type": "posted_without_platform_id"})
    return findings


def _groups(rows: list[dict[str, Any]], findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    finding_counts = Counter(
        (finding["status"], finding["action_type"], finding["discovery_source"])
        for finding in findings
    )
    row_counts = Counter((row["status"], row["action_type"], row["discovery_source"]) for row in rows)
    return [
        {
            "status": status,
            "action_type": action_type,
            "discovery_source": discovery_source,
            "action_count": count,
            "finding_count": finding_counts.get((status, action_type, discovery_source), 0),
        }
        for (status, action_type, discovery_source), count in sorted(row_counts.items())
    ]


def _counts(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    return dict(sorted(Counter(row[key] for row in rows).items()))


def _normalize_filter(value: str | Iterable[str]) -> tuple[str, ...]:
    if isinstance(value, str):
        parts = value.split(",")
    else:
        parts = [str(item) for item in value]
    normalized = tuple(sorted({_normal_text(part).lower() for part in parts if _normal_text(part)}))
    return normalized or ("all",)


def _matches(value: str, allowed: tuple[str, ...]) -> bool:
    return allowed == ("all",) or value in allowed


def _normal_status(value: Any) -> str:
    text = (_normal_text(value) or "pending").lower()
    if text in {"published", "sent", "completed"}:
        return "posted"
    if text in {"rejected", "expired"}:
        return "dismissed"
    return text


def _normal_text(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    return text.lower() if text else None


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    return (
        ISSUE_ORDER.get(str(finding["issue_type"]), 99),
        finding["status"],
        finding["action_type"],
        finding["discovery_source"],
        _int_or_text(finding["action_id"]),
    )


def _int_or_text(value: Any) -> int | str:
    try:
        return int(value)
    except (TypeError, ValueError):
        return str(value or "")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}
