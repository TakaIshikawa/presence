"""Readiness report for draft and recent newsletter issues."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 20


def build_newsletter_issue_readiness_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    require_sources: bool = True,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Evaluate recent or draft newsletter_sends rows for readiness blockers."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_rows(conn, schema, cutoff, generated_at)
    issues = [_evaluate(row, require_sources, generated_at) for row in rows]
    issues.sort(key=lambda item: (item["status"] != "blocked", -len(item["blocker_codes"]), item["send_at"] or "", item["newsletter_send_id"]))
    return {
        "artifact_type": "newsletter_issue_readiness",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "limit": limit, "require_sources": require_sources},
        "totals": {
            "rows_scanned": len(rows),
            "issues_returned": min(len(issues), limit),
            "by_status": dict(sorted(Counter(item["status"] for item in issues).items())),
            "blocker_counts": dict(sorted(Counter(code for item in issues for code in item["blocker_codes"]).items())),
            "warning_counts": dict(sorted(Counter(code for item in issues for code in item["warning_codes"]).items())),
        },
        "issues": issues[:limit],
        "missing_tables": [] if "newsletter_sends" in schema else ["newsletter_sends"],
    }


def format_newsletter_issue_readiness_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_issue_readiness_text(report: dict[str, Any]) -> str:
    lines = [
        "Newsletter Issue Readiness",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days={report['filters']['days']} limit={report['filters']['limit']} "
            f"require_sources={report['filters']['require_sources']}"
        ),
        f"Totals: scanned={report['totals']['rows_scanned']} returned={report['totals']['issues_returned']}",
    ]
    if not report["issues"]:
        lines.extend(["", "No newsletter issues found."])
        return "\n".join(lines)
    lines.extend(["", "Issues:"])
    for item in report["issues"]:
        lines.append(
            f"- send_id={item['newsletter_send_id']} issue={item['issue_id'] or '-'} "
            f"status={item['status']} blockers={','.join(item['blocker_codes']) or '-'} "
            f"warnings={','.join(item['warning_codes']) or '-'}"
        )
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    if "newsletter_sends" not in schema:
        return []
    cols = schema["newsletter_sends"]
    expr = lambda col: f"ns.{col}" if col in cols else f"NULL AS {col}"
    rows = conn.execute(
        f"""SELECT ns.id, {expr('issue_id')}, {expr('subject')},
                  {expr('source_content_ids')}, {expr('status')},
                  {expr('metadata')}, {expr('sent_at')},
                  {expr('preview_text')}, {expr('body')}
           FROM newsletter_sends ns
           ORDER BY sent_at ASC, ns.id ASC"""
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        send_at = _parse_dt(row["sent_at"]) or now
        status = (row["status"] or "sent").lower()
        if send_at >= cutoff or status == "draft":
            item = dict(row)
            item["send_at_dt"] = send_at
            out.append(item)
    return out


def _evaluate(row: dict[str, Any], require_sources: bool, now: datetime) -> dict[str, Any]:
    metadata = _json_obj(row.get("metadata"))
    source_ids = _parse_source_ids(row.get("source_content_ids"))
    sections = _sections(metadata)
    blockers: list[str] = []
    warnings: list[str] = []
    subject = str(row.get("subject") or "").strip()
    preview = str(row.get("preview_text") or metadata.get("preview") or metadata.get("preview_text") or metadata.get("preheader") or "").strip()
    body = str(row.get("body") or metadata.get("body") or metadata.get("body_html") or metadata.get("body_text") or "").strip()

    if not subject:
        blockers.append("missing_subject")
    if not preview:
        blockers.append("missing_preview")
    if require_sources and not source_ids:
        blockers.append("missing_source_content_ids")
    if not body and not sections:
        blockers.append("empty_body_metadata")
    if sections and len(set(sections)) < 2:
        warnings.append("low_section_diversity")
    if source_ids and len(set(source_ids)) < 2:
        warnings.append("low_source_diversity")
    if not source_ids and not require_sources:
        warnings.append("missing_source_content_ids")
    age_days = int((now - row["send_at_dt"]).total_seconds() // 86400)
    status_value = (row.get("status") or "sent").lower()
    if status_value == "draft" and age_days >= 7:
        warnings.append("stale_draft")
    if status_value == "sent" and age_days >= 14:
        warnings.append("send_age_risk")

    readiness = "blocked" if blockers else ("warning" if warnings else "ready")
    return {
        "newsletter_send_id": int(row["id"]),
        "issue_id": row.get("issue_id"),
        "send_at": row["send_at_dt"].isoformat(),
        "newsletter_status": status_value,
        "status": readiness,
        "blocker_codes": blockers,
        "warning_codes": warnings,
        "source_content_ids": source_ids,
        "section_keys": sorted(set(sections)),
        "source_diversity": len(set(source_ids)),
        "section_diversity": len(set(sections)),
        "age_days": age_days,
    }


def _parse_source_ids(value: Any) -> list[int]:
    if value in (None, ""):
        return []
    raw_values: list[Any]
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except (TypeError, json.JSONDecodeError):
        parsed = None
    if isinstance(parsed, list):
        raw_values = parsed
    else:
        raw_values = re.split(r"[,;|\s]+", str(value).strip())
    ids: list[int] = []
    for item in raw_values:
        try:
            ids.append(int(item))
        except (TypeError, ValueError):
            continue
    return ids


def _sections(metadata: dict[str, Any]) -> list[str]:
    sections = metadata.get("sections") or metadata.get("section_keys")
    if isinstance(sections, list):
        return [str(value).strip() for value in sections if str(value).strip()]
    if isinstance(sections, dict):
        return [str(key).strip() for key in sections if str(key).strip()]
    return []


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}
