"""Report reply queue rows that duplicate the same reply target."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any
from urllib.parse import urlparse, urlunparse


DEFAULT_LIMIT = 100
DEFAULT_PLATFORM = "all"
TABLE = "reply_queue"
ARTIFACT_TYPE = "reply_queue_duplicate_target_report"
REQUIRED_COLUMNS = {"id", "draft_text"}
TARGET_COLUMNS = ("inbound_tweet_id", "inbound_url", "inbound_cid", "conversation_id", "platform_metadata")
ISSUE_TYPES = (
    "missing_target_metadata",
    "duplicate_pending_drafts",
    "pending_after_sent",
    "conflicting_draft_text",
)
PENDING_STATUSES = {"pending", "draft", "needs_review", "approved_for_review"}
POSTED_STATUSES = {"approved", "sent", "posted", "published", "completed"}
RESOLVED_STATUSES = {"rejected", "dismissed", "expired", "cancelled", "resolved"}
_SPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]")


def build_reply_queue_duplicate_target_report(
    rows: list[dict[str, Any]],
    *,
    platform: str = DEFAULT_PLATFORM,
    include_resolved: bool = False,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic duplicate-target report from reply queue rows."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    normalized_platform = _normalize_platform(platform) or DEFAULT_PLATFORM
    normalized_rows = [
        row
        for row in (_normalize_row(row) for row in rows)
        if (normalized_platform == DEFAULT_PLATFORM or row["platform"] == normalized_platform)
        and (include_resolved or row["status"] not in RESOLVED_STATUSES)
    ]

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    missing_target_rows: list[dict[str, Any]] = []
    for row in normalized_rows:
        if row["target_key"]:
            grouped[(row["platform"], row["target_key"])].append(row)
        else:
            missing_target_rows.append(row)

    findings: list[dict[str, Any]] = [_missing_target_finding(row) for row in missing_target_rows]
    groups = []
    for (group_platform, target_key), group_rows in grouped.items():
        group_findings = _group_findings_for_target(group_rows)
        if group_findings:
            groups.append(_target_group(group_platform, target_key, group_rows, group_findings))
            findings.extend(group_findings)

    missing_groups = [_missing_group(row) for row in missing_target_rows]
    groups.sort(key=_group_sort_key)
    all_groups = sorted(groups + missing_groups, key=_group_sort_key)[:limit]
    shown_findings = [finding for group in all_groups for finding in group["findings"]]

    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"platform": normalized_platform, "include_resolved": include_resolved, "limit": limit},
        "summary": {
            "row_count": len(rows),
            "scanned_count": len(normalized_rows),
            "duplicate_group_count": len(groups),
            "missing_target_count": len(missing_target_rows),
            "finding_count": len(findings),
            "shown_count": len(shown_findings),
            "by_issue_type": _counts(findings),
            "sample_reply_ids": [reply_id for group in all_groups for reply_id in group["reply_ids"]][:10],
        },
        "groups": all_groups,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items())},
        "empty_state": {
            "is_empty": not findings,
            "message": "No reply queue duplicate target issues found." if not findings else None,
        },
    }


def build_reply_queue_duplicate_target_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load reply queue rows from SQLite and build the duplicate-target report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    columns = schema.get(TABLE)
    if columns is None:
        return build_reply_queue_duplicate_target_report([], missing_tables=[TABLE], **kwargs)
    missing = _missing_columns(columns)
    if "draft_text" in missing:
        return build_reply_queue_duplicate_target_report([], missing_columns={TABLE: missing}, **kwargs)
    return build_reply_queue_duplicate_target_report(_load_rows(conn, columns), missing_columns={TABLE: missing} if missing else {}, **kwargs)


def format_reply_queue_duplicate_target_report_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_queue_duplicate_target_report_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    summary = report["summary"]
    lines = [
        "Reply Queue Duplicate Target Report",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"platform={report['filters']['platform']} "
            f"include_resolved={report['filters']['include_resolved']} "
            f"limit={report['filters']['limit']}"
        ),
        (
            "Totals: "
            f"rows={summary['row_count']} scanned={summary['scanned_count']} "
            f"groups={summary['duplicate_group_count']} missing_targets={summary['missing_target_count']} "
            f"findings={summary['finding_count']} shown={summary['shown_count']}"
        ),
        "Sample reply IDs: " + (", ".join(str(item) for item in summary["sample_reply_ids"]) or "-"),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["groups"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.append("Groups:")
    for group in report["groups"]:
        issue_types = ",".join(finding["issue_type"] for finding in group["findings"])
        lines.append(
            f"  - platform={group['platform']} target={group['target_key'] or '-'} "
            f"replies={','.join(str(item) for item in group['reply_ids'])} issues={issue_types}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    aliases = [
        "id",
        "platform",
        "status",
        "draft_text",
        "detected_at",
        "inbound_tweet_id",
        "inbound_url",
        "inbound_cid",
        "conversation_id",
        "platform_metadata",
    ]
    select = ", ".join(f"{_column_expr(columns, alias)} AS {_quote_identifier(alias)}" for alias in aliases)
    rows = conn.execute(f"SELECT {select} FROM reply_queue ORDER BY {_order_clause(columns)}").fetchall()
    return [dict(row) for row in rows]


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    target_key, target_type = _target(row)
    return {
        "reply_id": row.get("reply_id", row.get("id")),
        "platform": _normalize_platform(row.get("platform")) or "x",
        "status": _normalize_status(row.get("status")),
        "draft_text": _clean(row.get("draft_text")),
        "draft_signature": _draft_signature(row.get("draft_text")),
        "detected_at": row.get("detected_at"),
        "target_key": target_key,
        "target_type": target_type,
    }


def _group_findings_for_target(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(rows) < 2:
        return []
    pending = [row for row in rows if row["status"] in PENDING_STATUSES]
    posted = [row for row in rows if row["status"] in POSTED_STATUSES]
    findings: list[dict[str, Any]] = []
    if len(pending) > 1:
        findings.append(_target_finding("duplicate_pending_drafts", rows, affected_reply_ids=[row["reply_id"] for row in pending]))
    if pending and posted:
        findings.append(_target_finding("pending_after_sent", rows, affected_reply_ids=[row["reply_id"] for row in pending], terminal_reply_ids=[row["reply_id"] for row in posted]))
    signatures = {row["draft_signature"] for row in rows if row["draft_signature"]}
    if len(signatures) > 1:
        findings.append(_target_finding("conflicting_draft_text", rows, distinct_draft_count=len(signatures)))
    return findings


def _target_group(platform: str, target_key: str, rows: list[dict[str, Any]], findings: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(rows, key=_row_sort_key)
    return {
        "platform": platform,
        "target_key": target_key,
        "target_type": ordered[0]["target_type"],
        "reply_ids": [row["reply_id"] for row in ordered],
        "statuses": dict(sorted(Counter(row["status"] for row in ordered).items())),
        "draft_texts": [_shorten(row["draft_text"], 120) for row in ordered if row["draft_text"]],
        "first_seen_at": ordered[0]["detected_at"],
        "last_seen_at": ordered[-1]["detected_at"],
        "findings": findings,
    }


def _missing_group(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "platform": row["platform"],
        "target_key": None,
        "target_type": None,
        "reply_ids": [row["reply_id"]],
        "statuses": {row["status"]: 1},
        "draft_texts": [_shorten(row["draft_text"], 120)] if row["draft_text"] else [],
        "first_seen_at": row["detected_at"],
        "last_seen_at": row["detected_at"],
        "findings": [_missing_target_finding(row)],
    }


def _target_finding(issue_type: str, rows: list[dict[str, Any]], **extra: Any) -> dict[str, Any]:
    ordered = sorted(rows, key=_row_sort_key)
    return {
        "issue_type": issue_type,
        "platform": ordered[0]["platform"],
        "target_key": ordered[0]["target_key"],
        "reply_ids": [row["reply_id"] for row in ordered],
        **extra,
    }


def _missing_target_finding(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "issue_type": "missing_target_metadata",
        "platform": row["platform"],
        "target_key": None,
        "reply_ids": [row["reply_id"]],
    }


def _target(row: dict[str, Any]) -> tuple[str | None, str | None]:
    for key in ("inbound_tweet_id", "inbound_cid", "conversation_id"):
        value = _clean(row.get(key))
        if value:
            prefix = "conversation" if key == "conversation_id" else "post"
            return f"{prefix}:{value}", prefix
    url = _normalize_url(row.get("inbound_url"))
    if url:
        return f"url:{url}", "url"
    metadata = _parse_metadata(row.get("platform_metadata"))
    if metadata:
        for key in ("inbound_tweet_id", "tweet_id", "post_id", "uri", "cid"):
            value = _clean(metadata.get(key))
            if value:
                return f"post:{value}", "post"
        for key in ("conversation_id", "conversation_uri", "thread_id"):
            value = _clean(metadata.get(key))
            if value:
                return f"conversation:{value}", "conversation"
    return None, None


def _parse_metadata(raw: Any) -> dict[str, Any] | None:
    if isinstance(raw, dict):
        return raw
    text = _clean(raw)
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _missing_columns(columns: set[str]) -> list[str]:
    missing = sorted(REQUIRED_COLUMNS - columns)
    if not set(TARGET_COLUMNS) & columns:
        missing.append("inbound_tweet_id|inbound_url|inbound_cid|conversation_id|platform_metadata")
    return missing


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({_quote_identifier(str(row[0]))})")} for row in rows}


def _column_expr(columns: set[str], column: str) -> str:
    return _quote_identifier(column) if column in columns else "NULL"


def _order_clause(columns: set[str]) -> str:
    order = []
    if "detected_at" in columns:
        order.append("datetime(detected_at) ASC")
    order.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(order)


def _counts(findings: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(finding["issue_type"] for finding in findings)
    return {issue_type: counts[issue_type] for issue_type in ISSUE_TYPES}


def _group_sort_key(group: dict[str, Any]) -> tuple[Any, ...]:
    issue_rank = min(ISSUE_TYPES.index(finding["issue_type"]) for finding in group["findings"])
    return (issue_rank, group["platform"], group["target_key"] or "", group["reply_ids"][0] or 0)


def _row_sort_key(row: dict[str, Any]) -> tuple[str, int]:
    return (_clean(row.get("detected_at")), _int_sort(row.get("reply_id")))


def _normalize_url(value: Any) -> str | None:
    text = _clean(value)
    if not text:
        return None
    parsed = urlparse(text if "://" in text else f"https://{text}")
    if not parsed.netloc:
        return None
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip("/"), "", "", ""))


def _draft_signature(value: Any) -> str:
    text = _clean(value).casefold()
    text = _PUNCT_RE.sub(" ", text)
    return _SPACE_RE.sub(" ", text).strip()


def _normalize_platform(value: Any) -> str:
    platform = _clean(value).lower()
    return "x" if platform in {"twitter", "tweet"} else platform


def _normalize_status(value: Any) -> str:
    status = _clean(value).lower() or "pending"
    return "posted" if status in {"published", "completed"} else status


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _shorten(value: str, limit: int) -> str:
    text = _SPACE_RE.sub(" ", value).strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "..."


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_sort(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
