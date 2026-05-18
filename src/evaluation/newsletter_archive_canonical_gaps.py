"""Audit newsletter archive and canonical URL consistency."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
URL_RE = re.compile(r"https?://[^\s<>)\"']+")
REASON_SEVERITY = {
    "missing_archive_url": "high",
    "missing_canonical_url": "high",
    "duplicate_canonical_url": "high",
    "canonical_archive_mismatch": "medium",
    "embedded_archive_mismatch": "medium",
}
SEVERITY_RANK = {"high": 3, "medium": 2, "low": 1}


def build_newsletter_archive_canonical_gaps_report(
    newsletter_send_rows: list[dict[str, Any]],
    archive_manifest_rows: list[dict[str, Any]] | None = None,
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    manifests = _manifest_by_key(archive_manifest_rows or [])
    sends = [_normalize_send(row, manifests) for row in newsletter_send_rows]
    canonical_counts = Counter(send["normalized_canonical_url"] for send in sends if send["normalized_canonical_url"])
    gaps = []
    reason_counts: Counter[str] = Counter()
    severity_counts: Counter[str] = Counter()

    for send in sends:
        reasons = _reasons(send, canonical_counts)
        if not reasons:
            continue
        for reason in reasons:
            reason_counts[reason] += 1
        severity = _severity(reasons)
        severity_counts[severity] += 1
        gaps.append(
            {
                "issue_id": send["issue_id"],
                "send_id": send["send_id"],
                "subject": send["subject"],
                "sent_at": send["sent_at"],
                "archive_url": send["archive_url"],
                "canonical_url": send["canonical_url"],
                "embedded_url_count": len(send["embedded_archive_urls"]),
                "issue_reason": reasons[0],
                "issue_reasons": reasons,
                "severity": severity,
            }
        )

    gaps.sort(key=lambda item: (-SEVERITY_RANK[item["severity"]], item["sent_at"] or "", item["issue_id"] or "", item["send_id"]))
    shown = gaps[:limit]
    return {
        "artifact_type": "newsletter_archive_canonical_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit},
        "totals": {
            "send_count": len(sends),
            "manifest_count": len(archive_manifest_rows or []),
            "gap_count": len(gaps),
            "shown_count": len(shown),
            "high_severity": severity_counts["high"],
            "medium_severity": severity_counts["medium"],
            "low_severity": severity_counts["low"],
            "reason_counts": {reason: reason_counts[reason] for reason in REASON_SEVERITY},
        },
        "gaps": shown,
        "empty_state": {
            "is_empty": not gaps,
            "message": "No newsletter archive canonical gaps found." if not gaps else None,
        },
    }


def build_newsletter_archive_canonical_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_newsletter_archive_canonical_gaps_report(_load_sends(conn, schema), _load_manifest(conn, schema), **kwargs)


def format_newsletter_archive_canonical_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_archive_canonical_gaps_text(report: dict[str, Any]) -> str:
    lines = [
        "Newsletter Archive Canonical Gaps",
        f"Generated: {report['generated_at']}",
        f"Limit: {report['filters']['limit']}",
        (
            f"Totals: sends={report['totals']['send_count']} gaps={report['totals']['gap_count']} "
            f"high={report['totals']['high_severity']} medium={report['totals']['medium_severity']}"
        ),
    ]
    if not report["gaps"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "issue_id | send_id | severity | reason | embedded_urls | archive_url | canonical_url | subject"])
    for gap in report["gaps"]:
        lines.append(
            f"{gap['issue_id'] or '-'} | {gap['send_id']} | {gap['severity']} | {gap['issue_reason']} | "
            f"{gap['embedded_url_count']} | {gap['archive_url'] or '-'} | {gap['canonical_url'] or '-'} | {gap['subject'] or '-'}"
        )
    return "\n".join(lines)


format_newsletter_archive_canonical_gaps_table = format_newsletter_archive_canonical_gaps_text


def _normalize_send(row: dict[str, Any], manifests: dict[str, dict[str, str]]) -> dict[str, Any]:
    metadata = _json_object(_first(row, "metadata", "raw_metadata"))
    send_id = _text(_first(row, "send_id", "id")) or "unknown"
    issue_id = _text(_first(row, "issue_id", "newsletter_issue_id")) or send_id
    manifest = manifests.get(issue_id) or manifests.get(send_id) or {}
    body = _text(_first(row, "body", "html", "content", "text")) + " " + json.dumps(metadata, sort_keys=True)
    archive_url = _text(
        _first(row, "archive_url", "url", "published_url")
        or metadata.get("archive_url")
        or metadata.get("archiveUrl")
        or manifest.get("archive_url")
    )
    canonical_url = _text(
        _first(row, "canonical_url")
        or metadata.get("canonical_url")
        or metadata.get("canonicalUrl")
        or manifest.get("canonical_url")
    )
    embedded_archive_urls = [_normalize_url(url) for url in URL_RE.findall(body) if _looks_like_archive_url(url)]
    return {
        "send_id": send_id,
        "issue_id": issue_id,
        "subject": _text(_first(row, "subject", "title")),
        "sent_at": _iso(_parse_ts(_first(row, "sent_at", "created_at", "published_at"))),
        "archive_url": archive_url or None,
        "canonical_url": canonical_url or None,
        "normalized_archive_url": _normalize_url(archive_url),
        "normalized_canonical_url": _normalize_url(canonical_url),
        "embedded_archive_urls": sorted(set(url for url in embedded_archive_urls if url)),
    }


def _manifest_by_key(rows: list[dict[str, Any]]) -> dict[str, dict[str, str]]:
    manifests = {}
    for row in rows:
        item = {
            "archive_url": _text(_first(row, "archive_url", "url", "published_url")),
            "canonical_url": _text(_first(row, "canonical_url", "canonical")),
        }
        for key in (_first(row, "issue_id", "newsletter_issue_id"), _first(row, "send_id", "id", "newsletter_send_id")):
            text = _text(key)
            if text:
                manifests[text] = item
    return manifests


def _reasons(send: dict[str, Any], canonical_counts: Counter[str]) -> list[str]:
    reasons = []
    if not send["normalized_archive_url"]:
        reasons.append("missing_archive_url")
    if not send["normalized_canonical_url"]:
        reasons.append("missing_canonical_url")
    if send["normalized_canonical_url"] and canonical_counts[send["normalized_canonical_url"]] > 1:
        reasons.append("duplicate_canonical_url")
    if (
        send["normalized_archive_url"]
        and send["normalized_canonical_url"]
        and send["normalized_archive_url"] != send["normalized_canonical_url"]
    ):
        reasons.append("canonical_archive_mismatch")
    if send["embedded_archive_urls"] and send["normalized_archive_url"] and send["normalized_archive_url"] not in send["embedded_archive_urls"]:
        reasons.append("embedded_archive_mismatch")
    return reasons


def _severity(reasons: list[str]) -> str:
    return max((REASON_SEVERITY[reason] for reason in reasons), key=lambda item: SEVERITY_RANK[item])


def _load_sends(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "newsletter_sends" not in schema:
        return []
    cols = schema["newsletter_sends"]
    selected = [
        _col(cols, "id", "send_id", default="NULL") + " AS send_id",
        _col(cols, "issue_id", "newsletter_issue_id", default="NULL") + " AS issue_id",
        _col(cols, "subject", "title", default="NULL") + " AS subject",
        _col(cols, "sent_at", "created_at", "published_at", default="NULL") + " AS sent_at",
        _col(cols, "archive_url", "url", "published_url", default="NULL") + " AS archive_url",
        _col(cols, "canonical_url", default="NULL") + " AS canonical_url",
        _col(cols, "body", "html", "content", "text", default="NULL") + " AS body",
        _col(cols, "metadata", "raw_metadata", default="NULL") + " AS metadata",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM newsletter_sends").fetchall()]


def _load_manifest(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = next((name for name in ("newsletter_archive_manifest", "newsletter_archives", "newsletter_archive") if name in schema), None)
    if table is None:
        return []
    cols = schema[table]
    selected = [
        _col(cols, "id", "send_id", "newsletter_send_id", default="NULL") + " AS send_id",
        _col(cols, "issue_id", "newsletter_issue_id", default="NULL") + " AS issue_id",
        _col(cols, "archive_url", "url", "published_url", default="NULL") + " AS archive_url",
        _col(cols, "canonical_url", "canonical", default="NULL") + " AS canonical_url",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]


def _looks_like_archive_url(value: str) -> bool:
    text = value.lower()
    return "archive" in text or "newsletter" in text or "/issues/" in text


def _normalize_url(value: Any) -> str:
    text = _text(value)
    return text.rstrip("/").lower()


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_ts(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if value in (None, ""):
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _col(columns: set[str], *names: str, default: str = "NULL") -> str:
    for name in names:
        if name in columns:
            return name
    return default


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
