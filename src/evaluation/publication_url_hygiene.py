"""Audit published generated content for publication URL hygiene issues."""

from __future__ import annotations

import json
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse


DEFAULT_DAYS = 30
URL_PLATFORMS = {"x", "twitter", "bluesky", "linkedin", "mastodon"}
ISSUE_TYPES = {
    "missing_published_url",
    "malformed_url",
    "duplicate_url",
    "identifier_url_mismatch",
}


def build_publication_url_hygiene_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str | None = None,
    issue_type: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only publication URL hygiene report."""
    if days <= 0:
        raise ValueError("days must be positive")
    if issue_type and issue_type not in ISSUE_TYPES:
        raise ValueError("invalid issue_type")
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _aware(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    records = _publication_records(conn, schema, cutoff)
    if platform:
        records = [record for record in records if record["platform"] == platform]
    issues = _issues(records)
    if issue_type:
        issues = [issue for issue in issues if issue["issue_type"] == issue_type]
    return {
        "artifact_type": "publication_url_hygiene",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "platform": platform,
            "issue_type": issue_type,
            "lookback_start": cutoff.isoformat(),
        },
        "totals": {
            "publication_records_scanned": len(records),
            "issue_count": len(issues),
            "by_issue_type": _counts(issues, "issue_type"),
            "by_platform": _counts(issues, "platform"),
        },
        "issues": issues,
        "empty_state": {
            "is_empty": not issues,
            "schema_present": "generated_content" in schema,
            "message": "No publication URL hygiene issues found." if not issues else None,
        },
    }


def format_publication_url_hygiene_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_url_hygiene_text(report: dict[str, Any]) -> str:
    lines = [
        "Publication URL Hygiene",
        f"Generated: {report['generated_at']}",
        (
            f"Window: {report['filters']['days']} days "
            f"platform={report['filters']['platform'] or 'all'} "
            f"issue_type={report['filters']['issue_type'] or 'all'}"
        ),
        (
            "Totals: "
            f"scanned={report['totals']['publication_records_scanned']} "
            f"issues={report['totals']['issue_count']}"
        ),
    ]
    if not report["issues"]:
        lines.extend(["", report["empty_state"]["message"]])
        return "\n".join(lines)
    lines.extend(["", "Issues:"])
    for issue in report["issues"]:
        lines.append(
            f"- content_id={issue['content_id']} platform={issue['platform']} "
            f"type={issue['issue_type']} url={issue['stored_url'] or '-'} "
            f"id={issue['identifier'] or '-'} action={issue['recommended_action']}"
        )
    return "\n".join(lines)


def _publication_records(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    gc_columns = schema.get("generated_content", set())
    if {"id", "published"}.issubset(gc_columns):
        where = ["COALESCE(published, 0) = 1"]
        params: list[Any] = []
        if "published_at" in gc_columns:
            where.append("(published_at IS NULL OR published_at >= ?)")
            params.append(cutoff.isoformat())
        rows = conn.execute(
            f"""SELECT id, published_url, tweet_id, published_at
                FROM generated_content
                WHERE {' AND '.join(where)}""",
            params,
        ).fetchall()
        for row in rows:
            records.append(
                {
                    "content_id": int(row["id"]),
                    "platform": "x" if row["tweet_id"] else "unknown",
                    "stored_url": row["published_url"],
                    "identifier": row["tweet_id"],
                    "source": "generated_content",
                }
            )
    cp_columns = schema.get("content_publications", set())
    if {"content_id", "platform", "status"}.issubset(cp_columns):
        where = ["LOWER(status) = 'published'"]
        params = []
        if "published_at" in cp_columns:
            where.append("(published_at IS NULL OR published_at >= ?)")
            params.append(cutoff.isoformat())
        rows = conn.execute(
            f"""SELECT content_id, platform, platform_url, platform_post_id
                FROM content_publications
                WHERE {' AND '.join(where)}""",
            params,
        ).fetchall()
        for row in rows:
            records.append(
                {
                    "content_id": int(row["content_id"]),
                    "platform": str(row["platform"] or "unknown"),
                    "stored_url": row["platform_url"],
                    "identifier": row["platform_post_id"],
                    "source": "content_publications",
                }
            )
    return records


def _issues(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    issues = []
    for record in records:
        url = (record["stored_url"] or "").strip()
        platform = record["platform"]
        if platform in URL_PLATFORMS and not url:
            issues.append(_issue(record, "missing_published_url", "populate the published URL from platform response"))
        if url and not _valid_url(url):
            issues.append(_issue(record, "malformed_url", "replace with a fully qualified https URL"))
        if url and record["identifier"] and _identifier_mismatch(platform, record["identifier"], url):
            issues.append(_issue(record, "identifier_url_mismatch", "reconcile platform identifier and stored URL"))

    by_url: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        url = _canonical_url(record["stored_url"])
        if url:
            by_url[url].append(record)
    for url, url_records in sorted(by_url.items()):
        content_ids = sorted({record["content_id"] for record in url_records})
        if len(content_ids) <= 1:
            continue
        for record in sorted(url_records, key=lambda item: (item["content_id"], item["platform"])):
            issues.append(
                {
                    **_issue(record, "duplicate_url", "split duplicate publication URLs or merge duplicate content records"),
                    "duplicate_content_ids": content_ids,
                }
            )
    issues.sort(key=lambda item: (item["content_id"], item["platform"], item["issue_type"]))
    return issues


def _issue(record: dict[str, Any], issue_type: str, action: str) -> dict[str, Any]:
    return {
        "content_id": record["content_id"],
        "platform": record["platform"],
        "issue_type": issue_type,
        "stored_url": record["stored_url"],
        "identifier": record["identifier"],
        "source": record["source"],
        "recommended_action": action,
    }


def _valid_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _canonical_url(value: Any) -> str:
    if not value or not _valid_url(str(value).strip()):
        return ""
    parsed = urlparse(str(value).strip())
    return parsed._replace(scheme=parsed.scheme.lower(), netloc=parsed.netloc.lower(), fragment="").geturl().rstrip("/")


def _identifier_mismatch(platform: str, identifier: str, url: str) -> bool:
    if not _valid_url(url):
        return False
    identifier = str(identifier).strip()
    if platform in {"x", "twitter"}:
        match = re.search(r"/status(?:es)?/([^/?#]+)", url)
        return bool(match and match.group(1) != identifier)
    if platform == "bluesky" and identifier.startswith("at://"):
        rkey = identifier.rstrip("/").split("/")[-1]
        return f"/post/{rkey}" not in url
    return False


def _counts(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = item[key]
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {
        row["name"]: {column["name"] for column in conn.execute(f"PRAGMA table_info({row['name']})")}
        for row in rows
    }


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
