"""Audit proactive action target URLs for shape and platform consistency."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any
from urllib.parse import urlparse


ARTIFACT_TYPE = "proactive_action_target_url_health"
DEFAULT_STATUS = "pending,queued"
DEFAULT_LIMIT = 100
TABLES = ("proactive_actions", "action_queue")
URL_COLUMNS = ("target_url", "url", "permalink", "target_permalink")
METADATA_COLUMNS = ("metadata", "target_metadata", "platform_metadata")
PLATFORM_DOMAINS = {
    "x": ("x.com", "twitter.com"),
    "twitter": ("x.com", "twitter.com"),
    "bluesky": ("bsky.app", "bsky.social"),
    "mastodon": (),
    "linkedin": ("linkedin.com",),
    "github": ("github.com",),
}


def build_proactive_action_target_url_health_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    table = next((name for name in TABLES if name in schema), None)
    if not table:
        return build_proactive_action_target_url_health_report([], missing_tables=["proactive_actions|action_queue"], **kwargs)
    columns = schema[table]
    missing = sorted({"id"} - columns)
    rows = _load_rows(conn, table, columns) if not missing else []
    return build_proactive_action_target_url_health_report(rows, missing_columns={table: missing} if missing else None, **kwargs)


def build_proactive_action_target_url_health_report(
    rows: list[dict[str, Any]],
    *,
    status: str | None = DEFAULT_STATUS,
    platform: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    wanted_status = _status_set(status)
    wanted_platform = _norm(platform)
    candidates: list[dict[str, Any]] = []
    scanned = 0
    for row in rows:
        action_status = _norm(row.get("status") or row.get("state")) or "unknown"
        action_platform = _norm(row.get("platform")) or "unknown"
        if wanted_status and action_status not in wanted_status:
            continue
        if wanted_platform and action_platform != wanted_platform:
            continue
        scanned += 1
        candidates.append({**row, "status": action_status, "platform": action_platform, "target_url": _target_url(row)})
    findings: list[dict[str, Any]] = []
    for row in candidates:
        url = row["target_url"]
        issue = _url_issue(url)
        if issue:
            findings.append(_finding(row, issue, url))
            continue
        mismatch = _platform_mismatch(row["platform"], url)
        if mismatch:
            findings.append(_finding(row, "platform_url_mismatch", url, details=mismatch))
    by_url: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in candidates:
        if row["target_url"] and not _url_issue(row["target_url"]):
            by_url[_canonical_url(row["target_url"])].append(row)
    for url, grouped in by_url.items():
        if len(grouped) > 1:
            action_ids = [row.get("id") or row.get("action_id") for row in grouped]
            for row in grouped:
                findings.append(_finding(row, "duplicate_target_url", row["target_url"], action_ids=action_ids, details=f"duplicate target URL {url}"))
    findings.sort(key=lambda item: (item["issue_type"], _sort_id(item["action_id"])))
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"status": status, "platform": platform, "limit": limit},
        "summary": {
            "actions_scanned": scanned,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": dict(sorted(Counter(item["issue_type"] for item in findings).items())),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items()) if cols},
    }


def format_proactive_action_target_url_health_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_proactive_action_target_url_health_text(report: dict[str, Any]) -> str:
    lines = [
        "Proactive Action Target URL Health",
        f"Generated: {report['generated_at']}",
        f"Filters: status={report['filters']['status'] or '*'} platform={report['filters']['platform'] or '*'} limit={report['filters']['limit']}",
        f"Totals: actions={report['summary']['actions_scanned']} findings={report['summary']['finding_count']} shown={report['summary']['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{t}.{c}" for t, cols in report["missing_columns"].items() for c in cols))
    if report["summary"]["by_issue_type"]:
        lines.append("By issue type: " + ", ".join(f"{k}={v}" for k, v in report["summary"]["by_issue_type"].items()))
    if not report["findings"]:
        lines.append("No proactive action target URL issues found.")
        return "\n".join(lines)
    lines.append("Findings:")
    for f in report["findings"]:
        lines.append(f"  - action={f['action_id']} platform={f['platform']} status={f['status']} issue={f['issue_type']} url={f['target_url'] or '-'}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, table: str, columns: set[str]) -> list[dict[str, Any]]:
    aliases = {
        "id": ("id",),
        "platform": ("platform",),
        "status": ("status", "state"),
        "target_url": URL_COLUMNS,
        "metadata": METADATA_COLUMNS,
    }
    select = ", ".join(f"{_coalesce(columns, names)} AS {alias}" for alias, names in aliases.items())
    return [dict(row) for row in conn.execute(f"SELECT {select} FROM {table} ORDER BY id ASC").fetchall()]


def _finding(row: dict[str, Any], issue_type: str, url: str | None, *, action_ids: list[Any] | None = None, details: str | None = None) -> dict[str, Any]:
    return {
        "action_id": row.get("id") or row.get("action_id"),
        "platform": row.get("platform") or "unknown",
        "status": row.get("status") or "unknown",
        "target_url": url,
        "issue_type": issue_type,
        "related_action_ids": sorted(action_ids or [row.get("id") or row.get("action_id")], key=_sort_id),
        "details": details,
    }


def _target_url(row: dict[str, Any]) -> str | None:
    if row.get("target_url"):
        return str(row["target_url"]).strip()
    metadata = _json(row.get("metadata"))
    for value in _metadata_values(metadata, ("target_url", "url", "permalink", "target_permalink")):
        if value:
            return str(value).strip()
    return None


def _url_issue(value: str | None) -> str | None:
    if not value:
        return "missing_target_url"
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return "malformed_target_url"
    if "." not in parsed.netloc and parsed.netloc != "localhost":
        return "malformed_target_url"
    return None


def _platform_mismatch(platform: str, url: str) -> str | None:
    expected = PLATFORM_DOMAINS.get(platform)
    if not expected:
        return None
    domain = urlparse(url).netloc.lower().removeprefix("www.")
    if any(domain == item or domain.endswith("." + item) for item in expected):
        return None
    return f"expected one of {', '.join(expected)}"


def _canonical_url(value: str) -> str:
    parsed = urlparse(value)
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower().removeprefix('www.')}{parsed.path.rstrip('/')}"


def _metadata_values(value: Any, keys: tuple[str, ...]) -> list[Any]:
    if isinstance(value, dict):
        found = [value[key] for key in keys if key in value]
        for child in value.values():
            found.extend(_metadata_values(child, keys))
        return found
    if isinstance(value, list):
        return [item for child in value for item in _metadata_values(child, keys)]
    return []


def _json(value: Any) -> Any:
    if not value:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return None


def _coalesce(columns: set[str], names: tuple[str, ...]) -> str:
    available = [name for name in names if name in columns]
    return "COALESCE(" + ", ".join(available) + ")" if len(available) > 1 else (available[0] if available else "NULL")


def _status_set(value: str | None) -> set[str]:
    return {_norm(part) for part in str(value or "").split(",") if _norm(part)}


def _norm(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    return text or None


def _sort_id(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, str(value or ""))


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
