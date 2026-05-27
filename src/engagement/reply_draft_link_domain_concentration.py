"""Report pending reply drafts whose outbound links concentrate on one domain."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any
from urllib.parse import urlparse


ARTIFACT_TYPE = "reply_draft_link_domain_concentration"
DEFAULT_STATUS = "pending,queued"
DEFAULT_LIMIT = 100
DEFAULT_WINDOW_DAYS = 14
DEFAULT_MAX_DOMAIN_SHARE = 0.6
TABLES = ("reply_drafts", "reply_queue", "reply_draft_queue")
TEXT_COLUMNS = ("draft_text", "content", "body", "reply_text", "text")
METADATA_COLUMNS = ("metadata", "draft_metadata", "platform_metadata", "raw_target_metadata")
URL_COLUMNS = ("permalink", "target_url", "inbound_url", "url")
URL_RE = re.compile(r"https?://[^\s<>)\"']+", re.IGNORECASE)


def build_reply_draft_link_domain_concentration_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    table = next((name for name in TABLES if name in schema), None)
    if not table:
        return build_reply_draft_link_domain_concentration_report([], missing_tables=["reply_drafts|reply_queue"], **kwargs)
    columns = schema[table]
    missing = sorted({"id"} - columns)
    if missing:
        return build_reply_draft_link_domain_concentration_report([], missing_columns={table: missing}, **kwargs)
    return build_reply_draft_link_domain_concentration_report(_load_rows(conn, table, columns), **kwargs)


def build_reply_draft_link_domain_concentration_report(
    rows: list[dict[str, Any]],
    *,
    status: str | None = DEFAULT_STATUS,
    max_domain_share: float = DEFAULT_MAX_DOMAIN_SHARE,
    window_days: int = DEFAULT_WINDOW_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if not 0 < max_domain_share <= 1:
        raise ValueError("max_domain_share must be between 0 and 1")
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=window_days)
    wanted = _status_set(status)
    findings: list[dict[str, Any]] = []
    scanned = 0
    for row in rows:
        draft_status = _norm(row.get("status") or row.get("draft_status")) or "unknown"
        if wanted and draft_status not in wanted:
            continue
        created_at = _parse_time(row.get("created_at") or row.get("updated_at"))
        if created_at and created_at < cutoff:
            continue
        scanned += 1
        domains = [_domain(url) for url in _urls_from_row(row)]
        domains = [domain for domain in domains if domain]
        total = len(domains)
        if not total:
            continue
        for domain, count in Counter(domains).items():
            share = count / total
            if share > max_domain_share:
                findings.append(
                    {
                        "reply_draft_id": row.get("id") or row.get("reply_draft_id"),
                        "platform": _norm(row.get("platform")) or "unknown",
                        "status": draft_status,
                        "domain": domain,
                        "link_count": count,
                        "total_link_count": total,
                        "domain_share": round(share, 4),
                    }
                )
    findings.sort(key=lambda f: (-f["domain_share"], f["domain"], _sort_id(f["reply_draft_id"])))
    shown = findings[:limit]
    by_domain: dict[str, dict[str, Any]] = {}
    for finding in findings:
        entry = by_domain.setdefault(
            finding["domain"],
            {"domain": finding["domain"], "finding_count": 0, "link_count": 0, "affected_draft_ids": []},
        )
        entry["finding_count"] += 1
        entry["link_count"] += finding["link_count"]
        entry["affected_draft_ids"].append(finding["reply_draft_id"])
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"status": status, "max_domain_share": max_domain_share, "window_days": window_days, "limit": limit},
        "summary": {
            "drafts_scanned": scanned,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_domain": sorted(
                (
                    {**entry, "affected_draft_ids": sorted(set(entry["affected_draft_ids"]), key=_sort_id)}
                    for entry in by_domain.values()
                ),
                key=lambda item: (-item["finding_count"], item["domain"]),
            ),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items()) if cols},
    }


def format_reply_draft_link_domain_concentration_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_draft_link_domain_concentration_text(report: dict[str, Any]) -> str:
    lines = [
        "Reply Draft Link Domain Concentration",
        f"Generated: {report['generated_at']}",
        f"Filters: status={report['filters']['status'] or '*'} max_share={report['filters']['max_domain_share']} window_days={report['filters']['window_days']} limit={report['filters']['limit']}",
        f"Totals: drafts={report['summary']['drafts_scanned']} findings={report['summary']['finding_count']} shown={report['summary']['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{t}.{c}" for t, cols in report["missing_columns"].items() for c in cols))
    if report["summary"]["by_domain"]:
        lines.append("Summary by domain:")
        for item in report["summary"]["by_domain"]:
            lines.append(f"  - {item['domain']}: findings={item['finding_count']} links={item['link_count']} drafts={','.join(str(v) for v in item['affected_draft_ids'])}")
    if not report["findings"]:
        lines.append("No concentrated reply draft link domains found.")
        return "\n".join(lines)
    lines.append("Findings:")
    for f in report["findings"]:
        lines.append(f"  - draft={f['reply_draft_id']} platform={f['platform']} status={f['status']} domain={f['domain']} links={f['link_count']}/{f['total_link_count']} share={f['domain_share']:.2f}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, table: str, columns: set[str]) -> list[dict[str, Any]]:
    aliases = {
        "id": ("id",),
        "platform": ("platform",),
        "status": ("draft_status", "status"),
        "created_at": ("created_at",),
        "updated_at": ("updated_at",),
        "permalink": ("permalink", "target_url", "inbound_url", "url"),
        "text": TEXT_COLUMNS,
        "metadata": METADATA_COLUMNS,
    }
    select = ", ".join(f"{_coalesce(columns, names)} AS {alias}" for alias, names in aliases.items())
    return [dict(row) for row in conn.execute(f"SELECT {select} FROM {table} ORDER BY id ASC").fetchall()]


def _urls_from_row(row: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    for value in (row.get("text"), row.get("permalink")):
        urls.extend(URL_RE.findall(str(value or "")))
    metadata = _json(row.get("metadata"))
    urls.extend(_urls_from_value(metadata))
    return [url.rstrip(".,;:!?]") for url in urls]


def _urls_from_value(value: Any) -> list[str]:
    if isinstance(value, str):
        return URL_RE.findall(value)
    if isinstance(value, list):
        return [url for item in value for url in _urls_from_value(item)]
    if isinstance(value, dict):
        return [url for item in value.values() for url in _urls_from_value(item)]
    return []


def _json(value: Any) -> Any:
    if not value:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return str(value)


def _domain(url: str) -> str:
    parsed = urlparse(url if "://" in url else f"https://{url}")
    return parsed.netloc.lower().removeprefix("www.")


def _coalesce(columns: set[str], names: tuple[str, ...]) -> str:
    available = [name for name in names if name in columns]
    return "COALESCE(" + ", ".join(available) + ")" if len(available) > 1 else (available[0] if available else "NULL")


def _status_set(value: str | None) -> set[str]:
    return {_norm(part) for part in str(value or "").split(",") if _norm(part)}


def _norm(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    return text or None


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


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
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in tables}


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
