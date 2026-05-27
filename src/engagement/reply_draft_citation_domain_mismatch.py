"""Report reply drafts whose linked citation domains do not match claimed sources."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any
from urllib.parse import urlparse


ARTIFACT_TYPE = "reply_draft_citation_domain_mismatch"
DEFAULT_STATUS = "pending,queued"
DEFAULT_WINDOW_DAYS = 14
DEFAULT_LIMIT = 100
TABLES = ("reply_drafts", "reply_queue", "reply_draft_queue")
TEXT_COLUMNS = ("draft_text", "content", "body", "reply_text", "text")
METADATA_COLUMNS = ("metadata", "draft_metadata", "citation_metadata", "platform_metadata")
URL_RE = re.compile(r"https?://[^\s<>)\"']+", re.I)
SOURCE_ALIASES = {
    "github": ("github.com",),
    "github docs": ("docs.github.com", "github.com"),
    "openai": ("openai.com", "platform.openai.com"),
    "openai docs": ("platform.openai.com", "docs.openai.com", "openai.com"),
    "python docs": ("docs.python.org", "python.org"),
    "mozilla mdn": ("developer.mozilla.org",),
    "mdn": ("developer.mozilla.org",),
}


def build_reply_draft_citation_domain_mismatch_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    table = next((name for name in TABLES if name in schema), None)
    if not table:
        return build_reply_draft_citation_domain_mismatch_report([], missing_tables=["reply_drafts|reply_queue"], **kwargs)
    columns = schema[table]
    missing = sorted({"id"} - columns)
    if not set(TEXT_COLUMNS) & columns:
        missing.append("|".join(TEXT_COLUMNS))
    rows = _load_rows(conn, table, columns) if not missing else []
    return build_reply_draft_citation_domain_mismatch_report(rows, missing_columns={table: missing} if missing else None, **kwargs)


def build_reply_draft_citation_domain_mismatch_report(
    rows: list[dict[str, Any]],
    *,
    status: str | None = DEFAULT_STATUS,
    window_days: int = DEFAULT_WINDOW_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
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
        text = str(row.get("text") or "")
        metadata = _json(row.get("metadata"))
        claims = sorted(set(_claimed_sources(text, metadata)))
        urls = _urls(text, metadata)
        for claim in claims:
            expected = SOURCE_ALIASES[claim]
            for url in urls:
                linked = _domain(url)
                if linked and not _domain_matches(linked, expected):
                    findings.append(
                        {
                            "reply_draft_id": row.get("id") or row.get("reply_draft_id"),
                            "platform": _norm(row.get("platform")) or "unknown",
                            "status": draft_status,
                            "claimed_source": claim,
                            "linked_domain": linked,
                            "url": url,
                            "mismatch_reason": f"claimed_{claim.replace(' ', '_')}_but_linked_{linked}",
                        }
                    )
    findings.sort(key=lambda item: (item["claimed_source"], item["linked_domain"], _sort_id(item["reply_draft_id"])))
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"status": status, "window_days": window_days, "limit": limit},
        "summary": {
            "drafts_scanned": scanned,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_claimed_source": dict(sorted(Counter(item["claimed_source"] for item in findings).items())),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items()) if cols},
    }


def format_reply_draft_citation_domain_mismatch_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_draft_citation_domain_mismatch_text(report: dict[str, Any]) -> str:
    lines = [
        "Reply Draft Citation Domain Mismatch",
        f"Generated: {report['generated_at']}",
        f"Filters: status={report['filters']['status'] or '*'} window_days={report['filters']['window_days']} limit={report['filters']['limit']}",
        f"Totals: drafts={report['summary']['drafts_scanned']} findings={report['summary']['finding_count']} shown={report['summary']['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{t}.{c}" for t, cols in report["missing_columns"].items() for c in cols))
    if not report["findings"]:
        lines.append("No citation domain mismatches found.")
        return "\n".join(lines)
    lines.append("Findings:")
    for f in report["findings"]:
        lines.append(f"  - draft={f['reply_draft_id']} claim={f['claimed_source']} linked={f['linked_domain']} reason={f['mismatch_reason']}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, table: str, columns: set[str]) -> list[dict[str, Any]]:
    aliases = {
        "id": ("id",),
        "platform": ("platform",),
        "status": ("draft_status", "status"),
        "text": TEXT_COLUMNS,
        "metadata": METADATA_COLUMNS,
        "created_at": ("created_at",),
        "updated_at": ("updated_at",),
    }
    select = ", ".join(f"{_coalesce(columns, names)} AS {alias}" for alias, names in aliases.items())
    return [dict(row) for row in conn.execute(f"SELECT {select} FROM {table} ORDER BY id ASC").fetchall()]


def _claimed_sources(text: str, metadata: Any) -> list[str]:
    haystack = text.lower()
    claims = [alias for alias in SOURCE_ALIASES if re.search(r"\b" + re.escape(alias) + r"\b", haystack)]
    for value in _metadata_values(metadata, ("claimed_source", "source", "source_name", "citation_source")):
        value_norm = str(value).strip().lower()
        for alias in SOURCE_ALIASES:
            if alias in value_norm:
                claims.append(alias)
    return _most_specific_claims(claims)


def _most_specific_claims(claims: list[str]) -> list[str]:
    unique = sorted(set(claims), key=lambda value: (-len(value), value))
    kept: list[str] = []
    for claim in unique:
        if any(claim != other and claim in other for other in kept):
            continue
        kept.append(claim)
    return kept


def _urls(text: str, metadata: Any) -> list[str]:
    values = [url.rstrip(".,;:!?]") for url in URL_RE.findall(text or "")]
    values.extend(url.rstrip(".,;:!?]") for url in _urls_from_value(metadata))
    return list(dict.fromkeys(values))


def _urls_from_value(value: Any) -> list[str]:
    if isinstance(value, str):
        return URL_RE.findall(value)
    if isinstance(value, list):
        return [url for item in value for url in _urls_from_value(item)]
    if isinstance(value, dict):
        return [url for item in value.values() for url in _urls_from_value(item)]
    return []


def _metadata_values(value: Any, keys: tuple[str, ...]) -> list[Any]:
    if isinstance(value, dict):
        found = [value[key] for key in keys if key in value]
        for child in value.values():
            found.extend(_metadata_values(child, keys))
        return found
    if isinstance(value, list):
        return [item for child in value for item in _metadata_values(child, keys)]
    return []


def _domain(url: str) -> str:
    return urlparse(url if "://" in url else f"https://{url}").netloc.lower().removeprefix("www.")


def _domain_matches(linked: str, expected: tuple[str, ...]) -> bool:
    return any(linked == domain or linked.endswith("." + domain) for domain in expected)


def _json(value: Any) -> Any:
    if not value:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return str(value)


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
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
