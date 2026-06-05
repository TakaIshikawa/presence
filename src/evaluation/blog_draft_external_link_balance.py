from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any
from urllib.parse import urlparse

ARTIFACT_TYPE = "blog_draft_external_link_balance"
DEFAULT_MIN_EXTERNAL_LINKS = 2
DEFAULT_MAX_EXTERNAL_LINKS = 12
DEFAULT_MAX_DOMAIN_SHARE = 0.6
DEFAULT_OWN_DOMAINS = ("example.com",)
DEFAULT_STATUS = ("draft", "ready", "review", "publishable")
DEFAULT_LOOKBACK_DAYS = 90
DEFAULT_LIMIT = 100

_MD_LINK_RE = re.compile(r"(?<!!)\[[^\]]+\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
_BARE_URL_RE = re.compile(r"https?://[^\s<>\]\)\"']+")


def build_blog_draft_external_link_balance_report(
    rows: list[dict[str, Any]],
    *,
    min_external_links: int = DEFAULT_MIN_EXTERNAL_LINKS,
    max_external_links: int = DEFAULT_MAX_EXTERNAL_LINKS,
    max_domain_share: float = DEFAULT_MAX_DOMAIN_SHARE,
    own_domains: tuple[str, ...] | list[str] | None = DEFAULT_OWN_DOMAINS,
    status: tuple[str, ...] | list[str] | str | None = DEFAULT_STATUS,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    _validate(min_external_links, max_external_links, max_domain_share, lookback_days, limit)
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    statuses = _status_set(status)
    own = {_domain(d) for d in (own_domains or ()) if _domain(d)}
    findings: list[dict[str, Any]] = []
    inspected = 0
    for row in rows:
        row_status = _clean(row.get("status")).lower()
        if statuses and row_status and row_status not in statuses:
            continue
        updated = _parse_time(row.get("updated_at") or row.get("created_at"))
        if updated and updated < cutoff:
            continue
        inspected += 1
        urls = extract_external_links(_body(row), own_domains=own)
        domains = [_domain(url) for url in urls]
        counts = Counter(d for d in domains if d)
        total = len(domains)
        unique = len(counts)
        top_domain = ""
        top_count = 0
        if counts:
            top_domain, top_count = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0]
        share = round(top_count / total, 4) if total else 0.0
        reasons = []
        if total < min_external_links:
            reasons.append("too_few_external_links")
        if total > max_external_links:
            reasons.append("too_many_external_links")
        if total >= min_external_links and share > max_domain_share:
            reasons.append("domain_concentration")
        if reasons:
            findings.append(
                {
                    "draft_id": row.get("draft_id") or row.get("id"),
                    "content_id": row.get("content_id"),
                    "title": _clean(row.get("title") or row.get("slug")),
                    "external_link_count": total,
                    "unique_domain_count": unique,
                    "top_domain": top_domain,
                    "top_domain_share": share,
                    "gap_reasons": reasons,
                    "updated_at": _clean(row.get("updated_at") or row.get("created_at")),
                }
            )
    findings.sort(key=lambda f: (-len(f["gap_reasons"]), -f["top_domain_share"], _clean(f["updated_at"]), _clean(f["title"])))
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {
            "min_external_links": min_external_links,
            "max_external_links": max_external_links,
            "max_domain_share": max_domain_share,
            "own_domains": sorted(own),
            "status": sorted(statuses),
            "lookback_days": lookback_days,
            "limit": limit,
        },
        "summary": {"draft_count": inspected, "finding_count": len(findings), "shown_count": len(shown)},
        "findings": shown,
        "missing_tables": missing_tables or [],
        "missing_columns": missing_columns or {},
        "empty_state": {
            "is_empty": not findings,
            "reason": "missing_schema" if missing_tables or missing_columns else ("no_findings" if not findings else None),
            "message": "No blog draft external link balance issues found." if not findings else None,
        },
    }


def build_blog_draft_external_link_balance_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _conn(db_or_conn)
    schema = _schema(conn)
    table = "blog_drafts" if "blog_drafts" in schema else ("generated_content" if "generated_content" in schema else None)
    if not table:
        return build_blog_draft_external_link_balance_report([], missing_tables=["blog_drafts|generated_content"], **kwargs)
    cols = schema[table]
    rows = [
        dict(r)
        for r in conn.execute(
            "SELECT "
            f"{_pick(cols, 'id', 'draft_id', fallback='rowid')} AS id,"
            f"{_pick(cols, 'draft_id', 'id', fallback='NULL')} AS draft_id,"
            f"{_pick(cols, 'content_id', fallback='NULL')} AS content_id,"
            f"{_pick(cols, 'title', 'slug', fallback='NULL')} AS title,"
            f"{_pick(cols, 'body', 'content', 'markdown', 'html', fallback='NULL')} AS body,"
            f"{_pick(cols, 'metadata', 'links', fallback='NULL')} AS metadata,"
            f"{_pick(cols, 'status', fallback='NULL')} AS status,"
            f"{_pick(cols, 'updated_at', 'created_at', fallback='NULL')} AS updated_at "
            f"FROM {table}"
        )
    ]
    return build_blog_draft_external_link_balance_report(rows, **kwargs)


def extract_external_links(text: Any, *, own_domains: set[str] | tuple[str, ...] | list[str] | None = None) -> list[str]:
    raw = _clean(text)
    own = {_domain(d) for d in (own_domains or ()) if _domain(d)}
    urls = []
    spans: list[tuple[int, int]] = []
    for match in _MD_LINK_RE.finditer(raw):
        spans.append(match.span(1))
        urls.append(match.group(1))
    for match in _BARE_URL_RE.finditer(raw):
        if any(start <= match.start() < end for start, end in spans):
            continue
        urls.append(match.group(0))
    external = []
    for url in urls:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            continue
        host = _domain(url)
        if not host or host in own:
            continue
        external.append(url.rstrip(".,;:"))
    return external


def format_blog_draft_external_link_balance_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_blog_draft_external_link_balance_text(report: dict[str, Any]) -> str:
    lines = ["Blog Draft External Link Balance"]
    summary = report.get("summary", {})
    lines.append("Summary: " + ", ".join(f"{k}={v}" for k, v in sorted(summary.items())))
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report.get("findings"):
        lines.append(report.get("empty_state", {}).get("message") or "No findings.")
    for finding in report.get("findings", []):
        lines.append(
            f"- draft={finding.get('draft_id')} title={finding.get('title')} "
            f"links={finding.get('external_link_count')} top={finding.get('top_domain')} "
            f"share={finding.get('top_domain_share')} reasons={','.join(finding.get('gap_reasons', []))}"
        )
    return "\n".join(lines)


def _body(row: dict[str, Any]) -> str:
    return "\n".join(_clean(row.get(k)) for k in ("body", "content", "markdown", "html", "metadata", "links"))


def _validate(min_links: int, max_links: int, max_share: float, lookback_days: int, limit: int) -> None:
    if min_links <= 0 or max_links <= 0 or lookback_days <= 0 or limit <= 0:
        raise ValueError("numeric filters must be positive")
    if min_links > max_links:
        raise ValueError("min_external_links must be <= max_external_links")
    if not 0 < max_share <= 1:
        raise ValueError("max_domain_share must be between 0 and 1")


def _status_set(value: tuple[str, ...] | list[str] | str | None) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        value = (value,)
    return {_clean(v).lower() for v in value if _clean(v)}


def _domain(url_or_domain: Any) -> str:
    text = _clean(url_or_domain).lower()
    parsed = urlparse(text if "://" in text else "//" + text)
    host = (parsed.hostname or "").strip(".")
    return host[4:] if host.startswith("www.") else host


def _conn(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(r[0]): {str(c[1]) for c in conn.execute(f"PRAGMA table_info({r[0]})")} for r in conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}


def _pick(cols: set[str], *names: str, fallback: str = "NULL") -> str:
    return next((name for name in names if name in cols), fallback)


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _parse_time(value: Any) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    try:
        return _utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None
