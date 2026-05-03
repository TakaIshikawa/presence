"""Audit content ideas for missing or unusable evidence links."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
import json
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse


DEFAULT_STATUS = "open"
DEFAULT_LIMIT = 100
STATUSES = ("open", "promoted", "dismissed")
PRIORITIES = ("high", "normal", "low")

REASON_NO_EVIDENCE_URL = "no_evidence_url"
REASON_INVALID_URL = "invalid_url"
REASON_DUPLICATE_URL = "duplicate_url"
REASON_SOCIAL_PROFILE_HOMEPAGE = "social_profile_homepage"
REASON_MALFORMED_METADATA = "malformed_metadata"
REASONS = (
    REASON_NO_EVIDENCE_URL,
    REASON_INVALID_URL,
    REASON_DUPLICATE_URL,
    REASON_SOCIAL_PROFILE_HOMEPAGE,
    REASON_MALFORMED_METADATA,
)

URL_KEYS = {
    "url",
    "source_url",
    "evidence_url",
    "evidence_urls",
    "links",
    "references",
}
URL_RE = re.compile(r"https?://[^\s<>()\"']+")
SOCIAL_PROFILE_HOSTS = {
    "bsky.app",
    "facebook.com",
    "github.com",
    "instagram.com",
    "linkedin.com",
    "medium.com",
    "tiktok.com",
    "twitter.com",
    "x.com",
    "youtube.com",
}


@dataclass(frozen=True)
class ContentIdeaEvidenceLinkFinding:
    """One content idea with missing or unusable evidence links."""

    idea_id: int
    topic: str | None
    priority: str
    source: str | None
    created_at: str | None
    extracted_urls: tuple[str, ...]
    reasons: tuple[str, ...]
    status: str | None = None
    invalid_urls: tuple[str, ...] = ()
    duplicate_urls: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["duplicate_urls"] = list(self.duplicate_urls)
        payload["extracted_urls"] = list(self.extracted_urls)
        payload["invalid_urls"] = list(self.invalid_urls)
        payload["reasons"] = list(self.reasons)
        return payload


@dataclass(frozen=True)
class ContentIdeaEvidenceLinkReport:
    """Evidence link audit report for content ideas."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    findings: tuple[ContentIdeaEvidenceLinkFinding, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def has_issues(self) -> bool:
        return bool(self.findings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "content_idea_evidence_links",
            "filters": dict(self.filters),
            "findings": [finding.to_dict() for finding in self.findings],
            "generated_at": self.generated_at,
            "has_issues": self.has_issues,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(self.totals),
        }


def build_content_idea_evidence_link_report(
    db_or_conn: Any,
    *,
    status: str | None = DEFAULT_STATUS,
    priority: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ContentIdeaEvidenceLinkReport:
    """Find content ideas whose source metadata lacks usable evidence URLs."""
    if status is not None and status not in STATUSES:
        raise ValueError(f"status must be one of: {', '.join(STATUSES)}")
    if priority is not None and priority not in PRIORITIES:
        raise ValueError(f"priority must be one of: {', '.join(PRIORITIES)}")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    filters = {"limit": limit, "priority": priority, "status": status}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    rows = (
        _load_rows(conn, schema, status=status, priority=priority)
        if not missing_tables and not any(missing_columns.values())
        else []
    )
    findings = [_audit_row(row) for row in rows]
    findings = [finding for finding in findings if finding is not None]
    return ContentIdeaEvidenceLinkReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(rows, findings),
        findings=tuple(findings[:limit]),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_content_idea_evidence_link_json(report: ContentIdeaEvidenceLinkReport) -> str:
    """Serialize the audit report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_content_idea_evidence_link_text(report: ContentIdeaEvidenceLinkReport) -> str:
    """Render the audit report for terminal review."""
    totals = report.totals
    filters = report.filters
    by_reason = totals["by_reason"]
    lines = [
        "Content Idea Evidence Link Audit",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"status={filters.get('status') or '*'} "
            f"priority={filters.get('priority') or '*'} "
            f"limit={filters['limit']}"
        ),
        (
            "Totals: "
            f"rows_scanned={totals['rows_scanned']} "
            f"ideas_with_issues={totals['ideas_with_issues']} "
            f"issue_count={totals['issue_count']} "
            + " ".join(f"{reason}={by_reason.get(reason, 0)}" for reason in REASONS)
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
            if columns
        )
        if missing:
            lines.append("Missing columns: " + missing)

    if not report.findings:
        lines.extend(["", "No content idea evidence link issues found."])
        return "\n".join(lines)

    lines.extend(["", "Findings:"])
    for finding in report.findings:
        lines.append(
            f"- idea_id={finding.idea_id} priority={finding.priority} "
            f"source={finding.source or '-'} topic={_shorten(finding.topic or '-', 48)} "
            f"created_at={finding.created_at or '-'} reasons={','.join(finding.reasons)}"
        )
        urls = json.dumps(list(finding.extracted_urls), sort_keys=True)
        lines.append("  urls=" + (urls if finding.extracted_urls else "[]"))
    return "\n".join(lines)


def _audit_row(row: Mapping[str, Any]) -> ContentIdeaEvidenceLinkFinding | None:
    metadata, malformed = _decode_metadata(row.get("source_metadata"))
    extracted = _extract_url_candidates(metadata) if metadata is not None else ()
    valid_urls = tuple(url for url in extracted if _is_valid_url(url))
    invalid_urls = tuple(url for url in extracted if not _is_valid_url(url))
    duplicate_urls = _duplicate_urls(valid_urls)
    usable_urls = tuple(
        url for url in dict.fromkeys(valid_urls) if not _is_social_profile_homepage(url)
    )

    reasons: list[str] = []
    if malformed:
        reasons.append(REASON_MALFORMED_METADATA)
    if invalid_urls:
        reasons.append(REASON_INVALID_URL)
    if duplicate_urls:
        reasons.append(REASON_DUPLICATE_URL)
    if valid_urls and not usable_urls:
        reasons.append(REASON_SOCIAL_PROFILE_HOMEPAGE)
    if not usable_urls:
        reasons.append(REASON_NO_EVIDENCE_URL)

    if not reasons:
        return None
    return ContentIdeaEvidenceLinkFinding(
        idea_id=int(row["id"]),
        topic=_none_if_blank(row.get("topic")),
        priority=_priority(row.get("priority")),
        source=_none_if_blank(row.get("source")),
        created_at=_none_if_blank(row.get("created_at")),
        extracted_urls=tuple(dict.fromkeys(extracted)),
        reasons=tuple(reasons),
        status=_none_if_blank(row.get("status")),
        invalid_urls=tuple(dict.fromkeys(invalid_urls)),
        duplicate_urls=duplicate_urls,
    )


def _extract_url_candidates(metadata: Any) -> tuple[str, ...]:
    values: list[str] = []

    def walk(value: Any, *, active: bool = False) -> None:
        if isinstance(value, Mapping):
            for key, nested in value.items():
                key_active = str(key).casefold() in URL_KEYS
                walk(nested, active=active or key_active)
            return
        if isinstance(value, list | tuple | set):
            for nested in value:
                walk(nested, active=active)
            return
        if isinstance(value, str):
            text = value.strip()
            if active:
                values.append(text)
            else:
                values.extend(match.group(0).rstrip(".,;]") for match in URL_RE.finditer(text))

    walk(metadata)
    return tuple(value for value in values if value)


def _decode_metadata(value: Any) -> tuple[dict[str, Any] | None, bool]:
    if isinstance(value, Mapping):
        return dict(value), False
    if value in (None, ""):
        return {}, False
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None, True
    if not isinstance(decoded, dict):
        return None, True
    return decoded, False


def _is_valid_url(value: str) -> bool:
    try:
        parsed = urlparse(value)
    except ValueError:
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _is_social_profile_homepage(value: str) -> bool:
    parsed = urlparse(value)
    host = parsed.netloc.casefold()
    if host.startswith("www."):
        host = host[4:]
    if host not in SOCIAL_PROFILE_HOSTS:
        return False
    path_parts = [part for part in parsed.path.split("/") if part]
    if host in {"github.com", "medium.com"}:
        return len(path_parts) == 1
    if host in {"linkedin.com"}:
        return len(path_parts) >= 2 and path_parts[0] in {"company", "in"}
    if host in {"youtube.com"}:
        return len(path_parts) >= 1 and (
            path_parts[0].startswith("@") or path_parts[0] in {"c", "channel", "user"}
        )
    if host in {"bsky.app"}:
        return len(path_parts) == 2 and path_parts[0] == "profile"
    return len(path_parts) <= 1


def _duplicate_urls(urls: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    duplicates: list[str] = []
    for url in urls:
        key = _canonical_url(url)
        if key in seen and url not in duplicates:
            duplicates.append(url)
        seen.add(key)
    return tuple(duplicates)


def _canonical_url(value: str) -> str:
    parsed = urlparse(value)
    scheme = parsed.scheme.casefold()
    host = parsed.netloc.casefold()
    path = parsed.path.rstrip("/")
    return f"{scheme}://{host}{path}?{parsed.query}" if parsed.query else f"{scheme}://{host}{path}"


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    status: str | None,
    priority: str | None,
) -> list[dict[str, Any]]:
    columns = schema["content_ideas"]
    selected = [
        _column_expr(columns, "id"),
        _column_expr(columns, "topic"),
        _column_expr(columns, "priority", "'normal'"),
        _column_expr(columns, "status", "'open'"),
        _column_expr(columns, "source"),
        _column_expr(columns, "source_metadata"),
        _column_expr(columns, "created_at"),
    ]
    where: list[str] = []
    params: list[Any] = []
    if status is not None:
        where.append("status = ?")
        params.append(status)
    if priority is not None:
        where.append("priority = ?")
        params.append(priority)
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM content_ideas
            {where_clause}
            ORDER BY
                CASE priority
                    WHEN 'high' THEN 0
                    WHEN 'normal' THEN 1
                    WHEN 'low' THEN 2
                    ELSE 3
                END,
                created_at ASC,
                id ASC""",
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows]


def _totals(
    rows: list[dict[str, Any]],
    findings: list[ContentIdeaEvidenceLinkFinding],
) -> dict[str, Any]:
    by_reason = Counter(reason for finding in findings for reason in finding.reasons)
    return {
        "by_reason": {reason: by_reason.get(reason, 0) for reason in REASONS},
        "ideas_with_issues": len(findings),
        "issue_count": sum(by_reason.values()),
        "rows_scanned": len(rows),
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("db_or_conn must be a sqlite3.Connection or expose .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    tables = {row["name"] if isinstance(row, sqlite3.Row) else row[0] for row in rows}
    return {table: _table_columns(conn, table) for table in tables}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        row["name"] if isinstance(row, sqlite3.Row) else row[1]
        for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")
    }


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "content_ideas": {
            "created_at",
            "id",
            "priority",
            "source",
            "source_metadata",
            "status",
            "topic",
        }
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    return missing_tables, missing_columns


def _column_expr(columns: set[str], column: str, fallback: str = "NULL") -> str:
    if column in columns:
        return _quote_identifier(column)
    return f"{fallback} AS {_quote_identifier(column)}"


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _priority(value: Any) -> str:
    text = str(value or "").strip().casefold()
    return text if text in PRIORITIES else "normal"


def _none_if_blank(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _shorten(value: str, limit: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."
