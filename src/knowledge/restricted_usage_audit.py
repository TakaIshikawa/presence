"""Audit generated content for restricted knowledge usage before publish."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any
from urllib.parse import urlparse


DEFAULT_DAYS = 30
LICENSE_RESTRICTED = "restricted"
LICENSE_ATTRIBUTION_REQUIRED = "attribution_required"
LICENSE_ALL = "all"
REASON_RESTRICTED = "restricted_source"
REASON_MISSING_ATTRIBUTION = "missing_attribution"


@dataclass(frozen=True)
class RestrictedUsageFinding:
    """One generated-content link that needs licensing review."""

    content_id: int
    content_type: str | None
    knowledge_id: int
    source_url: str | None
    author: str | None
    license: str
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RestrictedUsageAuditReport:
    """Restricted knowledge usage audit result."""

    artifact_type: str
    days: int
    include_published: bool
    license_filter: str
    generated_at: str
    checked_content_count: int
    finding_count: int
    findings: list[RestrictedUsageFinding]
    missing_required_tables: list[str]

    def as_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "findings": [finding.as_dict() for finding in self.findings],
        }


def build_restricted_usage_audit_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    include_published: bool = False,
    license_filter: str = LICENSE_ALL,
    now: datetime | None = None,
) -> RestrictedUsageAuditReport:
    """Find unpublished or queued content linked to restricted knowledge sources."""

    if days < 1:
        raise ValueError("days must be at least 1")
    if license_filter not in {LICENSE_RESTRICTED, LICENSE_ATTRIBUTION_REQUIRED, LICENSE_ALL}:
        raise ValueError("license_filter must be restricted, attribution_required, or all")

    generated_at = _normalize_datetime(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing = [
        table
        for table in ("generated_content", "content_knowledge_links", "knowledge")
        if table not in schema
    ]
    if missing:
        return _empty_report(
            days=days,
            include_published=include_published,
            license_filter=license_filter,
            generated_at=generated_at,
            missing=missing,
        )

    rows = _load_linked_rows(
        conn,
        schema,
        days=days,
        include_published=include_published,
        now=generated_at,
    )
    findings = [
        finding
        for row in rows
        for finding in _classify_row(row, license_filter=license_filter)
    ]
    return RestrictedUsageAuditReport(
        artifact_type="restricted_knowledge_usage_audit",
        days=days,
        include_published=include_published,
        license_filter=license_filter,
        generated_at=generated_at.isoformat(),
        checked_content_count=len({int(row["content_id"]) for row in rows}),
        finding_count=len(findings),
        findings=findings,
        missing_required_tables=[],
    )


def format_restricted_usage_audit_json(report: RestrictedUsageAuditReport) -> str:
    """Serialize an audit report as deterministic JSON."""

    return json.dumps(report.as_dict(), indent=2, sort_keys=True)


def format_restricted_usage_audit_text(report: RestrictedUsageAuditReport) -> str:
    """Render an audit report for terminal review."""

    lines = [
        "Restricted Knowledge Usage Audit",
        (
            "Filters: "
            f"days={report.days} "
            f"include_published={_yes_no(report.include_published)} "
            f"license={report.license_filter}"
        ),
        (
            "Counts: "
            f"content={report.checked_content_count} "
            f"findings={report.finding_count}"
        ),
    ]
    if report.missing_required_tables:
        lines.append("Missing required tables: " + ", ".join(report.missing_required_tables))
    if not report.findings:
        lines.append("")
        lines.append("No restricted or missing-attribution knowledge usage found.")
        return "\n".join(lines)

    current_content_id: int | None = None
    for finding in report.findings:
        if finding.content_id != current_content_id:
            current_content_id = finding.content_id
            lines.append("")
            lines.append(f"Content #{finding.content_id} [{finding.content_type or '-'}]")
        lines.append(
            "  - "
            f"{finding.reason}: knowledge #{finding.knowledge_id} "
            f"license={finding.license} "
            f"author={finding.author or '-'} "
            f"url={finding.source_url or '-'}"
        )
    return "\n".join(lines)


def has_visible_attribution(
    generated_text: str | None,
    *,
    source_url: str | None = None,
    author: str | None = None,
) -> bool:
    """Return whether generated text visibly credits a source URL or author."""

    text = _normalize_text(generated_text or "")
    if not text:
        return False
    if any(candidate in text for candidate in _url_candidates(source_url)):
        return True
    if author and author.strip():
        escaped = re.escape(_normalize_text(author))
        patterns = (
            rf"\bvia\s+{escaped}\b",
            rf"\bsource(?:d)?\s*(?:from|by|:|-)?\s*{escaped}\b",
            rf"\bcredit\s*(?:to|:|-)?\s*{escaped}\b",
            rf"\battribution\s*(?:to|:|-)?\s*{escaped}\b",
            rf"\bh/t\s+{escaped}\b",
            rf"\bby\s+{escaped}\b",
            rf"\bfrom\s+{escaped}\b",
            rf"\bthanks\s+to\s+{escaped}\b",
        )
        return any(re.search(pattern, text) for pattern in patterns)
    return False


def _load_linked_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    days: int,
    include_published: bool,
    now: datetime,
) -> list[dict[str, Any]]:
    content_columns = schema["generated_content"]
    knowledge_columns = schema["knowledge"]
    created_column = "gc.created_at" if "created_at" in content_columns else "gc.id"
    cutoff = now - timedelta(days=days)
    filters: list[str] = []
    params: list[Any] = []
    if "created_at" in content_columns:
        filters.append("datetime(gc.created_at) >= datetime(?)")
        params.append(cutoff.isoformat())
    if not include_published:
        prepublish_filters = []
        if "published" in content_columns:
            prepublish_filters.append("COALESCE(gc.published, 0) != 1")
        if "publish_queue" in schema:
            prepublish_filters.append(
                """EXISTS (
                    SELECT 1
                    FROM publish_queue pq
                    WHERE pq.content_id = gc.id
                      AND COALESCE(pq.status, 'queued') IN ('queued', 'failed', 'held')
                )"""
            )
        if prepublish_filters:
            filters.append("(" + " OR ".join(prepublish_filters) + ")")
    where = "WHERE " + " AND ".join(filters) if filters else ""
    rows = conn.execute(
        f"""SELECT gc.id AS content_id,
                  {_column_expr(content_columns, "content_type", "gc")} AS content_type,
                  {_column_expr(content_columns, "content", "gc")} AS content,
                  k.id AS knowledge_id,
                  {_column_expr(knowledge_columns, "source_url", "k")} AS source_url,
                  {_column_expr(knowledge_columns, "author", "k")} AS author,
                  {_column_expr(knowledge_columns, "license", "k")} AS license,
                  {_column_expr(schema["content_knowledge_links"], "relevance_score", "ckl")} AS relevance_score
           FROM generated_content gc
           INNER JOIN content_knowledge_links ckl ON ckl.content_id = gc.id
           INNER JOIN knowledge k ON k.id = ckl.knowledge_id
           {where}
           ORDER BY {created_column} DESC, gc.id ASC, ckl.relevance_score DESC, k.id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _classify_row(
    row: dict[str, Any],
    *,
    license_filter: str,
) -> list[RestrictedUsageFinding]:
    license_value = _normalize_license(row.get("license"))
    if license_value not in {LICENSE_RESTRICTED, LICENSE_ATTRIBUTION_REQUIRED}:
        return []
    if license_filter != LICENSE_ALL and license_value != license_filter:
        return []

    reason: str | None = None
    if license_value == LICENSE_RESTRICTED:
        reason = REASON_RESTRICTED
    elif not has_visible_attribution(
        row.get("content"),
        source_url=row.get("source_url"),
        author=row.get("author"),
    ):
        reason = REASON_MISSING_ATTRIBUTION

    if reason is None:
        return []
    return [
        RestrictedUsageFinding(
            content_id=int(row["content_id"]),
            content_type=_clean_string(row.get("content_type")),
            knowledge_id=int(row["knowledge_id"]),
            source_url=_clean_string(row.get("source_url")),
            author=_clean_string(row.get("author")),
            license=license_value,
            reason=reason,
        )
    ]


def _empty_report(
    *,
    days: int,
    include_published: bool,
    license_filter: str,
    generated_at: datetime,
    missing: list[str],
) -> RestrictedUsageAuditReport:
    return RestrictedUsageAuditReport(
        artifact_type="restricted_knowledge_usage_audit",
        days=days,
        include_published=include_published,
        license_filter=license_filter,
        generated_at=generated_at.isoformat(),
        checked_content_count=0,
        finding_count=0,
        findings=[],
        missing_required_tables=missing,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[table] = {info[1] for info in conn.execute(f"PRAGMA table_info({table})")}
    return schema


def _column_expr(columns: set[str], column: str, alias: str) -> str:
    return f"{alias}.{column}" if column in columns else "NULL"


def _normalize_license(value: Any) -> str | None:
    text = _clean_string(value)
    if not text:
        return None
    return re.sub(r"[^a-z0-9]+", "_", text.casefold()).strip("_")


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.casefold()).strip()


def _url_candidates(source_url: str | None) -> set[str]:
    raw = _clean_string(source_url)
    if not raw:
        return set()

    candidates = {raw, raw.rstrip("/")}
    parsed = urlparse(raw)
    if parsed.netloc and parsed.path:
        host_path = f"{parsed.netloc}{parsed.path}".rstrip("/")
        candidates.add(host_path)
        if parsed.query:
            candidates.add(f"{host_path}?{parsed.query}")
        if parsed.netloc == "x.com":
            candidates.add(f"twitter.com{parsed.path}".rstrip("/"))
        elif parsed.netloc == "twitter.com":
            candidates.add(f"x.com{parsed.path}".rstrip("/"))
    return {_normalize_text(candidate) for candidate in candidates if candidate}


def _clean_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"
