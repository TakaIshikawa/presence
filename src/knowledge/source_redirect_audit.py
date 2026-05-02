"""Audit stored knowledge source URL redirect drift without network calls."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .link_metadata_enricher import SOURCE_TYPES, TRACKING_QUERY_PARAMS, TRACKING_QUERY_PREFIXES


DEFAULT_LIMIT = 50
FINAL_URL_KEYS = (
    "final_url",
    "resolved_url",
    "effective_url",
    "redirect_url",
    "url_after_redirects",
)


@dataclass(frozen=True)
class SourceRedirectAuditFinding:
    """One stored source whose URL identity has drifted."""

    source_table: str
    row_id: int
    source_type: str
    source_id: str
    original_url: str | None
    canonical_url: str | None
    final_url: str | None
    normalized_original_url: str | None
    normalized_canonical_url: str | None
    normalized_final_url: str | None
    old_domain: str | None
    new_domain: str | None
    domain_changed: bool
    classification: str
    severity: str
    suggested_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SourceRedirectAuditReport:
    """Read-only report of stored source URL redirect drift."""

    generated_at: str
    filters: dict[str, Any]
    summary: dict[str, int]
    findings: tuple[SourceRedirectAuditFinding, ...]
    missing_optional_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "knowledge_source_redirect_audit",
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "summary": dict(self.summary),
            "findings": [finding.to_dict() for finding in self.findings],
            "missing_optional_tables": list(self.missing_optional_tables),
        }


def normalize_audit_url(url: str | None) -> str | None:
    """Normalize URL identity for deterministic stored-metadata comparisons."""
    if not url or not str(url).strip():
        return None
    parts = urlsplit(str(url).strip())
    scheme = parts.scheme.lower() or "https"
    netloc = parts.netloc.lower()
    if not netloc:
        return None
    host = parts.hostname.lower() if parts.hostname else netloc
    port = parts.port
    if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        host = f"{host}:{port}"
    path = parts.path or ""
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    query_items = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        normalized_key = key.casefold()
        if normalized_key in TRACKING_QUERY_PARAMS:
            continue
        if any(normalized_key.startswith(prefix) for prefix in TRACKING_QUERY_PREFIXES):
            continue
        query_items.append((key, value))
    query = urlencode(sorted(query_items), doseq=True)
    return urlunsplit((scheme, host, path, query, ""))


def audit_knowledge_source_redirects(
    db_or_conn: Any,
    *,
    source_type: str = "all",
    domain_change_only: bool = False,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> SourceRedirectAuditReport:
    """Find stored source URLs whose canonical/final URL has drifted."""
    if source_type not in SOURCE_TYPES:
        raise ValueError(f"source_type must be one of: {', '.join(SOURCE_TYPES)}")
    if limit is not None and limit < 0:
        raise ValueError("limit must be non-negative")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc)).isoformat()
    filters = {
        "source_type": source_type,
        "domain_change_only": domain_change_only,
        "limit": limit,
    }

    rows = _source_rows(conn, schema, source_type)
    findings = [_finding_for_row(row) for row in rows]
    findings = [finding for finding in findings if finding is not None]
    if domain_change_only:
        findings = [finding for finding in findings if finding.domain_changed]
    findings.sort(
        key=lambda finding: (
            _severity_rank(finding.severity),
            finding.source_table,
            finding.source_type,
            finding.source_id,
            finding.row_id,
        )
    )
    limited = findings[:limit] if limit is not None else findings
    summary = {
        "scanned_count": len(rows),
        "finding_count": len(findings),
        "returned_count": len(limited),
        "domain_change_count": sum(1 for finding in findings if finding.domain_changed),
        "canonical_cleanup_count": sum(
            1 for finding in findings if finding.classification == "canonical_cleanup"
        ),
    }
    return SourceRedirectAuditReport(
        generated_at=generated_at,
        filters=filters,
        summary=summary,
        findings=tuple(limited),
        missing_optional_tables=_missing_optional_tables(schema),
    )


def format_source_redirect_audit_json(report: SourceRedirectAuditReport) -> str:
    """Render a redirect audit as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_source_redirect_audit_text(report: SourceRedirectAuditReport) -> str:
    """Render a compact operator-facing redirect audit."""
    filters = report.filters
    summary = report.summary
    lines = [
        "Knowledge source redirect audit",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"source_type={filters['source_type']} "
            f"domain_change_only={_yes_no(filters['domain_change_only'])} "
            f"limit={filters['limit'] if filters['limit'] is not None else '-'}"
        ),
        (
            "Totals: "
            f"scanned={summary['scanned_count']} "
            f"findings={summary['finding_count']} "
            f"returned={summary['returned_count']} "
            f"domain_changes={summary['domain_change_count']} "
            f"canonical_cleanups={summary['canonical_cleanup_count']}"
        ),
    ]
    if report.missing_optional_tables:
        lines.append("Missing optional tables: " + ", ".join(report.missing_optional_tables))
    if not report.findings:
        lines.append("No stored redirect drift found.")
        return "\n".join(lines)

    columns = [
        ("source", "SOURCE", 34),
        ("severity", "SEV", 8),
        ("classification", "CLASSIFICATION", 22),
        ("domains", "DOMAINS", 44),
        ("action", "ACTION", 52),
    ]
    lines.append("  ".join(label.ljust(width) for _, label, width in columns))
    lines.append("  ".join("-" * width for _, _, width in columns))
    for finding in report.findings:
        rendered = {
            "source": f"{finding.source_table}#{finding.row_id}:{finding.source_type}:{finding.source_id}",
            "severity": finding.severity,
            "classification": finding.classification,
            "domains": f"{finding.old_domain or '-'} -> {finding.new_domain or '-'}",
            "action": finding.suggested_action,
        }
        lines.append(
            "  ".join(_clip(rendered[key], width).ljust(width) for key, _, width in columns)
        )
    return "\n".join(lines)


def _finding_for_row(row: dict[str, Any]) -> SourceRedirectAuditFinding | None:
    original_url = _clean(row.get("original_url"))
    canonical_url = _clean(row.get("canonical_url"))
    final_url = _clean(row.get("final_url"))
    normalized_original = normalize_audit_url(original_url)
    normalized_canonical = normalize_audit_url(canonical_url)
    normalized_final = normalize_audit_url(final_url)

    if not normalized_canonical:
        return None
    compare_from = normalized_canonical
    compare_to = normalized_final or normalized_original
    if not compare_to or compare_from == compare_to:
        return None

    old_domain = domain_for_url(compare_from)
    new_domain = domain_for_url(compare_to)
    domain_changed = bool(old_domain and new_domain and old_domain != new_domain)
    classification = _classification(
        domain_changed=domain_changed,
        has_final_url=normalized_final is not None,
    )
    severity = _severity(classification)
    return SourceRedirectAuditFinding(
        source_table=str(row["source_table"]),
        row_id=int(row["row_id"]),
        source_type=str(row.get("source_type") or ""),
        source_id=str(row.get("source_id") or ""),
        original_url=original_url,
        canonical_url=canonical_url,
        final_url=final_url,
        normalized_original_url=normalized_original,
        normalized_canonical_url=normalized_canonical,
        normalized_final_url=normalized_final,
        old_domain=old_domain,
        new_domain=new_domain,
        domain_changed=domain_changed,
        classification=classification,
        severity=severity,
        suggested_action=_suggested_action(classification),
    )


def domain_for_url(url: str | None) -> str | None:
    """Return the normalized domain component used for redirect classification."""
    normalized = normalize_audit_url(url)
    if not normalized:
        return None
    host = urlsplit(normalized).hostname
    if not host:
        return None
    return host[4:] if host.startswith("www.") else host


def _classification(*, domain_changed: bool, has_final_url: bool) -> str:
    if domain_changed:
        return "domain_change_redirect"
    if has_final_url:
        return "same_domain_redirect"
    return "canonical_cleanup"


def _severity(classification: str) -> str:
    if classification == "domain_change_redirect":
        return "high"
    if classification == "same_domain_redirect":
        return "medium"
    return "low"


def _suggested_action(classification: str) -> str:
    if classification == "domain_change_redirect":
        return "Review source trust and update stored canonical URL/domain attribution."
    if classification == "same_domain_redirect":
        return "Refresh stored link metadata to the final normalized URL."
    return "Update stored canonical URL to the normalized canonical form."


def _source_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    source_type: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if source_type in {"all", "knowledge", "curated_article", "curated_newsletter"}:
        rows.extend(_knowledge_rows(conn, schema, source_type))
    if source_type in {"all", "curated_sources", "blog", "newsletter"}:
        rows.extend(_curated_source_rows(conn, schema, source_type))
    return rows


def _knowledge_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    source_type: str,
) -> list[dict[str, Any]]:
    columns = schema.get("knowledge", set())
    if not {"id", "source_type", "source_id"}.issubset(columns):
        return []
    selected = (
        ("curated_article", "curated_newsletter")
        if source_type in {"all", "knowledge"}
        else (source_type,)
    )
    placeholders = ",".join("?" for _ in selected)
    select = {
        "source_url": _column_expr(columns, "source_url"),
        "metadata": _column_expr(columns, "metadata"),
        "created_at": _column_expr(columns, "created_at"),
    }
    rows = conn.execute(
        f"""SELECT id, source_type, source_id,
                  {select['source_url']} AS source_url,
                  {select['metadata']} AS metadata,
                  {select['created_at']} AS created_at
           FROM knowledge
           WHERE source_type IN ({placeholders})
           ORDER BY created_at ASC, id ASC""",
        selected,
    ).fetchall()
    loaded = []
    for row in rows:
        data = dict(row)
        metadata = _parse_metadata(data.get("metadata"))
        link_metadata = metadata.get("link_metadata")
        if not isinstance(link_metadata, dict):
            link_metadata = {}
        loaded.append(
            {
                "source_table": "knowledge",
                "row_id": int(data["id"]),
                "source_type": data.get("source_type"),
                "source_id": data.get("source_id"),
                "original_url": data.get("source_url") or data.get("source_id"),
                "canonical_url": _first_text(
                    link_metadata.get("canonical_url"),
                    metadata.get("canonical_url"),
                ),
                "final_url": _first_text(
                    *[link_metadata.get(key) for key in FINAL_URL_KEYS],
                    *[metadata.get(key) for key in FINAL_URL_KEYS],
                ),
            }
        )
    return loaded


def _curated_source_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    source_type: str,
) -> list[dict[str, Any]]:
    columns = schema.get("curated_sources", set())
    if not {"id", "source_type", "identifier"}.issubset(columns):
        return []
    selected = ("blog", "newsletter") if source_type in {"all", "curated_sources"} else (source_type,)
    placeholders = ",".join("?" for _ in selected)
    filters = [f"source_type IN ({placeholders})"]
    if "status" in columns:
        filters.append("status = 'active'")
    select = {
        "feed_url": _column_expr(columns, "feed_url"),
        "canonical_url": _column_expr(columns, "canonical_url"),
        "metadata": _column_expr(columns, "metadata"),
        "created_at": _column_expr(columns, "created_at"),
    }
    rows = conn.execute(
        f"""SELECT id, source_type, identifier,
                  {select['feed_url']} AS feed_url,
                  {select['canonical_url']} AS canonical_url,
                  {select['metadata']} AS metadata,
                  {select['created_at']} AS created_at
           FROM curated_sources
           WHERE {" AND ".join(filters)}
           ORDER BY created_at ASC, id ASC""",
        selected,
    ).fetchall()
    loaded = []
    for row in rows:
        data = dict(row)
        metadata = _parse_metadata(data.get("metadata"))
        link_metadata = metadata.get("link_metadata")
        if not isinstance(link_metadata, dict):
            link_metadata = {}
        loaded.append(
            {
                "source_table": "curated_sources",
                "row_id": int(data["id"]),
                "source_type": data.get("source_type"),
                "source_id": data.get("identifier"),
                "original_url": data.get("feed_url") or data.get("identifier"),
                "canonical_url": _first_text(
                    data.get("canonical_url"),
                    link_metadata.get("canonical_url"),
                    metadata.get("canonical_url"),
                ),
                "final_url": _first_text(
                    *[link_metadata.get(key) for key in FINAL_URL_KEYS],
                    *[metadata.get(key) for key in FINAL_URL_KEYS],
                ),
            }
        )
    return loaded


def _missing_optional_tables(schema: dict[str, set[str]]) -> tuple[str, ...]:
    return tuple(table for table in ("knowledge", "curated_sources") if table not in schema)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("db_or_conn must be a sqlite3 connection or Database-like object")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {
        _row_value(row, "name", 0): {
            _row_value(info, "name", 1)
            for info in conn.execute(f"PRAGMA table_info({_row_value(row, 'name', 0)})")
        }
        for row in rows
    }


def _column_expr(columns: set[str], column: str) -> str:
    return column if column in columns else "NULL"


def _parse_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _first_text(*values: Any) -> str | None:
    for value in values:
        cleaned = _clean(value)
        if cleaned:
            return cleaned
    return None


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _row_value(row: Any, key: str, index: int) -> Any:
    try:
        return row[key]
    except (IndexError, KeyError, TypeError):
        return row[index]


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _severity_rank(severity: str) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(severity, 9)


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"


def _clip(value: Any, width: int) -> str:
    text = str(value)
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."
