"""Report curated sources that need ingestion recovery."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any
from urllib.parse import urlparse


DEFAULT_DAYS = 30
DEFAULT_FAILURE_THRESHOLD = 3
DEFAULT_LIMIT = 25


@dataclass(frozen=True)
class KnowledgeIngestGapRecoveryRow:
    source_key: str
    domain: str | None
    author: str | None
    case_type: str
    failure_count: int
    last_success_at: str | None
    last_failure_at: str | None
    missing_metadata_fields: tuple[str, ...]
    recovery_priority: str
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["missing_metadata_fields"] = list(self.missing_metadata_fields)
        return payload


@dataclass(frozen=True)
class KnowledgeIngestGapRecoveryReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    sources: tuple[KnowledgeIngestGapRecoveryRow, ...]
    schema_warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "knowledge_ingest_gap_recovery",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "schema_warnings": list(self.schema_warnings),
            "sources": [source.to_dict() for source in self.sources],
            "totals": dict(sorted(self.totals.items())),
        }


def build_knowledge_ingest_gap_recovery_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    failure_threshold: int = DEFAULT_FAILURE_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> KnowledgeIngestGapRecoveryReport:
    if days <= 0:
        raise ValueError("days must be positive")
    if failure_threshold <= 0:
        raise ValueError("failure_threshold must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {"days": days, "failure_threshold": failure_threshold, "limit": limit, "cutoff": cutoff.isoformat()}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    warnings = _schema_warnings(schema)
    if "curated_sources" not in schema:
        return _report(generated_at, filters, (), warnings, source_count=0)
    rows = _source_rows(conn, schema)
    knowledge_success = _knowledge_success(conn, schema)
    findings = [_classify(row, knowledge_success, cutoff, failure_threshold) for row in rows]
    findings = [finding for finding in findings if finding.case_type != "healthy"]
    findings.sort(key=_sort_key)
    return _report(generated_at, filters, tuple(findings[:limit]), warnings, source_count=len(rows))


def format_knowledge_ingest_gap_recovery_json(report: KnowledgeIngestGapRecoveryReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_knowledge_ingest_gap_recovery_text(report: KnowledgeIngestGapRecoveryReport) -> str:
    lines = [
        "Knowledge Ingest Gap Recovery",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['days']} days",
        f"Failure threshold: {report.filters['failure_threshold']}",
        f"Totals: sources={report.totals['source_count']} flagged={report.totals['flagged_count']}",
    ]
    if report.schema_warnings:
        lines.append("Schema warnings: " + "; ".join(report.schema_warnings))
    if not report.sources:
        lines.append("No knowledge ingest recovery gaps found.")
        return "\n".join(lines)
    lines.append("")
    lines.append("Sources:")
    for source in report.sources:
        missing = ",".join(source.missing_metadata_fields) or "-"
        lines.append(
            f"- {source.source_key} case={source.case_type} priority={source.recovery_priority} "
            f"failures={source.failure_count} last_success={source.last_success_at or '-'} "
            f"last_failure={source.last_failure_at or '-'} missing={missing} action={source.recommended_action}"
        )
    return "\n".join(lines)


def _source_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cs = schema["curated_sources"]
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT id,
                      {_column_expr(cs, 'identifier', 'NULL', 'cs')} AS identifier,
                      {_column_expr(cs, 'canonical_url', 'NULL', 'cs')} AS canonical_url,
                      {_column_expr(cs, 'feed_url', 'NULL', 'cs')} AS feed_url,
                      {_column_expr(cs, 'source_type', 'NULL', 'cs')} AS source_type,
                      {_column_expr(cs, 'consecutive_failures', '0', 'cs')} AS consecutive_failures,
                      {_column_expr(cs, 'last_success_at', 'NULL', 'cs')} AS last_success_at,
                      {_column_expr(cs, 'last_failure_at', 'NULL', 'cs')} AS last_failure_at,
                      {_column_expr(cs, 'link_title', 'NULL', 'cs')} AS link_title,
                      {_column_expr(cs, 'site_name', 'NULL', 'cs')} AS site_name,
                      {_column_expr(cs, 'status', "'active'", 'cs')} AS status
               FROM curated_sources cs
               ORDER BY id ASC"""
        )
    ]


def _knowledge_success(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[str, str]:
    if "knowledge" not in schema:
        return {}
    k = schema["knowledge"]
    if not {"source_url", "ingested_at"}.issubset(k):
        return {}
    result: dict[str, str] = {}
    for row in conn.execute("SELECT source_url, author, MAX(ingested_at) AS last_success FROM knowledge GROUP BY source_url, author"):
        for key in _keys(row["source_url"], row["author"]):
            if row["last_success"]:
                result[key] = row["last_success"]
    return result


def _classify(row: dict[str, Any], success: dict[str, str], cutoff: datetime, failure_threshold: int) -> KnowledgeIngestGapRecoveryRow:
    url = row.get("canonical_url") or row.get("feed_url") or row.get("identifier")
    author = row.get("identifier") if str(row.get("source_type") or "").endswith("account") else None
    source_key = str(url or author or row.get("id"))
    last_success = row.get("last_success_at") or next((success[key] for key in _keys(url, author) if key in success), None)
    last_failure = row.get("last_failure_at")
    failure_count = int(row.get("consecutive_failures") or 0)
    missing = _missing_metadata(row)
    success_dt = _parse_datetime(last_success)
    if missing:
        case = "metadata_blocked"
    elif not last_success:
        case = "never_ingested"
    elif failure_count >= failure_threshold:
        case = "repeated_failure"
    elif success_dt and success_dt < cutoff:
        case = "stale_success"
    else:
        case = "healthy"
    return KnowledgeIngestGapRecoveryRow(
        source_key=source_key,
        domain=_domain(url),
        author=author,
        case_type=case,
        failure_count=failure_count,
        last_success_at=last_success,
        last_failure_at=last_failure,
        missing_metadata_fields=tuple(missing),
        recovery_priority=_priority(case, failure_count),
        recommended_action=_action(case),
    )


def _missing_metadata(row: dict[str, Any]) -> list[str]:
    missing = []
    if not (row.get("canonical_url") or row.get("feed_url") or row.get("identifier")):
        missing.append("source_url")
    if str(row.get("source_type") or "") in {"blog", "newsletter"} and not (row.get("link_title") or row.get("site_name")):
        missing.append("title_or_site_name")
    return missing


def _keys(url: Any, author: Any) -> list[str]:
    values = [str(value).lower() for value in (url, author, _domain(url)) if value]
    return values


def _domain(url: Any) -> str | None:
    if not url:
        return None
    parsed = urlparse(str(url) if "://" in str(url) else "https://" + str(url))
    return parsed.netloc.lower() or None


def _priority(case: str, failures: int) -> str:
    if case in {"metadata_blocked", "never_ingested"} or failures >= 5:
        return "high"
    if case in {"repeated_failure", "stale_success"}:
        return "medium"
    return "low"


def _action(case: str) -> str:
    return {
        "metadata_blocked": "fill required source metadata and retry ingest",
        "never_ingested": "run first ingest for source",
        "repeated_failure": "inspect fetch error and retry with backoff",
        "stale_success": "schedule source recrawl",
    }.get(case, "no recovery needed")


def _report(generated_at: datetime, filters: dict[str, Any], sources: tuple[KnowledgeIngestGapRecoveryRow, ...], warnings: tuple[str, ...], *, source_count: int) -> KnowledgeIngestGapRecoveryReport:
    return KnowledgeIngestGapRecoveryReport(generated_at.isoformat(), filters, {"source_count": source_count, "flagged_count": len(sources)}, sources, warnings)


def _schema_warnings(schema: dict[str, set[str]]) -> tuple[str, ...]:
    warnings = []
    if "curated_sources" not in schema:
        warnings.append("missing optional table: curated_sources")
    if "knowledge" not in schema:
        warnings.append("missing optional table: knowledge")
    return tuple(warnings)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _column_expr(columns: set[str], column: str, fallback: str, alias: str) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _sort_key(row: KnowledgeIngestGapRecoveryRow) -> tuple[int, int, str]:
    priorities = {"high": 0, "medium": 1, "low": 2}
    return (priorities.get(row.recovery_priority, 9), -row.failure_count, row.source_key)
