"""Report stale newsletter link metadata before a send is reviewed."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any
from urllib.parse import urlparse


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 25


@dataclass(frozen=True)
class NewsletterLinkFreshnessRow:
    newsletter_send_id: int
    issue_id: str
    url: str
    domain: str
    title: str | None
    sent_at: str | None
    last_checked_at: str | None
    age_days: int | None
    issue_labels: tuple[str, ...]
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["issue_labels"] = list(self.issue_labels)
        return payload


@dataclass(frozen=True)
class NewsletterLinkFreshnessReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    links: tuple[NewsletterLinkFreshnessRow, ...]
    schema_warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_link_freshness",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "links": [row.to_dict() for row in self.links],
            "schema_warnings": list(self.schema_warnings),
            "totals": dict(sorted(self.totals.items())),
        }


def build_newsletter_link_freshness_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> NewsletterLinkFreshnessReport:
    """Build a deterministic freshness report from recent newsletter link rows."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {"days": days, "limit": limit, "cutoff": cutoff.isoformat()}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    warnings = _schema_warnings(schema)
    if warnings:
        return _report(generated_at, filters, (), warnings, row_count=0)

    rows = _load_latest_rows(conn, schema, cutoff=cutoff)
    findings = tuple(sorted((_freshness_row(row, days) for row in rows), key=_sort_key)[:limit])
    return _report(generated_at, filters, findings, (), row_count=len(rows))


def format_newsletter_link_freshness_json(report: NewsletterLinkFreshnessReport) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_link_freshness_text(report: NewsletterLinkFreshnessReport) -> str:
    """Render a stable text report."""
    lines = [
        "Newsletter Link Freshness",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['days']} days",
        (
            "Totals: "
            f"rows={report.totals['row_count']} "
            f"links={report.totals['link_count']} "
            f"flagged={report.totals['flagged_link_count']}"
        ),
    ]
    if report.schema_warnings:
        lines.append("Schema warnings: " + "; ".join(report.schema_warnings))
    if not report.links:
        lines.append("No newsletter link freshness issues found.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Links:")
    for row in report.links:
        labels = ",".join(row.issue_labels) if row.issue_labels else "healthy"
        age = "-" if row.age_days is None else str(row.age_days)
        title = row.title or "-"
        checked = row.last_checked_at or "-"
        lines.append(
            f"- {row.url} domain={row.domain or '-'} labels={labels} "
            f"age_days={age} checked={checked} title={title} "
            f"action={row.recommended_action}"
        )
    return "\n".join(lines)


def _load_latest_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    nlc = schema["newsletter_link_clicks"]
    ns = schema["newsletter_sends"]
    raw_metrics = _column_expr(nlc, "raw_metrics", "NULL", alias="nlc")
    raw_url = _column_expr(nlc, "raw_url", "NULL", alias="nlc")
    fetched_at = _column_expr(nlc, "fetched_at", "NULL", alias="nlc")
    issue_id = _column_expr(ns, "issue_id", "''", alias="ns")
    sent_at = _column_expr(ns, "sent_at", "NULL", alias="ns")
    subject = _column_expr(ns, "subject", "''", alias="ns")
    return [
        dict(row)
        for row in conn.execute(
            f"""WITH latest_links AS (
                   SELECT nlc.*
                   FROM newsletter_link_clicks nlc
                   WHERE nlc.id = (
                       SELECT latest.id
                       FROM newsletter_link_clicks latest
                       WHERE latest.newsletter_send_id = nlc.newsletter_send_id
                         AND latest.link_url = nlc.link_url
                       ORDER BY latest.fetched_at DESC, latest.id DESC
                       LIMIT 1
                   )
               )
               SELECT
                   nlc.id,
                   nlc.newsletter_send_id,
                   nlc.link_url,
                   {raw_url} AS raw_url,
                   {raw_metrics} AS raw_metrics,
                   {fetched_at} AS fetched_at,
                   {issue_id} AS issue_id,
                   {sent_at} AS sent_at,
                   {subject} AS subject
               FROM latest_links nlc
               INNER JOIN newsletter_sends ns ON ns.id = nlc.newsletter_send_id
               WHERE ({sent_at} IS NULL OR datetime({sent_at}) >= datetime(?))
               ORDER BY {sent_at} DESC, nlc.newsletter_send_id DESC, nlc.link_url ASC""",
            (cutoff.isoformat(),),
        ).fetchall()
    ]


def _freshness_row(row: dict[str, Any], stale_days: int) -> NewsletterLinkFreshnessRow:
    metadata = _json_object(row.get("raw_metrics"))
    url = str(row.get("link_url") or row.get("raw_url") or "").strip()
    domain = _first_text(metadata, "domain", "host", "hostname") or _domain(url)
    title = _first_text(metadata, "title", "link_title", "page_title")
    last_checked_at = _first_text(
        metadata,
        "last_checked_at",
        "last_checked",
        "checked_at",
        "metadata_checked_at",
    )
    checked_dt = _parse_datetime(last_checked_at)
    sent_dt = _parse_datetime(row.get("sent_at"))
    age_days = _age_days(checked_dt, sent_dt)
    labels: list[str] = []
    if checked_dt is None:
        labels.append("missing_last_checked")
    elif age_days is not None and age_days > stale_days:
        labels.append("stale")
    if not title:
        labels.append("missing_title")
    if not _first_text(metadata, "domain", "host", "hostname"):
        labels.append("missing_domain")
    return NewsletterLinkFreshnessRow(
        newsletter_send_id=int(row["newsletter_send_id"]),
        issue_id=str(row.get("issue_id") or ""),
        url=url,
        domain=domain,
        title=title,
        sent_at=row.get("sent_at"),
        last_checked_at=last_checked_at,
        age_days=age_days,
        issue_labels=tuple(labels),
        recommended_action=_recommended_action(labels),
    )


def _report(
    generated_at: datetime,
    filters: dict[str, Any],
    links: tuple[NewsletterLinkFreshnessRow, ...],
    warnings: tuple[str, ...],
    *,
    row_count: int,
) -> NewsletterLinkFreshnessReport:
    return NewsletterLinkFreshnessReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "row_count": row_count,
            "link_count": len(links),
            "flagged_link_count": sum(1 for row in links if row.issue_labels),
        },
        links=links,
        schema_warnings=warnings,
    )


def _schema_warnings(schema: dict[str, set[str]]) -> tuple[str, ...]:
    required = {
        "newsletter_sends": {"id"},
        "newsletter_link_clicks": {"id", "newsletter_send_id", "link_url"},
    }
    warnings: list[str] = []
    for table, columns in required.items():
        if table not in schema:
            warnings.append(f"missing table: {table}")
            continue
        missing = sorted(columns - schema[table])
        if missing:
            warnings.append(f"missing columns: {table}({', '.join(missing)})")
    return tuple(warnings)


def _sort_key(row: NewsletterLinkFreshnessRow) -> tuple[Any, ...]:
    severity = {"stale": 0, "missing_last_checked": 1, "missing_title": 2, "missing_domain": 3}
    first = min((severity[label] for label in row.issue_labels), default=9)
    return (
        first,
        0 if row.issue_labels else 1,
        -(row.age_days or 0),
        row.domain,
        row.url,
        row.newsletter_send_id,
    )


def _recommended_action(labels: list[str]) -> str:
    if "stale" in labels or "missing_last_checked" in labels:
        return "refresh link metadata before send"
    if "missing_title" in labels:
        return "fetch page title or add manual link label"
    if "missing_domain" in labels:
        return "normalize URL domain metadata"
    return "no action needed"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _column_expr(columns: set[str], column: str, fallback: str, *, alias: str) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _first_text(data: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = data.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _domain(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc.lower()


def _age_days(checked: datetime | None, sent: datetime | None) -> int | None:
    if checked is None:
        return None
    end = sent or datetime.now(timezone.utc)
    return max(0, int((_ensure_utc(end) - checked).total_seconds() // 86400))


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
