"""Audit newsletter links for missing UTM tracking parameters."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse

from output.newsletter_link_health import extract_newsletter_links
from output.link_tracking import LOCAL_HOSTS


DEFAULT_DAYS = 30
REQUIRED_UTM_PARAMETERS = ("utm_source", "utm_medium", "utm_campaign")
TRACKING_STATUSES = (
    "complete",
    "missing_utm_source",
    "missing_utm_medium",
    "missing_utm_campaign",
    "not_trackable",
)
NEWSLETTER_VARIANT_MARKERS = ("newsletter", "email")
SEND_METADATA_TEXT_KEYS = (
    "body",
    "html",
    "text",
    "content",
    "markdown",
    "preview",
)


@dataclass(frozen=True)
class NewsletterUtmLink:
    """One unique audited URL for a newsletter send."""

    url: str
    status: str
    missing_parameters: tuple[str, ...]
    domain: str
    sources: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "status": self.status,
            "missing_parameters": list(self.missing_parameters),
            "domain": self.domain,
            "sources": list(self.sources),
        }


@dataclass(frozen=True)
class NewsletterUtmSend:
    """Per-send UTM audit details."""

    newsletter_send_id: int
    issue_id: str
    subject: str
    sent_at: str
    status_totals: dict[str, int]
    domain_totals: dict[str, dict[str, int]]
    links: tuple[NewsletterUtmLink, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "newsletter_send_id": self.newsletter_send_id,
            "issue_id": self.issue_id,
            "subject": self.subject,
            "sent_at": self.sent_at,
            "status_totals": dict(self.status_totals),
            "domain_totals": {
                domain: dict(counts)
                for domain, counts in sorted(self.domain_totals.items())
            },
            "links": [link.to_dict() for link in self.links],
        }


@dataclass(frozen=True)
class NewsletterUtmAuditReport:
    """Read-only newsletter UTM coverage audit."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    domain_totals: dict[str, dict[str, int]]
    sends: tuple[NewsletterUtmSend, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_utm_audit",
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "totals": dict(self.totals),
            "domain_totals": {
                domain: dict(counts)
                for domain, counts in sorted(self.domain_totals.items())
            },
            "sends": [send.to_dict() for send in self.sends],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_newsletter_utm_audit_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    include_complete: bool = False,
    now: datetime | None = None,
) -> NewsletterUtmAuditReport:
    """Build a deterministic UTM audit from sends, click rows, and variants."""
    if days <= 0:
        raise ValueError("days must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "sent_after": cutoff.isoformat(),
        "include_complete": include_complete,
    }
    missing_tables = tuple(
        table
        for table in ("newsletter_sends", "newsletter_link_clicks", "content_variants")
        if table not in schema
    )
    missing_columns: dict[str, tuple[str, ...]] = {}
    if "newsletter_sends" not in schema:
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    required_send_columns = {"id", "issue_id", "subject", "sent_at"}
    missing_send_columns = tuple(sorted(required_send_columns - schema["newsletter_sends"]))
    if missing_send_columns:
        missing_columns["newsletter_sends"] = missing_send_columns
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    sends = _load_sends(conn, schema, cutoff)
    send_ids = [int(send["id"]) for send in sends]
    click_urls = _load_click_urls(conn, schema, send_ids)
    variant_urls = _load_variant_urls(conn, schema, sends)

    audited_sends: list[NewsletterUtmSend] = []
    for send in sends:
        send_id = int(send["id"])
        grouped: dict[str, list[str]] = {}
        for url, source in _send_body_urls(send):
            grouped.setdefault(url, []).append(source)
        for url, source in click_urls.get(send_id, ()):
            grouped.setdefault(url, []).append(source)
        for url, source in variant_urls.get(send_id, ()):
            grouped.setdefault(url, []).append(source)

        links = tuple(
            _audit_link(url, sources)
            for url, sources in sorted(grouped.items(), key=lambda item: item[0])
        )
        if not include_complete:
            links = tuple(link for link in links if link.status != "complete")
        audited_sends.append(_audit_send(send, links))

    total_statuses = _status_counts(
        link for audited_send in audited_sends for link in audited_send.links
    )
    domain_totals = _domain_counts(
        link for audited_send in audited_sends for link in audited_send.links
    )
    return NewsletterUtmAuditReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "send_count": len(audited_sends),
            "link_count": sum(len(send.links) for send in audited_sends),
            "status_totals": total_statuses,
            "missing_tables": len(missing_tables),
        },
        domain_totals=domain_totals,
        sends=tuple(audited_sends),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def classify_newsletter_utm_url(url: str) -> tuple[str, tuple[str, ...], str]:
    """Classify one URL and return status, missing UTM parameters, and domain."""
    parsed = urlparse(url)
    scheme = parsed.scheme.casefold()
    domain = (parsed.hostname or parsed.netloc or "").casefold()
    if scheme not in {"http", "https"} or not parsed.netloc:
        return "not_trackable", (), domain
    if domain in LOCAL_HOSTS or domain.endswith(".local"):
        return "not_trackable", (), domain

    query = parse_qs(parsed.query, keep_blank_values=True)
    missing = tuple(
        parameter
        for parameter in REQUIRED_UTM_PARAMETERS
        if not any(value.strip() for value in query.get(parameter, ()))
    )
    if not missing:
        return "complete", (), domain
    return f"missing_{missing[0]}", missing, domain


def format_newsletter_utm_audit_json(report: NewsletterUtmAuditReport) -> str:
    """Serialize the audit as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_utm_audit_text(report: NewsletterUtmAuditReport) -> str:
    """Render a compact human-readable UTM audit."""
    totals = report.totals.get("status_totals", {})
    lines = [
        "Newsletter UTM Audit",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['sent_after']} -> {report.generated_at}",
        (
            "Totals: "
            f"sends={report.totals['send_count']} links={report.totals['link_count']} "
            + " ".join(f"{status}={totals.get(status, 0)}" for status in TRACKING_STATUSES)
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        details = ", ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + details)
    if not report.sends:
        lines.append("No newsletter sends found.")
        return "\n".join(lines)

    for send in report.sends:
        lines.append("")
        lines.append(
            f"Send {send.newsletter_send_id} {send.issue_id or '(no issue)'} "
            f"{send.sent_at}: {send.subject}"
        )
        if not send.links:
            lines.append("  No matching links.")
            continue
        for link in send.links:
            detail = link.status
            if link.missing_parameters:
                detail += f" missing={','.join(link.missing_parameters)}"
            source_text = ",".join(link.sources)
            lines.append(f"  {detail} [{link.domain or 'unknown'}] {link.url} ({source_text})")
    return "\n".join(lines)


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> NewsletterUtmAuditReport:
    return NewsletterUtmAuditReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "send_count": 0,
            "link_count": 0,
            "status_totals": _empty_status_counts(),
            "missing_tables": len(missing_tables),
        },
        domain_totals={},
        sends=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _audit_send(row: dict[str, Any], links: tuple[NewsletterUtmLink, ...]) -> NewsletterUtmSend:
    return NewsletterUtmSend(
        newsletter_send_id=int(row["id"]),
        issue_id=row.get("issue_id") or "",
        subject=row.get("subject") or "",
        sent_at=row.get("sent_at") or "",
        status_totals=_status_counts(links),
        domain_totals=_domain_counts(links),
        links=links,
    )


def _audit_link(url: str, sources: list[str]) -> NewsletterUtmLink:
    status, missing, domain = classify_newsletter_utm_url(url)
    return NewsletterUtmLink(
        url=url,
        status=status,
        missing_parameters=missing,
        domain=domain,
        sources=tuple(sorted(set(sources))),
    )


def _send_body_urls(send: dict[str, Any]) -> list[tuple[str, str]]:
    urls: list[tuple[str, str]] = []
    body = send.get("body")
    if body:
        urls.extend(_extract_http_urls(str(body), "newsletter_sends.body"))

    metadata = _parse_json(send.get("metadata"))
    if isinstance(metadata, dict):
        for source, text in _metadata_texts(metadata, prefix="newsletter_sends.metadata"):
            urls.extend(_extract_http_urls(text, source))
    return urls


def _load_sends(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
) -> list[dict[str, Any]]:
    columns = schema["newsletter_sends"]
    selected = [
        column
        for column in ("id", "issue_id", "subject", "sent_at", "source_content_ids", "metadata", "body")
        if column in columns
    ]
    cursor = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM newsletter_sends
            WHERE datetime(sent_at) >= datetime(?)
            ORDER BY datetime(sent_at) DESC, id DESC""",
        (cutoff.isoformat(),),
    )
    return [_row_dict(cursor, row) for row in cursor.fetchall()]


def _load_click_urls(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    send_ids: list[int],
) -> dict[int, list[tuple[str, str]]]:
    if not send_ids or "newsletter_link_clicks" not in schema:
        return {}
    columns = schema["newsletter_link_clicks"]
    required = {"newsletter_send_id", "link_url"}
    if not required.issubset(columns):
        return {}
    selected = [
        column
        for column in ("id", "newsletter_send_id", "link_url", "raw_url", "fetched_at")
        if column in columns
    ]
    placeholders = ",".join("?" for _ in send_ids)
    cursor = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM newsletter_link_clicks
            WHERE newsletter_send_id IN ({placeholders})
            ORDER BY datetime(fetched_at) DESC, id DESC""",
        send_ids,
    )

    seen: set[tuple[int, str, str]] = set()
    urls: dict[int, list[tuple[str, str]]] = defaultdict(list)
    for row in cursor.fetchall():
        item = _row_dict(cursor, row)
        send_id = int(item["newsletter_send_id"])
        for column in ("link_url", "raw_url"):
            value = item.get(column)
            if not value:
                continue
            for url, source in _extract_http_urls(str(value), f"newsletter_link_clicks.{column}"):
                key = (send_id, column, url)
                if key in seen:
                    continue
                seen.add(key)
                urls[send_id].append((url, source))
    return urls


def _load_variant_urls(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    sends: list[dict[str, Any]],
) -> dict[int, list[tuple[str, str]]]:
    if not sends or "content_variants" not in schema:
        return {}
    columns = schema["content_variants"]
    required = {"content_id", "platform", "variant_type", "content"}
    if not required.issubset(columns):
        return {}

    send_sources = {
        int(send["id"]): _parse_source_content_ids(send.get("source_content_ids"))
        for send in sends
    }
    content_ids = sorted({content_id for ids in send_sources.values() for content_id in ids})
    if not content_ids:
        return {}

    selected = [
        column
        for column in ("content_id", "platform", "variant_type", "content", "metadata")
        if column in columns
    ]
    placeholders = ",".join("?" for _ in content_ids)
    cursor = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM content_variants
            WHERE content_id IN ({placeholders})
            ORDER BY content_id, platform, variant_type, id""",
        content_ids,
    )
    by_content: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in cursor.fetchall():
        item = _row_dict(cursor, row)
        if _is_newsletter_variant(item):
            by_content[int(item["content_id"])].append(item)

    urls: dict[int, list[tuple[str, str]]] = defaultdict(list)
    for send_id, source_ids in send_sources.items():
        for content_id in source_ids:
            for variant in by_content.get(content_id, ()):
                source = (
                    "content_variants:"
                    f"{variant.get('platform') or ''}:{variant.get('variant_type') or ''}"
                )
                urls[send_id].extend(_extract_http_urls(variant.get("content") or "", source))
                metadata = _parse_json(variant.get("metadata"))
                if isinstance(metadata, dict):
                    for meta_source, text in _metadata_texts(
                        metadata,
                        prefix=f"{source}.metadata",
                    ):
                        urls[send_id].extend(_extract_http_urls(text, meta_source))
    return urls


def _is_newsletter_variant(row: dict[str, Any]) -> bool:
    haystack = " ".join(
        str(row.get(key) or "").casefold()
        for key in ("platform", "variant_type")
    )
    if any(marker in haystack for marker in NEWSLETTER_VARIANT_MARKERS):
        return True
    metadata = _parse_json(row.get("metadata"))
    if isinstance(metadata, dict):
        metadata_text = json.dumps(metadata, sort_keys=True).casefold()
        return any(marker in metadata_text for marker in NEWSLETTER_VARIANT_MARKERS)
    return False


def _extract_http_urls(text: str, source: str) -> list[tuple[str, str]]:
    return [
        (occurrence.url, source)
        for occurrence in extract_newsletter_links(body=text)
        if urlparse(occurrence.url).scheme.casefold() in {"http", "https"}
    ]


def _metadata_texts(value: Any, *, prefix: str) -> Iterable[tuple[str, str]]:
    if isinstance(value, dict):
        for key, item in value.items():
            child_prefix = f"{prefix}.{key}"
            if isinstance(item, str) and (
                key in SEND_METADATA_TEXT_KEYS or "url" in key.casefold()
            ):
                yield child_prefix, item
            elif isinstance(item, (dict, list, tuple)):
                yield from _metadata_texts(item, prefix=child_prefix)
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            child_prefix = f"{prefix}[{index}]"
            if isinstance(item, str):
                yield child_prefix, item
            elif isinstance(item, (dict, list, tuple)):
                yield from _metadata_texts(item, prefix=child_prefix)


def _status_counts(links: Iterable[NewsletterUtmLink]) -> dict[str, int]:
    counts = _empty_status_counts()
    for link in links:
        counts[link.status] = counts.get(link.status, 0) + 1
    return counts


def _domain_counts(links: Iterable[NewsletterUtmLink]) -> dict[str, dict[str, int]]:
    counters: dict[str, Counter[str]] = defaultdict(Counter)
    for link in links:
        counters[link.domain or "unknown"][link.status] += 1
    return {
        domain: {status: counter.get(status, 0) for status in TRACKING_STATUSES}
        for domain, counter in sorted(counters.items())
    }


def _empty_status_counts() -> dict[str, int]:
    return {status: 0 for status in TRACKING_STATUSES}


def _parse_source_content_ids(raw_value: Any) -> tuple[int, ...]:
    parsed = _parse_json(raw_value)
    if not isinstance(parsed, list):
        return ()
    ids: list[int] = []
    for item in parsed:
        try:
            ids.append(int(item))
        except (TypeError, ValueError):
            continue
    return tuple(ids)


def _parse_json(raw_value: Any) -> Any:
    if raw_value is None or raw_value == "":
        return None
    if isinstance(raw_value, (dict, list)):
        return raw_value
    try:
        return json.loads(str(raw_value))
    except (TypeError, json.JSONDecodeError):
        return None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or Database-like object")
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        columns = conn.execute(f"PRAGMA table_info({table})").fetchall()
        schema[str(table)] = {
            column["name"] if isinstance(column, sqlite3.Row) else column[1]
            for column in columns
        }
    return schema


def _row_dict(cursor: sqlite3.Cursor, row: Any) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        return dict(row)
    names = [description[0] for description in cursor.description or ()]
    return dict(zip(names, row))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
