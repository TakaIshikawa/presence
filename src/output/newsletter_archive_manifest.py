"""Build a portable archive manifest for sent newsletters."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 365
PREVIEW_LENGTH = 180
SENT_STATUSES = ("sent", "resonated", "low_resonance")


@dataclass(frozen=True)
class NewsletterSourcePreview:
    """A generated_content preview referenced by a newsletter issue."""

    content_id: int
    content_type: str | None
    content_format: str | None
    content_preview: str | None
    created_at: str | None
    published_at: str | None
    published_url: str | None
    missing: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterEngagementSnapshot:
    """Latest aggregate engagement snapshot for one newsletter issue."""

    opens: int
    clicks: int
    unsubscribes: int
    fetched_at: str | None
    open_rate: float | None
    click_rate: float | None
    unsubscribe_rate: float | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterLinkSnapshot:
    """Latest link-click snapshot for one newsletter URL."""

    link_url: str
    raw_url: str | None
    clicks: int
    unique_clicks: int | None
    content_id: int | None
    source_kind: str | None
    fetched_at: str | None
    raw_metrics: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterArchiveIssue:
    """One issue-level archive manifest entry."""

    newsletter_send_id: int
    issue_id: str
    subject: str
    sent_at: str
    status: str
    subscriber_count: int
    source_content_ids: tuple[int, ...]
    canonical_source_content_ids: tuple[int, ...]
    source_parse_warnings: tuple[str, ...]
    sources: tuple[NewsletterSourcePreview, ...]
    engagement: NewsletterEngagementSnapshot | None
    links: tuple[NewsletterLinkSnapshot, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "newsletter_send_id": self.newsletter_send_id,
            "issue_id": self.issue_id,
            "subject": self.subject,
            "sent_at": self.sent_at,
            "status": self.status,
            "subscriber_count": self.subscriber_count,
            "source_content_ids": list(self.source_content_ids),
            "canonical_source_content_ids": list(self.canonical_source_content_ids),
            "source_parse_warnings": list(self.source_parse_warnings),
            "sources": [source.to_dict() for source in self.sources],
            "engagement": self.engagement.to_dict() if self.engagement else None,
            "links": [link.to_dict() for link in self.links],
        }


@dataclass(frozen=True)
class NewsletterArchiveManifest:
    """Machine-readable newsletter archive manifest."""

    generated_at: str
    filters: dict[str, Any]
    summary: dict[str, int]
    issues: tuple[NewsletterArchiveIssue, ...]
    missing_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "summary": dict(self.summary),
            "issues": [issue.to_dict() for issue in self.issues],
            "missing_tables": list(self.missing_tables),
        }


def build_newsletter_archive_manifest(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    issue_id: str | None = None,
    now: datetime | None = None,
) -> NewsletterArchiveManifest:
    """Return one manifest issue for each sent newsletter in the filtered window."""
    if days <= 0:
        raise ValueError("days must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "issue_id": issue_id,
        "sent_after": cutoff.isoformat(),
    }
    missing_tables = tuple(
        table
        for table in (
            "newsletter_sends",
            "newsletter_engagement",
            "newsletter_link_clicks",
            "generated_content",
        )
        if table not in schema
    )
    if "newsletter_sends" in missing_tables:
        return _empty_manifest(generated_at, filters, missing_tables)

    sends = _load_sends(conn, schema, cutoff=cutoff, issue_id=issue_id)
    parsed_sources: dict[int, tuple[list[int], list[str]]] = {}
    content_ids: set[int] = set()
    for send in sends:
        source_ids, warnings = parse_source_content_ids(send.get("source_content_ids"))
        send_id = int(send["newsletter_send_id"])
        parsed_sources[send_id] = (source_ids, warnings)
        content_ids.update(source_ids)

    content = _load_generated_content(conn, schema, content_ids)
    send_ids = [int(row["newsletter_send_id"]) for row in sends]
    engagement = _load_latest_engagement(conn, schema, send_ids)
    links = _load_latest_links(conn, schema, send_ids)

    issues: list[NewsletterArchiveIssue] = []
    for send in sends:
        send_id = int(send["newsletter_send_id"])
        source_ids, warnings = parsed_sources.get(send_id, ([], []))
        canonical_source_ids = tuple(_dedupe(source_ids))
        sources = tuple(
            _source_preview(content_id, content.get(content_id))
            for content_id in canonical_source_ids
        )
        issues.append(
            NewsletterArchiveIssue(
                newsletter_send_id=send_id,
                issue_id=send.get("issue_id") or "",
                subject=send.get("subject") or "",
                sent_at=send.get("sent_at") or "",
                status=send.get("status") or "",
                subscriber_count=int(send.get("subscriber_count") or 0),
                source_content_ids=tuple(source_ids),
                canonical_source_content_ids=canonical_source_ids,
                source_parse_warnings=tuple(warnings),
                sources=sources,
                engagement=engagement.get(send_id),
                links=tuple(links.get(send_id, ())),
            )
        )

    return NewsletterArchiveManifest(
        generated_at=generated_at.isoformat(),
        filters=filters,
        summary={
            "issue_count": len(issues),
            "source_count": sum(len(issue.source_content_ids) for issue in issues),
            "canonical_source_count": sum(
                len(issue.canonical_source_content_ids) for issue in issues
            ),
            "issues_with_engagement": sum(1 for issue in issues if issue.engagement),
            "link_count": sum(len(issue.links) for issue in issues),
            "missing_tables": len(missing_tables),
        },
        issues=tuple(issues),
        missing_tables=missing_tables,
    )


def format_newsletter_archive_manifest_json(
    manifest: NewsletterArchiveManifest,
) -> str:
    """Serialize a newsletter archive manifest as deterministic JSON."""
    return json.dumps(manifest.to_dict(), indent=2, sort_keys=True)


def parse_source_content_ids(raw_value: Any) -> tuple[list[int], list[str]]:
    """Parse newsletter_sends.source_content_ids without raising on bad data."""
    if raw_value in (None, ""):
        return [], ["missing_source_content_ids"]
    try:
        parsed = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    except (TypeError, json.JSONDecodeError):
        return [], ["malformed_source_content_ids"]
    if not isinstance(parsed, list):
        return [], ["malformed_source_content_ids"]

    source_ids: list[int] = []
    malformed = False
    for item in parsed:
        try:
            content_id = int(item)
        except (TypeError, ValueError):
            malformed = True
            continue
        if content_id <= 0:
            malformed = True
            continue
        source_ids.append(content_id)

    warnings = ["malformed_source_content_ids"] if malformed else []
    if not source_ids and not warnings:
        warnings.append("missing_source_content_ids")
    return source_ids, warnings


def _empty_manifest(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
) -> NewsletterArchiveManifest:
    return NewsletterArchiveManifest(
        generated_at=generated_at.isoformat(),
        filters=filters,
        summary={
            "issue_count": 0,
            "source_count": 0,
            "canonical_source_count": 0,
            "issues_with_engagement": 0,
            "link_count": 0,
            "missing_tables": len(missing_tables),
        },
        issues=(),
        missing_tables=missing_tables,
    )


def _load_sends(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    issue_id: str | None,
) -> list[dict[str, Any]]:
    columns = schema["newsletter_sends"]
    if "id" not in columns:
        return []
    select = {
        "newsletter_send_id": "ns.id",
        "issue_id": _column_expr(columns, "issue_id", "''", alias="ns"),
        "subject": _column_expr(columns, "subject", "''", alias="ns"),
        "source_content_ids": _column_expr(
            columns, "source_content_ids", "NULL", alias="ns"
        ),
        "subscriber_count": _column_expr(
            columns, "subscriber_count", "0", alias="ns"
        ),
        "status": _column_expr(columns, "status", "''", alias="ns"),
        "sent_at": _column_expr(columns, "sent_at", "NULL", alias="ns"),
    }
    filters = []
    params: list[Any] = []
    if "sent_at" in columns:
        filters.append("ns.sent_at IS NOT NULL")
        filters.append("ns.sent_at >= ?")
        params.append(cutoff.isoformat())
    if "status" in columns:
        placeholders = ",".join("?" for _ in SENT_STATUSES)
        filters.append(f"(ns.status IS NULL OR ns.status IN ({placeholders}))")
        params.extend(SENT_STATUSES)
    if issue_id is not None and "issue_id" in columns:
        filters.append("ns.issue_id = ?")
        params.append(issue_id)
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT
                   {select['newsletter_send_id']} AS newsletter_send_id,
                   {select['issue_id']} AS issue_id,
                   {select['subject']} AS subject,
                   {select['source_content_ids']} AS source_content_ids,
                   {select['subscriber_count']} AS subscriber_count,
                   {select['status']} AS status,
                   {select['sent_at']} AS sent_at
               FROM newsletter_sends ns
               {where_clause}
               ORDER BY {select['sent_at']} DESC, ns.id DESC""",
            params,
        ).fetchall()
    ]


def _load_generated_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: set[int],
) -> dict[int, dict[str, Any]]:
    columns = schema.get("generated_content")
    ids = sorted(content_ids)
    if not ids or not columns or "id" not in columns:
        return {}
    placeholders = ",".join("?" for _ in ids)
    select = {
        "id": "gc.id",
        "content_type": _column_expr(columns, "content_type", alias="gc"),
        "content_format": _column_expr(columns, "content_format", alias="gc"),
        "content": _column_expr(columns, "content", alias="gc"),
        "created_at": _column_expr(columns, "created_at", alias="gc"),
        "published_at": _column_expr(columns, "published_at", alias="gc"),
        "published_url": _column_expr(columns, "published_url", alias="gc"),
    }
    rows = conn.execute(
        f"""SELECT
               {select['id']} AS id,
               {select['content_type']} AS content_type,
               {select['content_format']} AS content_format,
               {select['content']} AS content,
               {select['created_at']} AS created_at,
               {select['published_at']} AS published_at,
               {select['published_url']} AS published_url
           FROM generated_content gc
           WHERE gc.id IN ({placeholders})""",
        ids,
    ).fetchall()
    return {int(row["id"]): dict(row) for row in rows}


def _load_latest_engagement(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    send_ids: list[int],
) -> dict[int, NewsletterEngagementSnapshot]:
    columns = schema.get("newsletter_engagement")
    if not send_ids or not columns or "newsletter_send_id" not in columns:
        return {}
    send_columns = schema.get("newsletter_sends", set())
    placeholders = ",".join("?" for _ in send_ids)
    select = {
        "newsletter_send_id": "ne.newsletter_send_id",
        "opens": _column_expr(columns, "opens", "0", alias="ne"),
        "clicks": _column_expr(columns, "clicks", "0", alias="ne"),
        "unsubscribes": _column_expr(columns, "unsubscribes", "0", alias="ne"),
        "fetched_at": _column_expr(columns, "fetched_at", "NULL", alias="ne"),
        "subscriber_count": _column_expr(
            send_columns, "subscriber_count", "0", alias="ns"
        ),
    }
    rows = conn.execute(
        f"""SELECT
               {select['newsletter_send_id']} AS newsletter_send_id,
               {select['opens']} AS opens,
               {select['clicks']} AS clicks,
               {select['unsubscribes']} AS unsubscribes,
               {select['fetched_at']} AS fetched_at,
               {select['subscriber_count']} AS subscriber_count
           FROM newsletter_engagement ne
           LEFT JOIN newsletter_sends ns ON ns.id = ne.newsletter_send_id
           WHERE ne.newsletter_send_id IN ({placeholders})
             AND ne.id = (
                 SELECT latest.id
                 FROM newsletter_engagement latest
                 WHERE latest.newsletter_send_id = ne.newsletter_send_id
                 ORDER BY datetime(latest.fetched_at) DESC, latest.id DESC
                 LIMIT 1
             )""",
        send_ids,
    ).fetchall()
    snapshots = {}
    for row in rows:
        subscribers = int(row["subscriber_count"] or 0)
        opens = int(row["opens"] or 0)
        clicks = int(row["clicks"] or 0)
        unsubscribes = int(row["unsubscribes"] or 0)
        snapshots[int(row["newsletter_send_id"])] = NewsletterEngagementSnapshot(
            opens=opens,
            clicks=clicks,
            unsubscribes=unsubscribes,
            fetched_at=row["fetched_at"],
            open_rate=_rate(opens, subscribers),
            click_rate=_rate(clicks, subscribers),
            unsubscribe_rate=_rate(unsubscribes, subscribers),
        )
    return snapshots


def _load_latest_links(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    send_ids: list[int],
) -> dict[int, list[NewsletterLinkSnapshot]]:
    columns = schema.get("newsletter_link_clicks")
    if not send_ids or not columns or "newsletter_send_id" not in columns:
        return {}
    placeholders = ",".join("?" for _ in send_ids)
    select = {
        "newsletter_send_id": "nlc.newsletter_send_id",
        "link_url": _column_expr(columns, "link_url", "''", alias="nlc"),
        "raw_url": _column_expr(columns, "raw_url", "NULL", alias="nlc"),
        "clicks": _column_expr(columns, "clicks", "0", alias="nlc"),
        "unique_clicks": _column_expr(columns, "unique_clicks", "NULL", alias="nlc"),
        "content_id": _column_expr(columns, "content_id", "NULL", alias="nlc"),
        "source_kind": _column_expr(columns, "source_kind", "NULL", alias="nlc"),
        "fetched_at": _column_expr(columns, "fetched_at", "NULL", alias="nlc"),
        "raw_metrics": _column_expr(columns, "raw_metrics", "NULL", alias="nlc"),
    }
    rows = conn.execute(
        f"""SELECT
               {select['newsletter_send_id']} AS newsletter_send_id,
               {select['link_url']} AS link_url,
               {select['raw_url']} AS raw_url,
               {select['clicks']} AS clicks,
               {select['unique_clicks']} AS unique_clicks,
               {select['content_id']} AS content_id,
               {select['source_kind']} AS source_kind,
               {select['fetched_at']} AS fetched_at,
               {select['raw_metrics']} AS raw_metrics
           FROM newsletter_link_clicks nlc
           WHERE nlc.newsletter_send_id IN ({placeholders})
             AND nlc.id = (
                 SELECT latest.id
                 FROM newsletter_link_clicks latest
                 WHERE latest.newsletter_send_id = nlc.newsletter_send_id
                   AND latest.link_url = nlc.link_url
                 ORDER BY datetime(latest.fetched_at) DESC, latest.id DESC
                 LIMIT 1
             )
           ORDER BY newsletter_send_id ASC, clicks DESC, link_url ASC""",
        send_ids,
    ).fetchall()
    links: dict[int, list[NewsletterLinkSnapshot]] = {}
    for row in rows:
        send_id = int(row["newsletter_send_id"])
        links.setdefault(send_id, []).append(
            NewsletterLinkSnapshot(
                link_url=row["link_url"] or "",
                raw_url=row["raw_url"],
                clicks=int(row["clicks"] or 0),
                unique_clicks=(
                    None if row["unique_clicks"] is None else int(row["unique_clicks"])
                ),
                content_id=None if row["content_id"] is None else int(row["content_id"]),
                source_kind=row["source_kind"],
                fetched_at=row["fetched_at"],
                raw_metrics=_parse_json_object(row["raw_metrics"]),
            )
        )
    return links


def _source_preview(
    content_id: int,
    row: dict[str, Any] | None,
) -> NewsletterSourcePreview:
    if row is None:
        return NewsletterSourcePreview(
            content_id=content_id,
            content_type=None,
            content_format=None,
            content_preview=None,
            created_at=None,
            published_at=None,
            published_url=None,
            missing=True,
        )
    return NewsletterSourcePreview(
        content_id=content_id,
        content_type=row.get("content_type"),
        content_format=row.get("content_format"),
        content_preview=_preview(row.get("content")),
        created_at=row.get("created_at"),
        published_at=row.get("published_at"),
        published_url=row.get("published_url"),
        missing=False,
    )


def _preview(value: Any, limit: int = PREVIEW_LENGTH) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 4)


def _parse_json_object(raw_value: Any) -> dict[str, Any]:
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _dedupe(values: list[int]) -> list[int]:
    seen = set()
    result = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[table] = {
            item["name"] if isinstance(item, sqlite3.Row) else item[1]
            for item in conn.execute(f"PRAGMA table_info({table})")
        }
    return schema


def _column_expr(
    columns: set[str],
    column: str,
    default: str = "NULL",
    *,
    alias: str,
) -> str:
    if column in columns:
        return f"{alias}.{column}"
    return default


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
