"""Attribute newsletter unsubscribe pressure to source content and topics."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Mapping


DEFAULT_DAYS = 60
DEFAULT_MIN_UNSUBSCRIBES = 1

MALFORMED_SOURCE_CONTENT_IDS = "malformed_source_content_ids"
NO_SOURCE_CONTENT_IDS = "no_source_content_ids"


@dataclass(frozen=True)
class NewsletterUnsubscribeTopicAttribution:
    """Aggregated unsubscribe pressure for a topic."""

    topic: str
    send_count: int
    source_count: int
    subscriber_count: int
    unsubscribes: int
    unsubscribe_rate: float | None
    pressure_score: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterUnsubscribeContentTypeAttribution:
    """Aggregated unsubscribe pressure for a generated_content content_type."""

    content_type: str
    send_count: int
    source_count: int
    subscriber_count: int
    unsubscribes: int
    unsubscribe_rate: float | None
    pressure_score: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterUnsubscribeSendDetail:
    """One newsletter send with unsubscribe attribution inputs."""

    newsletter_send_id: int
    issue_id: str
    subject: str
    sent_at: str | None
    subscriber_count: int | None
    opens: int | None
    clicks: int | None
    unsubscribes: int
    unsubscribe_rate: float | None
    source_content_ids: tuple[int, ...]
    attributed_topics: tuple[str, ...]
    content_types: tuple[str, ...]
    warnings: tuple[str, ...] = ()
    fetched_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["source_content_ids"] = list(self.source_content_ids)
        payload["attributed_topics"] = list(self.attributed_topics)
        payload["content_types"] = list(self.content_types)
        payload["warnings"] = list(self.warnings)
        return payload


@dataclass(frozen=True)
class NewsletterUnsubscribeAttributionReport:
    """Ranked unsubscribe attribution report for recent newsletter sends."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    ranked_topics: tuple[NewsletterUnsubscribeTopicAttribution, ...]
    ranked_content_types: tuple[NewsletterUnsubscribeContentTypeAttribution, ...]
    send_details: tuple[NewsletterUnsubscribeSendDetail, ...]
    warnings: tuple[str, ...] = ()
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_unsubscribe_attribution",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "ranked_content_types": [
                item.to_dict() for item in self.ranked_content_types
            ],
            "ranked_topics": [item.to_dict() for item in self.ranked_topics],
            "send_details": [item.to_dict() for item in self.send_details],
            "totals": dict(sorted(self.totals.items())),
            "warnings": list(self.warnings),
        }


def build_newsletter_unsubscribe_attribution_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_unsubscribes: int = DEFAULT_MIN_UNSUBSCRIBES,
    topic: str | None = None,
    now: datetime | None = None,
) -> NewsletterUnsubscribeAttributionReport:
    """Build a local, read-only unsubscribe attribution report."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_unsubscribes <= 0:
        raise ValueError("min_unsubscribes must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    topic_filter = _normalize_topic(topic)
    filters = {
        "cutoff": cutoff.isoformat(),
        "days": days,
        "min_unsubscribes": min_unsubscribes,
        "topic": topic_filter,
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)

    blocking = _blocking_schema_gaps(missing_tables, missing_columns)
    if blocking:
        return NewsletterUnsubscribeAttributionReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            totals=_totals(
                send_count=0,
                attributed_send_count=0,
                unsubscribe_count=0,
                warning_count=0,
            ),
            ranked_topics=(),
            ranked_content_types=(),
            send_details=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_send_rows(conn, schema, cutoff=cutoff, min_unsubscribes=min_unsubscribes)
    source_ids = sorted(
        {
            content_id
            for row in rows
            for content_id in _parse_source_ids(row.get("source_content_ids"))[0]
        }
    )
    content_by_id = _load_content(conn, schema, source_ids)
    topics_by_id = _load_topics(conn, schema, source_ids)

    all_details = tuple(
        _send_detail(row, content_by_id=content_by_id, topics_by_id=topics_by_id)
        for row in rows
    )
    details = tuple(
        detail
        for detail in all_details
        if topic_filter is None
        or topic_filter in {_normalize_topic(item) for item in detail.attributed_topics}
    )
    warnings = tuple(
        sorted({warning for detail in details for warning in detail.warnings})
    )

    return NewsletterUnsubscribeAttributionReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(
            send_count=len(details),
            attributed_send_count=sum(
                1
                for detail in details
                if detail.attributed_topics or detail.content_types
            ),
            unsubscribe_count=sum(detail.unsubscribes for detail in details),
            warning_count=sum(len(detail.warnings) for detail in details),
        ),
        ranked_topics=_rank_topics(details),
        ranked_content_types=_rank_content_types(details),
        send_details=details,
        warnings=warnings,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_newsletter_unsubscribe_attribution_json(
    report: NewsletterUnsubscribeAttributionReport,
) -> str:
    """Format an unsubscribe attribution report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_unsubscribe_attribution_text(
    report: NewsletterUnsubscribeAttributionReport,
) -> str:
    """Format a concise markdown report for operators."""
    filters = report.filters
    totals = report.totals
    lines = [
        "# Newsletter Unsubscribe Attribution",
        f"Generated: {report.generated_at}",
        (
            f"Window: {filters['days']} days cutoff={filters['cutoff']} "
            f"min_unsubscribes={filters['min_unsubscribes']} "
            f"topic={filters['topic'] or '-'}"
        ),
        (
            f"Totals: sends={totals['send_count']} attributed={totals['attributed_send_count']} "
            f"unsubscribes={totals['unsubscribe_count']} warnings={totals['warning_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        ]
        lines.append("Missing columns: " + "; ".join(missing))
    if report.warnings:
        lines.append("Warnings: " + ", ".join(report.warnings))
    lines.append("")

    if not report.send_details:
        lines.append("No newsletter sends with unsubscribe attribution found.")
        return "\n".join(lines)

    lines.append("## Highest Unsubscribe-Rate Sends")
    for detail in report.send_details[:10]:
        lines.append(
            f"- send={detail.newsletter_send_id} issue={detail.issue_id or '-'} "
            f"rate={_format_rate(detail.unsubscribe_rate)} "
            f"unsubscribes={detail.unsubscribes} subject={detail.subject or '-'}"
        )
        lines.append(
            "  attribution: "
            f"topics={_format_items(detail.attributed_topics)} "
            f"content_types={_format_items(detail.content_types)} "
            f"sources={_format_items(detail.source_content_ids)}"
        )
        if detail.warnings:
            lines.append(f"  warnings={', '.join(detail.warnings)}")

    lines.append("")
    lines.append("## Top Topics")
    if report.ranked_topics:
        for item in report.ranked_topics[:10]:
            lines.append(
                f"- topic={item.topic} pressure={item.pressure_score:.4f} "
                f"rate={_format_rate(item.unsubscribe_rate)} "
                f"unsubscribes={item.unsubscribes} sends={item.send_count}"
            )
    else:
        lines.append("- none")

    lines.append("")
    lines.append("## Top Content Types")
    if report.ranked_content_types:
        for item in report.ranked_content_types[:10]:
            lines.append(
                f"- content_type={item.content_type} pressure={item.pressure_score:.4f} "
                f"rate={_format_rate(item.unsubscribe_rate)} "
                f"unsubscribes={item.unsubscribes} sends={item.send_count}"
            )
    else:
        lines.append("- none")
    return "\n".join(lines)


def _send_detail(
    row: Mapping[str, Any],
    *,
    content_by_id: Mapping[int, Mapping[str, Any]],
    topics_by_id: Mapping[int, tuple[str, ...]],
) -> NewsletterUnsubscribeSendDetail:
    source_ids, parse_warnings = _parse_source_ids(row.get("source_content_ids"))
    warnings = set(parse_warnings)
    if not source_ids:
        warnings.add(NO_SOURCE_CONTENT_IDS)

    topics = sorted(
        {
            topic
            for content_id in source_ids
            for topic in topics_by_id.get(content_id, ())
            if topic
        },
        key=lambda item: item.lower(),
    )
    content_types = sorted(
        {
            str(content_by_id[content_id].get("content_type") or "unknown")
            for content_id in source_ids
            if content_id in content_by_id
        },
        key=lambda item: item.lower(),
    )
    subscribers = _optional_int(row.get("subscriber_count"))
    unsubscribes = _optional_int(row.get("unsubscribes")) or 0

    return NewsletterUnsubscribeSendDetail(
        newsletter_send_id=int(row.get("newsletter_send_id") or row.get("id")),
        issue_id=str(row.get("issue_id") or ""),
        subject=str(row.get("subject") or ""),
        sent_at=_text_or_none(row.get("sent_at")),
        subscriber_count=subscribers,
        opens=_optional_int(row.get("opens")),
        clicks=_optional_int(row.get("clicks")),
        unsubscribes=unsubscribes,
        unsubscribe_rate=_rate(unsubscribes, subscribers),
        source_content_ids=tuple(source_ids),
        attributed_topics=tuple(topics),
        content_types=tuple(content_types),
        warnings=tuple(sorted(warnings)),
        fetched_at=_text_or_none(row.get("fetched_at")),
    )


def _rank_topics(
    details: tuple[NewsletterUnsubscribeSendDetail, ...],
) -> tuple[NewsletterUnsubscribeTopicAttribution, ...]:
    buckets: dict[str, list[NewsletterUnsubscribeSendDetail]] = defaultdict(list)
    source_counts: dict[str, int] = defaultdict(int)
    for detail in details:
        for topic in detail.attributed_topics:
            buckets[topic].append(detail)
            source_counts[topic] += sum(
                1 for content_id in detail.source_content_ids if topic
            )
    rows = [
        _topic_attribution(topic, bucket, source_counts[topic])
        for topic, bucket in buckets.items()
    ]
    return tuple(
        sorted(
            rows,
            key=lambda item: (
                item.pressure_score,
                item.unsubscribe_rate or 0.0,
                item.unsubscribes,
                item.topic.lower(),
            ),
            reverse=True,
        )
    )


def _rank_content_types(
    details: tuple[NewsletterUnsubscribeSendDetail, ...],
) -> tuple[NewsletterUnsubscribeContentTypeAttribution, ...]:
    buckets: dict[str, list[NewsletterUnsubscribeSendDetail]] = defaultdict(list)
    source_counts: dict[str, int] = defaultdict(int)
    for detail in details:
        for content_type in detail.content_types:
            buckets[content_type].append(detail)
            source_counts[content_type] += sum(
                1 for _content_id in detail.source_content_ids
            )
    rows = [
        _content_type_attribution(content_type, bucket, source_counts[content_type])
        for content_type, bucket in buckets.items()
    ]
    return tuple(
        sorted(
            rows,
            key=lambda item: (
                item.pressure_score,
                item.unsubscribe_rate or 0.0,
                item.unsubscribes,
                item.content_type.lower(),
            ),
            reverse=True,
        )
    )


def _topic_attribution(
    topic: str,
    details: list[NewsletterUnsubscribeSendDetail],
    source_count: int,
) -> NewsletterUnsubscribeTopicAttribution:
    subscribers = sum(max(detail.subscriber_count or 0, 0) for detail in details)
    unsubscribes = sum(detail.unsubscribes for detail in details)
    rate = _rate(unsubscribes, subscribers)
    return NewsletterUnsubscribeTopicAttribution(
        topic=topic,
        send_count=len({detail.newsletter_send_id for detail in details}),
        source_count=source_count,
        subscriber_count=subscribers,
        unsubscribes=unsubscribes,
        unsubscribe_rate=rate,
        pressure_score=_pressure_score(unsubscribes, rate),
    )


def _content_type_attribution(
    content_type: str,
    details: list[NewsletterUnsubscribeSendDetail],
    source_count: int,
) -> NewsletterUnsubscribeContentTypeAttribution:
    subscribers = sum(max(detail.subscriber_count or 0, 0) for detail in details)
    unsubscribes = sum(detail.unsubscribes for detail in details)
    rate = _rate(unsubscribes, subscribers)
    return NewsletterUnsubscribeContentTypeAttribution(
        content_type=content_type,
        send_count=len({detail.newsletter_send_id for detail in details}),
        source_count=source_count,
        subscriber_count=subscribers,
        unsubscribes=unsubscribes,
        unsubscribe_rate=rate,
        pressure_score=_pressure_score(unsubscribes, rate),
    )


def _pressure_score(unsubscribes: int, unsubscribe_rate: float | None) -> float:
    return round(float(unsubscribes) + ((unsubscribe_rate or 0.0) * 100.0), 4)


def _totals(
    *,
    send_count: int,
    attributed_send_count: int,
    unsubscribe_count: int,
    warning_count: int,
) -> dict[str, int]:
    return {
        "attributed_send_count": attributed_send_count,
        "send_count": send_count,
        "unsubscribe_count": unsubscribe_count,
        "warning_count": warning_count,
    }


def _load_send_rows(
    conn: sqlite3.Connection,
    schema: Mapping[str, set[str]],
    *,
    cutoff: datetime,
    min_unsubscribes: int,
) -> list[dict[str, Any]]:
    status_filter = ""
    if "status" in schema["newsletter_sends"]:
        status_filter = "AND COALESCE(ns.status, 'sent') NOT IN ('draft', 'queued')"
    opens_expr = "le.opens" if "opens" in schema["newsletter_engagement"] else "NULL"
    clicks_expr = "le.clicks" if "clicks" in schema["newsletter_engagement"] else "NULL"
    query = f"""WITH latest_engagement AS (
                   SELECT ne.*
                   FROM newsletter_engagement ne
                   WHERE ne.id = (
                       SELECT latest.id
                       FROM newsletter_engagement latest
                       WHERE latest.newsletter_send_id = ne.newsletter_send_id
                          OR (
                              latest.newsletter_send_id IS NULL
                              AND ne.newsletter_send_id IS NULL
                              AND latest.issue_id = ne.issue_id
                          )
                       ORDER BY datetime(latest.fetched_at) DESC, latest.id DESC
                       LIMIT 1
                   )
               )
               SELECT ns.id AS newsletter_send_id,
                      ns.issue_id,
                      ns.subject,
                      ns.subscriber_count,
                      ns.source_content_ids,
                      ns.sent_at,
                      {opens_expr} AS opens,
                      {clicks_expr} AS clicks,
                      le.unsubscribes,
                      le.fetched_at
               FROM newsletter_sends ns
               INNER JOIN latest_engagement le
                 ON le.newsletter_send_id = ns.id
               WHERE datetime(ns.sent_at) >= datetime(?)
                 AND COALESCE(le.unsubscribes, 0) >= ?
                 {status_filter}
               ORDER BY
                 CASE WHEN ns.subscriber_count > 0
                   THEN CAST(le.unsubscribes AS REAL) / ns.subscriber_count
                   ELSE 0
                 END DESC,
                 le.unsubscribes DESC,
                 datetime(ns.sent_at) DESC,
                 ns.id DESC"""
    return [
        dict(row)
        for row in conn.execute(query, (cutoff.isoformat(), min_unsubscribes)).fetchall()
    ]


def _load_content(
    conn: sqlite3.Connection,
    schema: Mapping[str, set[str]],
    source_ids: list[int],
) -> dict[int, dict[str, Any]]:
    if not source_ids or "generated_content" not in schema:
        return {}
    if not {"id", "content_type"}.issubset(schema["generated_content"]):
        return {}
    placeholders = ",".join("?" for _ in source_ids)
    rows = conn.execute(
        f"""SELECT id, content_type
            FROM generated_content
            WHERE id IN ({placeholders})""",
        source_ids,
    ).fetchall()
    return {int(row["id"]): dict(row) for row in rows}


def _load_topics(
    conn: sqlite3.Connection,
    schema: Mapping[str, set[str]],
    source_ids: list[int],
) -> dict[int, tuple[str, ...]]:
    if not source_ids or "content_topics" not in schema:
        return {}
    if not {"content_id", "topic"}.issubset(schema["content_topics"]):
        return {}
    placeholders = ",".join("?" for _ in source_ids)
    rows = conn.execute(
        f"""SELECT content_id, topic
            FROM content_topics
            WHERE content_id IN ({placeholders})
            ORDER BY content_id ASC, topic ASC""",
        source_ids,
    ).fetchall()
    by_id: dict[int, list[str]] = defaultdict(list)
    for row in rows:
        topic = str(row["topic"] or "").strip()
        if topic:
            by_id[int(row["content_id"])].append(topic)
    return {content_id: tuple(sorted(set(topics))) for content_id, topics in by_id.items()}


def _parse_source_ids(raw_value: Any) -> tuple[list[int], tuple[str, ...]]:
    if raw_value in (None, ""):
        return [], (NO_SOURCE_CONTENT_IDS,)
    try:
        parsed = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    except (TypeError, json.JSONDecodeError):
        return [], (MALFORMED_SOURCE_CONTENT_IDS,)
    if not isinstance(parsed, list):
        return [], (MALFORMED_SOURCE_CONTENT_IDS,)

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
    warnings = (MALFORMED_SOURCE_CONTENT_IDS,) if malformed else ()
    if not source_ids:
        warnings = tuple(sorted({*warnings, NO_SOURCE_CONTENT_IDS}))
    return source_ids, warnings


def _schema_gaps(
    schema: Mapping[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "newsletter_sends": {
            "id",
            "issue_id",
            "subject",
            "subscriber_count",
            "source_content_ids",
            "sent_at",
        },
        "newsletter_engagement": {
            "id",
            "newsletter_send_id",
            "issue_id",
            "unsubscribes",
            "fetched_at",
        },
    }
    optional = {
        "generated_content": {"id", "content_type"},
        "content_topics": {"content_id", "topic"},
    }
    all_tables = {**required, **optional}
    missing_tables = tuple(table for table in all_tables if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in all_tables.items()
        if table in schema and columns - schema.get(table, set())
    }
    return missing_tables, missing_columns


def _blocking_schema_gaps(
    missing_tables: tuple[str, ...],
    missing_columns: Mapping[str, tuple[str, ...]],
) -> bool:
    required_tables = {"newsletter_sends", "newsletter_engagement"}
    return bool(
        required_tables.intersection(missing_tables)
        or any(table in required_tables for table in missing_columns)
    )


def _rate(numerator: int | None, denominator: int | None) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return round(numerator / denominator, 4)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _text_or_none(value: Any) -> str | None:
    return str(value) if value is not None else None


def _format_rate(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _format_items(values: tuple[Any, ...]) -> str:
    if not values:
        return "-"
    return ", ".join(str(value) for value in values)


def _normalize_topic(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    return normalized or None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        row[0]: {
            column[1]
            for column in conn.execute(f"PRAGMA table_info({row[0]})").fetchall()
        }
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
