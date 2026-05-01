"""Newsletter topic retention and engagement reporting."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_ISSUES = 12
DEFAULT_MIN_SENDS = 2


@dataclass(frozen=True)
class NewsletterTopicRetentionRow:
    """Aggregated retention and engagement for one recurring newsletter topic."""

    topic: str
    topic_kind: str
    issue_count: int
    sends: int
    opens: int | None
    clicks: int | None
    unsubscribes: int | None
    retention_delta: int | None
    open_rate: float | None
    click_rate: float | None
    unsubscribe_rate: float | None
    recommendation: str
    sample_status: str
    availability: dict[str, bool]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterTopicRetentionReport:
    """Read-only topic retention report for recent newsletter sends."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    rows: tuple[NewsletterTopicRetentionRow, ...]
    availability: dict[str, bool]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": self.filters,
            "totals": self.totals,
            "rows": [row.to_dict() for row in self.rows],
            "availability": dict(sorted(self.availability.items())),
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_newsletter_topic_retention_report(
    db_or_conn: Any,
    *,
    lookback_issues: int = DEFAULT_LOOKBACK_ISSUES,
    min_sends: int = DEFAULT_MIN_SENDS,
    now: datetime | None = None,
) -> NewsletterTopicRetentionReport:
    """Connect recent newsletter topics to subscriber and engagement momentum."""
    if lookback_issues <= 0:
        raise ValueError("lookback_issues must be positive")
    if min_sends <= 0:
        raise ValueError("min_sends must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: set[str] = set()
    missing_columns: dict[str, tuple[str, ...]] = {}

    sends = _recent_sends(
        conn,
        schema,
        lookback_issues,
        missing_tables,
        missing_columns,
    )
    send_ids = [int(send["id"]) for send in sends]
    content_ids = sorted(
        {
            content_id
            for send in sends
            for content_id in _parse_source_content_ids(send.get("source_content_ids"))
        }
    )
    topic_map, topic_kind, topic_available = _topic_map(
        conn,
        schema,
        content_ids,
        missing_tables,
        missing_columns,
    )
    engagement, engagement_available = _latest_engagement(
        conn,
        schema,
        send_ids,
        missing_tables,
        missing_columns,
    )
    link_clicks, link_clicks_available = _latest_link_clicks_by_topic(
        conn,
        schema,
        send_ids,
        topic_map,
        missing_tables,
        missing_columns,
    )
    retention, subscriber_available = _subscriber_deltas(
        conn,
        schema,
        sends,
        missing_tables,
        missing_columns,
    )

    availability = {
        "topics": topic_available,
        "engagement": engagement_available,
        "link_clicks": link_clicks_available,
        "subscriber_metrics": subscriber_available,
    }
    rows = _topic_rows(
        sends=sends,
        topic_map=topic_map,
        topic_kind=topic_kind,
        engagement=engagement,
        link_clicks=link_clicks,
        retention=retention,
        availability=availability,
        min_sends=min_sends,
    )

    return NewsletterTopicRetentionReport(
        generated_at=generated_at.isoformat(),
        filters={
            "lookback_issues": lookback_issues,
            "min_sends": min_sends,
        },
        totals={
            "issues_considered": len(sends),
            "topic_count": len(rows),
            "low_sample_count": sum(
                1 for row in rows if row.sample_status == "low_sample"
            ),
            "included_topic_count": sum(
                1 for row in rows if row.sample_status == "included"
            ),
        },
        rows=tuple(rows),
        availability=availability,
        missing_tables=tuple(sorted(missing_tables)),
        missing_columns=missing_columns,
    )


def format_newsletter_topic_retention_json(
    report: NewsletterTopicRetentionReport,
) -> str:
    """Serialize a topic retention report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_topic_retention_text(
    report: NewsletterTopicRetentionReport,
) -> str:
    """Format a topic retention report for terminal review."""
    lines = [
        "Newsletter Topic Retention",
        f"Generated: {report.generated_at}",
        f"Lookback issues: {report.filters['lookback_issues']}",
        f"Minimum sends: {report.filters['min_sends']}",
        (
            f"Topics: {report.totals['topic_count']} "
            f"(included={report.totals['included_topic_count']}, "
            f"low_sample={report.totals['low_sample_count']})"
        ),
        "Availability: "
        + ", ".join(
            f"{name}={'yes' if value else 'no'}"
            for name, value in sorted(report.availability.items())
        ),
    ]
    if report.missing_tables:
        lines.append("Missing optional tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        details = ", ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + details)
    if not report.rows:
        lines.append("No newsletter topics found for the selected lookback.")
        return "\n".join(lines)

    lines.append("Topic rows:")
    for row in report.rows:
        lines.append(
            f"- {row.topic} ({row.topic_kind}): issues={row.issue_count} "
            f"sends={row.sends} opens={_format_optional_int(row.opens)} "
            f"clicks={_format_optional_int(row.clicks)} "
            f"unsubscribes={_format_optional_int(row.unsubscribes)} "
            f"retention_delta={_format_optional_signed(row.retention_delta)} "
            f"open_rate={_format_rate(row.open_rate)} "
            f"click_rate={_format_rate(row.click_rate)} "
            f"recommendation={row.recommendation} sample={row.sample_status}"
        )
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _recent_sends(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    lookback_issues: int,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> list[dict[str, Any]]:
    if "newsletter_sends" not in schema:
        missing_tables.add("newsletter_sends")
        return []
    required = ("id", "sent_at")
    missing = tuple(
        column for column in required if column not in schema["newsletter_sends"]
    )
    if missing:
        missing_columns["newsletter_sends"] = missing
        return []

    optional = [
        column
        for column in ("issue_id", "subject", "source_content_ids", "subscriber_count")
        if column in schema["newsletter_sends"]
    ]
    selected = ["id", "sent_at", *optional]
    return _fetch_dicts(
        conn,
        f"""SELECT {', '.join(selected)}
            FROM newsletter_sends
            ORDER BY datetime(sent_at) DESC, id DESC
            LIMIT ?""",
        (lookback_issues,),
    )


def _topic_map(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: list[int],
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> tuple[dict[int, set[str]], str, bool]:
    if not content_ids:
        return {}, "topic", False

    if "content_topics" in schema:
        required = ("content_id", "topic")
        missing = tuple(
            column for column in required if column not in schema["content_topics"]
        )
        if missing:
            missing_columns["content_topics"] = missing
        else:
            placeholders = ",".join("?" for _ in content_ids)
            rows = _fetch_dicts(
                conn,
                f"""SELECT content_id, topic
                    FROM content_topics
                    WHERE content_id IN ({placeholders})
                    ORDER BY topic ASC, content_id ASC""",
                content_ids,
            )
            mapped: dict[int, set[str]] = {}
            for row in rows:
                topic = _normalize_label(row.get("topic"))
                if topic:
                    mapped.setdefault(int(row["content_id"]), set()).add(topic)
            if mapped:
                return mapped, "topic", True
    else:
        missing_tables.add("content_topics")

    if "generated_content" not in schema:
        missing_tables.add("generated_content")
        return {}, "topic", False
    required = ("id", "content_type")
    missing = tuple(
        column for column in required if column not in schema["generated_content"]
    )
    if missing:
        missing_columns["generated_content"] = missing
        return {}, "topic", False

    placeholders = ",".join("?" for _ in content_ids)
    rows = _fetch_dicts(
        conn,
        f"""SELECT id, content_type
            FROM generated_content
            WHERE id IN ({placeholders})
            ORDER BY content_type ASC, id ASC""",
        content_ids,
    )
    mapped = {
        int(row["id"]): {_section_label(row.get("content_type"))}
        for row in rows
        if _section_label(row.get("content_type"))
    }
    return mapped, "section", bool(mapped)


def _latest_engagement(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    send_ids: list[int],
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> tuple[dict[int, dict[str, int]], bool]:
    if not send_ids:
        return {}, False
    if "newsletter_engagement" not in schema:
        missing_tables.add("newsletter_engagement")
        return {}, False
    required = (
        "id",
        "newsletter_send_id",
        "opens",
        "clicks",
        "unsubscribes",
        "fetched_at",
    )
    missing = tuple(
        column for column in required if column not in schema["newsletter_engagement"]
    )
    if missing:
        missing_columns["newsletter_engagement"] = missing
        return {}, False

    placeholders = ",".join("?" for _ in send_ids)
    rows = _fetch_dicts(
        conn,
        f"""SELECT ne.newsletter_send_id, ne.opens, ne.clicks, ne.unsubscribes
            FROM newsletter_engagement ne
            WHERE ne.newsletter_send_id IN ({placeholders})
              AND ne.id = (
                  SELECT latest.id
                  FROM newsletter_engagement latest
                  WHERE latest.newsletter_send_id = ne.newsletter_send_id
                  ORDER BY datetime(latest.fetched_at) DESC, latest.id DESC
                  LIMIT 1
              )
            ORDER BY ne.newsletter_send_id ASC""",
        send_ids,
    )
    return {
        int(row["newsletter_send_id"]): {
            "opens": _int(row.get("opens")),
            "clicks": _int(row.get("clicks")),
            "unsubscribes": _int(row.get("unsubscribes")),
        }
        for row in rows
    }, True


def _latest_link_clicks_by_topic(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    send_ids: list[int],
    topic_map: dict[int, set[str]],
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> tuple[dict[tuple[int, str], int], bool]:
    if not send_ids:
        return {}, False
    if "newsletter_link_clicks" not in schema:
        missing_tables.add("newsletter_link_clicks")
        return {}, False
    required = (
        "id",
        "newsletter_send_id",
        "content_id",
        "link_url",
        "clicks",
        "fetched_at",
    )
    missing = tuple(
        column for column in required if column not in schema["newsletter_link_clicks"]
    )
    if missing:
        missing_columns["newsletter_link_clicks"] = missing
        return {}, False

    placeholders = ",".join("?" for _ in send_ids)
    rows = _fetch_dicts(
        conn,
        f"""SELECT nlc.newsletter_send_id, nlc.content_id, nlc.link_url, nlc.clicks
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
            ORDER BY nlc.newsletter_send_id ASC, nlc.link_url ASC""",
        send_ids,
    )
    clicks: dict[tuple[int, str], int] = {}
    for row in rows:
        content_id = row.get("content_id")
        if content_id is None:
            continue
        for topic in topic_map.get(int(content_id), set()):
            key = (int(row["newsletter_send_id"]), topic)
            clicks[key] = clicks.get(key, 0) + _int(row.get("clicks"))
    return clicks, True


def _subscriber_deltas(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    sends: list[dict[str, Any]],
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> tuple[dict[int, int | None], bool]:
    if not sends:
        return {}, False
    if "newsletter_subscriber_metrics" not in schema:
        missing_tables.add("newsletter_subscriber_metrics")
        return {int(send["id"]): None for send in sends}, False
    required = ("id", "subscriber_count", "fetched_at")
    missing = tuple(
        column
        for column in required
        if column not in schema["newsletter_subscriber_metrics"]
    )
    if missing:
        missing_columns["newsletter_subscriber_metrics"] = missing
        return {int(send["id"]): None for send in sends}, False

    rows = _fetch_dicts(
        conn,
        """SELECT id, subscriber_count, fetched_at
           FROM newsletter_subscriber_metrics
           ORDER BY datetime(fetched_at) ASC, id ASC""",
        (),
    )
    snapshots = [
        {
            "subscriber_count": _int(row.get("subscriber_count")),
            "fetched_at": parsed,
        }
        for row in rows
        if (parsed := _parse_timestamp(row.get("fetched_at"))) is not None
    ]
    deltas: dict[int, int | None] = {}
    for send in sends:
        sent_at = _parse_timestamp(send.get("sent_at"))
        if sent_at is None:
            deltas[int(send["id"])] = None
            continue
        before = [row for row in snapshots if row["fetched_at"] <= sent_at]
        after = [row for row in snapshots if row["fetched_at"] > sent_at]
        if not before or not after:
            deltas[int(send["id"])] = None
        else:
            deltas[int(send["id"])] = (
                after[0]["subscriber_count"] - before[-1]["subscriber_count"]
            )
    return deltas, True


def _topic_rows(
    *,
    sends: list[dict[str, Any]],
    topic_map: dict[int, set[str]],
    topic_kind: str,
    engagement: dict[int, dict[str, int]],
    link_clicks: dict[tuple[int, str], int],
    retention: dict[int, int | None],
    availability: dict[str, bool],
    min_sends: int,
) -> list[NewsletterTopicRetentionRow]:
    totals: dict[str, dict[str, Any]] = {}
    for send in sends:
        send_id = int(send["id"])
        topics = _topics_for_send(send, topic_map)
        if not topics:
            topics = {"unknown"}
        for topic in topics:
            total = totals.setdefault(
                topic,
                {
                    "issue_ids": set(),
                    "sends": 0,
                    "opens": 0,
                    "clicks": 0,
                    "unsubscribes": 0,
                    "retention_delta": 0,
                    "has_retention": False,
                },
            )
            total["issue_ids"].add(send_id)
            total["sends"] += _int(send.get("subscriber_count"))
            if availability["engagement"] and send_id in engagement:
                total["opens"] += engagement[send_id]["opens"]
                total["unsubscribes"] += engagement[send_id]["unsubscribes"]
            if availability["link_clicks"]:
                total["clicks"] += link_clicks.get((send_id, topic), 0)
            elif availability["engagement"] and send_id in engagement:
                total["clicks"] += engagement[send_id]["clicks"]
            if retention.get(send_id) is not None:
                total["retention_delta"] += int(retention[send_id] or 0)
                total["has_retention"] = True

    rows: list[NewsletterTopicRetentionRow] = []
    for topic, total in totals.items():
        issue_count = len(total["issue_ids"])
        sends = int(total["sends"])
        opens = int(total["opens"]) if availability["engagement"] else None
        clicks = (
            int(total["clicks"])
            if availability["link_clicks"] or availability["engagement"]
            else None
        )
        unsubscribes = int(total["unsubscribes"]) if availability["engagement"] else None
        retention_delta = (
            int(total["retention_delta"])
            if availability["subscriber_metrics"] and total["has_retention"]
            else None
        )
        sample_status = "included" if issue_count >= min_sends else "low_sample"
        open_rate = _rate(opens, sends)
        click_rate = _rate(clicks, sends)
        unsubscribe_rate = _rate(unsubscribes, sends)
        rows.append(
            NewsletterTopicRetentionRow(
                topic=topic,
                topic_kind=topic_kind if topic != "unknown" else "unknown",
                issue_count=issue_count,
                sends=sends,
                opens=opens,
                clicks=clicks,
                unsubscribes=unsubscribes,
                retention_delta=retention_delta,
                open_rate=open_rate,
                click_rate=click_rate,
                unsubscribe_rate=unsubscribe_rate,
                recommendation=_recommendation(
                    sample_status=sample_status,
                    retention_delta=retention_delta,
                    click_rate=click_rate,
                    open_rate=open_rate,
                    unsubscribe_rate=unsubscribe_rate,
                ),
                sample_status=sample_status,
                availability=dict(availability),
            )
        )

    return sorted(
        rows,
        key=lambda row: (
            _sort_desc_optional(row.retention_delta),
            _sort_desc_optional(row.click_rate),
            _sort_desc_optional(row.open_rate),
            -row.issue_count,
            row.topic,
        ),
    )


def _topics_for_send(
    send: dict[str, Any],
    topic_map: dict[int, set[str]],
) -> set[str]:
    topics: set[str] = set()
    for content_id in _parse_source_content_ids(send.get("source_content_ids")):
        topics.update(topic_map.get(content_id, set()))
    return topics


def _parse_source_content_ids(raw_value: Any) -> list[int]:
    if raw_value in (None, ""):
        return []
    try:
        parsed = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(parsed, list):
        return []
    ids: list[int] = []
    for item in parsed:
        try:
            content_id = int(item)
        except (TypeError, ValueError):
            continue
        if content_id > 0:
            ids.append(content_id)
    return ids


def _fetch_dicts(
    conn: sqlite3.Connection,
    query: str,
    params: list[Any] | tuple[Any, ...],
) -> list[dict[str, Any]]:
    cursor = conn.execute(query, params)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _normalize_label(value: Any) -> str:
    return str(value or "").strip().lower()


def _section_label(value: Any) -> str:
    normalized = _normalize_label(value)
    if not normalized:
        return ""
    return normalized.replace("_", "-")


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _rate(numerator: int | None, denominator: int) -> float | None:
    if numerator is None or denominator <= 0:
        return None
    return round(numerator / denominator, 6)


def _sort_desc_optional(value: int | float | None) -> tuple[int, int | float]:
    if value is None:
        return (1, 0)
    return (0, -value)


def _recommendation(
    *,
    sample_status: str,
    retention_delta: int | None,
    click_rate: float | None,
    open_rate: float | None,
    unsubscribe_rate: float | None,
) -> str:
    if sample_status == "low_sample":
        return "collect_more_data"
    if retention_delta is not None and retention_delta < 0:
        return "monitor_churn"
    if unsubscribe_rate is not None and unsubscribe_rate >= 0.01:
        return "monitor_churn"
    if click_rate is not None and click_rate < 0.01:
        return "improve_engagement"
    if open_rate is not None and open_rate < 0.2:
        return "improve_engagement"
    if retention_delta is not None and retention_delta > 0:
        return "grow_topic"
    return "maintain"


def _format_optional_int(value: int | None) -> str:
    return "n/a" if value is None else str(value)


def _format_optional_signed(value: int | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:+d}"


def _format_rate(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.2f}%"
