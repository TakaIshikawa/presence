"""Select older resonated content that is safe to recirculate."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import math
import sqlite3
from typing import Any


DEFAULT_DAYS_OLD = 30
DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_LIMIT = 10
ENGAGEMENT_TABLES = {
    "x": "post_engagement",
    "bluesky": "bluesky_engagement",
    "linkedin": "linkedin_engagement",
    "mastodon": "mastodon_engagement",
}


@dataclass(frozen=True)
class RecirculationCandidate:
    """One read-only recommendation for giving old content a second life."""

    content_id: int
    content_type: str
    content_format: str | None
    content_preview: str
    published_at: str
    age_days: int
    score: float
    engagement_score: float
    engagement_by_platform: dict[str, float]
    score_components: dict[str, float]
    topics: tuple[str, ...]
    topic_last_published_at: str | None
    last_reused_at: str | None
    recommended_formats: tuple[str, ...]
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["topics"] = list(self.topics)
        data["recommended_formats"] = list(self.recommended_formats)
        data["reasons"] = list(self.reasons)
        return data


@dataclass(frozen=True)
class RecirculationReport:
    """Read-only report of recirculation opportunities."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    candidates: tuple[RecirculationCandidate, ...]
    missing_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "totals": dict(self.totals),
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "missing_tables": list(self.missing_tables),
        }


def build_content_recirculation_report(
    db_or_conn: Any,
    *,
    days_old: int = DEFAULT_DAYS_OLD,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> RecirculationReport:
    """Return ranked old published content candidates without creating variants."""
    if days_old <= 0:
        raise ValueError("days-old must be positive")
    if lookback_days <= 0:
        raise ValueError("lookback-days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    old_cutoff = generated_at - timedelta(days=days_old)
    reuse_cutoff = generated_at - timedelta(days=lookback_days)
    missing = tuple(
        table
        for table in (
            "generated_content",
            "content_publications",
            "content_variants",
            "content_topics",
        )
        if table not in schema
    )
    if "generated_content" in missing:
        return _empty_report(generated_at, days_old, lookback_days, limit, missing)

    topics_by_content = _load_topics(conn, schema)
    publication_dates = _load_publication_dates(conn, schema)
    engagement = _load_engagement(conn, schema)
    newsletter = _load_newsletter_signals(conn, schema)
    for content_id, signals in newsletter.items():
        if signals.get("engagement_score") is not None:
            engagement.setdefault(content_id, {})["newsletter"] = float(
                signals["engagement_score"]
            )
        if signals.get("sent_at"):
            publication_dates[content_id] = max(
                [publication_dates.get(content_id), _parse_datetime(signals["sent_at"])],
                key=lambda value: value or datetime.min.replace(tzinfo=timezone.utc),
            )

    rows = _load_content_rows(conn, schema)
    effective_publication_dates = dict(publication_dates)
    for row in rows:
        content_id = int(row["id"])
        legacy_published_at = _effective_legacy_published_at(row)
        if legacy_published_at and (
            content_id not in effective_publication_dates
            or legacy_published_at > effective_publication_dates[content_id]
        ):
            effective_publication_dates[content_id] = legacy_published_at
    variants = _load_variant_reuse(conn, schema)
    topic_dates = _topic_publication_dates(topics_by_content, effective_publication_dates)
    candidates: list[RecirculationCandidate] = []
    excluded_recent_publication = 0
    excluded_recent_reuse = 0
    excluded_unpublished = 0
    for row in rows:
        content_id = int(row["id"])
        published_at = publication_dates.get(content_id) or _effective_legacy_published_at(row)
        if not _is_published(row, content_id, publication_dates):
            excluded_unpublished += 1
            continue
        if published_at is None:
            excluded_unpublished += 1
            continue
        if published_at > old_cutoff:
            excluded_recent_publication += 1
            continue
        last_reused_at = variants.get(content_id)
        if last_reused_at and last_reused_at >= reuse_cutoff:
            excluded_recent_reuse += 1
            continue
        candidate = _build_candidate(
            row,
            published_at=published_at,
            now=generated_at,
            topics=topics_by_content.get(content_id, ()),
            all_topic_dates=topic_dates,
            engagement=engagement.get(content_id, {}),
            last_reused_at=last_reused_at,
        )
        candidates.append(candidate)

    ranked = tuple(
        sorted(
            candidates,
            key=lambda item: (
                -item.score,
                -item.engagement_score,
                -item.age_days,
                item.content_id,
            ),
        )[:limit]
    )
    return RecirculationReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days_old": days_old,
            "lookback_days": lookback_days,
            "limit": limit,
            "published_before": old_cutoff.isoformat(),
            "reuse_cutoff": reuse_cutoff.isoformat(),
        },
        totals={
            "eligible": len(candidates),
            "returned": len(ranked),
            "excluded_unpublished": excluded_unpublished,
            "excluded_recent_publication": excluded_recent_publication,
            "excluded_recent_reuse": excluded_recent_reuse,
            "missing_tables": len(missing),
        },
        candidates=ranked,
        missing_tables=missing,
    )


def format_content_recirculation_json(report: RecirculationReport) -> str:
    """Serialize a recirculation report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_content_recirculation_text(report: RecirculationReport) -> str:
    """Format recirculation candidates for terminal review."""
    lines = [
        "Content Recirculation Selector",
        f"Generated: {report.generated_at}",
        (
            f"Older than: {report.filters['days_old']} days "
            f"(before {report.filters['published_before']})"
        ),
        (
            "Summary: "
            f"eligible={report.totals['eligible']} "
            f"returned={report.totals['returned']} "
            f"recent_publication={report.totals['excluded_recent_publication']} "
            f"recent_reuse={report.totals['excluded_recent_reuse']} "
            f"unpublished={report.totals['excluded_unpublished']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if not report.candidates:
        lines.append("No recirculation candidates found.")
        return "\n".join(lines)

    lines.append("Candidates:")
    for item in report.candidates:
        formats = ", ".join(item.recommended_formats)
        topics = ", ".join(item.topics) if item.topics else "untagged"
        lines.append(
            f"- content_id={item.content_id} score={item.score:.2f} "
            f"engagement={item.engagement_score:.2f} age={item.age_days}d "
            f"formats={formats}"
        )
        lines.append(f"  topics: {topics}")
        lines.append(f"  reason: {'; '.join(item.reasons)}")
    return "\n".join(lines)


def _build_candidate(
    row: dict[str, Any],
    *,
    published_at: datetime,
    now: datetime,
    topics: tuple[str, ...],
    all_topic_dates: dict[str, list[tuple[int, datetime]]],
    engagement: dict[str, float],
    last_reused_at: datetime | None,
) -> RecirculationCandidate:
    content_id = int(row["id"])
    age_days = max(0, int((now - published_at).total_seconds() // 86400))
    engagement_score = round(sum(engagement.values()), 2)
    engagement_component = min(45.0, math.log1p(max(engagement_score, 0.0)) * 12.0)
    age_component = min(25.0, age_days / 4.0)
    freshness_component, topic_last = _topic_freshness(
        content_id,
        topics,
        all_topic_dates,
        now,
    )
    eval_component = min(10.0, max(float(row.get("eval_score") or 0.0), 0.0))
    fallback_component = 8.0 if not engagement else 0.0
    score = round(
        engagement_component
        + age_component
        + freshness_component
        + eval_component
        + fallback_component,
        2,
    )
    formats = _recommended_formats(row, engagement)
    reasons = _reasons(
        age_days=age_days,
        engagement=engagement,
        topics=topics,
        topic_last=topic_last,
        last_reused_at=last_reused_at,
        has_engagement=bool(engagement),
    )
    return RecirculationCandidate(
        content_id=content_id,
        content_type=str(row.get("content_type") or ""),
        content_format=row.get("content_format"),
        content_preview=_preview(row.get("content")),
        published_at=published_at.isoformat(),
        age_days=age_days,
        score=score,
        engagement_score=engagement_score,
        engagement_by_platform={key: round(value, 2) for key, value in sorted(engagement.items())},
        score_components={
            "engagement": round(engagement_component, 2),
            "age": round(age_component, 2),
            "topic_freshness": round(freshness_component, 2),
            "quality_fallback": round(eval_component + fallback_component, 2),
        },
        topics=topics,
        topic_last_published_at=topic_last.isoformat() if topic_last else None,
        last_reused_at=last_reused_at.isoformat() if last_reused_at else None,
        recommended_formats=formats,
        reasons=reasons,
    )


def _recommended_formats(
    row: dict[str, Any],
    engagement: dict[str, float],
) -> tuple[str, ...]:
    content_type = str(row.get("content_type") or "").lower()
    formats = ["variant"]
    if content_type != "x_thread":
        formats.append("thread")
    if "newsletter" not in content_type:
        formats.append("newsletter_section")
    if content_type != "blog_post":
        formats.append("blog_seed")
    if "bluesky" in engagement and "bluesky_post" not in formats:
        formats.insert(1, "bluesky_post")
    return tuple(dict.fromkeys(formats))


def _reasons(
    *,
    age_days: int,
    engagement: dict[str, float],
    topics: tuple[str, ...],
    topic_last: datetime | None,
    last_reused_at: datetime | None,
    has_engagement: bool,
) -> tuple[str, ...]:
    reasons = [f"published {age_days} days ago"]
    if has_engagement:
        top_platform, top_score = max(engagement.items(), key=lambda item: item[1])
        reasons.append(f"{top_platform} engagement score {top_score:.2f}")
    else:
        reasons.append("no engagement snapshots; ranked with quality and age fallback")
    if topics and topic_last:
        reasons.append(f"topic has been quiet since {topic_last.date().isoformat()}")
    elif topics:
        reasons.append("topic has no newer publication history")
    else:
        reasons.append("no topic tags; topic freshness treated as neutral")
    if last_reused_at:
        reasons.append(f"last reused on {last_reused_at.date().isoformat()}")
    else:
        reasons.append("no prior reuse variant found")
    return tuple(reasons)


def _topic_freshness(
    content_id: int,
    topics: tuple[str, ...],
    all_topic_dates: dict[str, list[tuple[int, datetime]]],
    now: datetime,
) -> tuple[float, datetime | None]:
    dates = [
        published_at
        for topic in topics
        for other_id, published_at in all_topic_dates.get(topic, [])
        if other_id != content_id
    ]
    if not topics:
        return 8.0, None
    if not dates:
        return 20.0, None
    latest = max(dates)
    quiet_days = max(0, int((now - latest).total_seconds() // 86400))
    return min(20.0, quiet_days / 3.0), latest


def _load_content_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    order = _order_columns(columns, [("published_at", "ASC"), ("created_at", "ASC"), ("id", "ASC")])
    rows = conn.execute(
        f"SELECT * FROM generated_content ORDER BY {', '.join(order)}"
    ).fetchall()
    return [_row_dict(row) for row in rows]


def _load_publication_dates(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, datetime]:
    if "content_publications" not in schema:
        return {}
    columns = schema["content_publications"]
    if not {"content_id", "published_at"}.issubset(columns):
        return {}
    status_filter = "AND status = 'published'" if "status" in columns else ""
    rows = conn.execute(
        f"""SELECT content_id, MAX(published_at) AS published_at
            FROM content_publications
            WHERE published_at IS NOT NULL {status_filter}
            GROUP BY content_id"""
    ).fetchall()
    dates = {}
    for row in rows:
        parsed = _parse_datetime(_value(row, "published_at", 1))
        if parsed:
            dates[int(_value(row, "content_id", 0))] = parsed
    return dates


def _load_topics(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, tuple[str, ...]]:
    if "content_topics" not in schema or not {"content_id", "topic"}.issubset(schema["content_topics"]):
        return {}
    rows = conn.execute(
        "SELECT content_id, topic FROM content_topics ORDER BY content_id ASC, topic ASC"
    ).fetchall()
    topics: dict[int, list[str]] = {}
    for row in rows:
        topic = str(_value(row, "topic", 1) or "").strip().lower()
        if topic:
            topics.setdefault(int(_value(row, "content_id", 0)), []).append(topic)
    return {content_id: tuple(dict.fromkeys(values)) for content_id, values in topics.items()}


def _load_engagement(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, dict[str, float]]:
    result: dict[int, dict[str, float]] = {}
    for platform, table in ENGAGEMENT_TABLES.items():
        if table not in schema:
            continue
        columns = schema[table]
        if not {"content_id", "engagement_score"}.issubset(columns):
            continue
        order = _order_columns(columns, [("fetched_at", "DESC"), ("created_at", "DESC"), ("id", "DESC")])
        rows = conn.execute(
            f"""SELECT content_id, engagement_score
                FROM {table}
                WHERE engagement_score IS NOT NULL
                ORDER BY content_id ASC, {', '.join(order)}"""
        ).fetchall()
        seen: set[int] = set()
        for row in rows:
            content_id = int(_value(row, "content_id", 0))
            if content_id in seen:
                continue
            seen.add(content_id)
            result.setdefault(content_id, {})[platform] = float(_value(row, "engagement_score", 1) or 0.0)
    return result


def _load_newsletter_signals(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, dict[str, Any]]:
    if "newsletter_sends" not in schema:
        return {}
    send_columns = schema["newsletter_sends"]
    if not {"id", "source_content_ids"}.issubset(send_columns):
        return {}
    metric_by_send = _newsletter_metrics(conn, schema)
    selected = [
        _column_expr(send_columns, "id"),
        _column_expr(send_columns, "source_content_ids"),
        _column_expr(send_columns, "subscriber_count"),
        _column_expr(send_columns, "sent_at"),
    ]
    rows = conn.execute(f"SELECT {', '.join(selected)} FROM newsletter_sends").fetchall()
    signals: dict[int, dict[str, Any]] = {}
    for row in rows:
        send = _row_dict(row)
        sent_at = _parse_datetime(send.get("sent_at"))
        metrics = metric_by_send.get(int(send["id"]), {})
        score = _newsletter_score(metrics, send.get("subscriber_count"))
        for content_id in _parse_content_ids(send.get("source_content_ids")):
            current = signals.setdefault(content_id, {})
            if sent_at and (
                not current.get("sent_at")
                or sent_at > _parse_datetime(current.get("sent_at"))
            ):
                current["sent_at"] = sent_at.isoformat()
            if score is not None:
                current["engagement_score"] = max(
                    float(current.get("engagement_score") or 0.0),
                    score,
                )
    return signals


def _newsletter_metrics(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, dict[str, Any]]:
    if "newsletter_engagement" not in schema:
        return {}
    columns = schema["newsletter_engagement"]
    if not {"newsletter_send_id", "opens", "clicks"}.issubset(columns):
        return {}
    order = _order_columns(columns, [("fetched_at", "DESC"), ("created_at", "DESC"), ("id", "DESC")])
    rows = conn.execute(
        f"""SELECT *
            FROM newsletter_engagement
            WHERE newsletter_send_id IS NOT NULL
            ORDER BY newsletter_send_id ASC, {', '.join(order)}"""
    ).fetchall()
    result: dict[int, dict[str, Any]] = {}
    for row in rows:
        data = _row_dict(row)
        send_id = int(data["newsletter_send_id"])
        result.setdefault(send_id, data)
    return result


def _newsletter_score(metrics: dict[str, Any], subscriber_count: Any) -> float | None:
    if not metrics:
        return None
    opens = float(metrics.get("opens") or 0)
    clicks = float(metrics.get("clicks") or 0)
    subscribers = float(subscriber_count or 0)
    if subscribers > 0:
        return round(((opens / subscribers) * 20.0) + ((clicks / subscribers) * 80.0), 2)
    raw = opens + (clicks * 3.0)
    return round(raw, 2) if raw else 0.0


def _load_variant_reuse(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, datetime]:
    if "content_variants" not in schema:
        return {}
    columns = schema["content_variants"]
    if not {"content_id", "created_at"}.issubset(columns):
        return {}
    rows = conn.execute(
        """SELECT content_id, MAX(created_at) AS last_reused_at
           FROM content_variants
           GROUP BY content_id"""
    ).fetchall()
    result = {}
    for row in rows:
        parsed = _parse_datetime(_value(row, "last_reused_at", 1))
        if parsed:
            result[int(_value(row, "content_id", 0))] = parsed
    return result


def _topic_publication_dates(
    topics_by_content: dict[int, tuple[str, ...]],
    publication_dates: dict[int, datetime],
) -> dict[str, list[tuple[int, datetime]]]:
    result: dict[str, list[tuple[int, datetime]]] = {}
    for content_id, topics in topics_by_content.items():
        published_at = publication_dates.get(content_id)
        if not published_at:
            continue
        for topic in topics:
            result.setdefault(topic, []).append((content_id, published_at))
    return result


def _is_published(
    row: dict[str, Any],
    content_id: int,
    publication_dates: dict[int, datetime],
) -> bool:
    if content_id in publication_dates:
        return True
    return row.get("published") in (1, "1", True) and bool(row.get("published_at"))


def _effective_legacy_published_at(row: dict[str, Any]) -> datetime | None:
    return _parse_datetime(row.get("published_at"))


def _empty_report(
    generated_at: datetime,
    days_old: int,
    lookback_days: int,
    limit: int,
    missing: tuple[str, ...],
) -> RecirculationReport:
    return RecirculationReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days_old": days_old,
            "lookback_days": lookback_days,
            "limit": limit,
            "published_before": (generated_at - timedelta(days=days_old)).isoformat(),
            "reuse_cutoff": (generated_at - timedelta(days=lookback_days)).isoformat(),
        },
        totals={
            "eligible": 0,
            "returned": 0,
            "excluded_unpublished": 0,
            "excluded_recent_publication": 0,
            "excluded_recent_reuse": 0,
            "missing_tables": len(missing),
        },
        candidates=(),
        missing_tables=missing,
    )


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    names = [str(_value(row, "name", 0)) for row in rows]
    return {
        name: {
            str(_value(column, "name", 1))
            for column in conn.execute(f"PRAGMA table_info({name})").fetchall()
        }
        for name in names
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _row_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "keys"):
        return {key: row[key] for key in row.keys()}
    return dict(row)


def _value(row: Any, key: str, index: int) -> Any:
    if hasattr(row, "keys") and key in row.keys():
        return row[key]
    return row[index]


def _column_expr(columns: set[str], name: str) -> str:
    return name if name in columns else f"NULL AS {name}"


def _order_columns(
    columns: set[str],
    requested: list[tuple[str, str]],
) -> list[str]:
    order = [f"{name} {direction}" for name, direction in requested if name in columns]
    return order or ["id ASC"]


def _parse_content_ids(value: Any) -> list[int]:
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(parsed, list):
        return []
    result = []
    for item in parsed:
        try:
            content_id = int(item)
        except (TypeError, ValueError):
            continue
        if content_id > 0:
            result.append(content_id)
    return result


def _preview(value: Any, limit: int = 140) -> str:
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "..."


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
