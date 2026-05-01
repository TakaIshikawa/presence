"""Score repeat inbound reply authors from review and outcome history."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_INTERACTIONS = 2
DEFAULT_LIMIT = 25
TIERS = ("trusted", "neutral", "noisy", "blocked_candidate")
ACCEPT_EVENT_TYPES = {"approved", "edited"}
POSTED_EVENT_TYPES = {"posted"}
DISMISS_EVENT_TYPES = {"rejected", "dismissed", "failed"}
EXPIRED_EVENT_TYPES = {"expired"}
ACCEPTED_STATUSES = {"approved"}
POSTED_STATUSES = {"posted"}
DISMISSED_STATUSES = {"dismissed", "rejected"}
PENDING_STATUSES = {"pending"}
PRIORITY_RANK = {"high": 0, "normal": 1, "low": 2}


def build_reply_author_reputation_report(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_interactions: int = DEFAULT_MIN_INTERACTIONS,
    platform: str | None = None,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return a stable reputation report for repeat inbound reply authors."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_interactions <= 0:
        raise ValueError("min_interactions must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")

    conn = _connection(db)
    now = _as_utc(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days)
    columns = _table_columns(conn, "reply_queue")
    if not columns:
        return _empty_report(days, min_interactions, platform, limit, now)

    rows = _reply_rows(conn, columns, cutoff=cutoff, platform=platform)
    events = _events_by_reply(conn, [row["id"] for row in rows])
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        identity = _author_identity(row)
        if identity is None:
            continue
        grouped[(row["platform"], identity["kind"], identity["value"])].append(row)

    authors = [
        _author_summary(key, matches, events)
        for key, matches in grouped.items()
        if len(matches) >= min_interactions
    ]
    authors.sort(key=_author_sort_key)
    if limit is not None:
        authors = authors[:limit]

    return {
        "generated_at": now.isoformat(),
        "filters": {
            "days": days,
            "min_interactions": min_interactions,
            "platform": platform,
            "limit": limit,
        },
        "totals": {
            "authors": len(authors),
            "interactions": sum(author["counts"]["total"] for author in authors),
        },
        "tier_counts": dict(sorted(Counter(author["tier"] for author in authors).items())),
        "authors": authors,
    }


def format_reply_author_reputation_json(report: dict[str, Any]) -> str:
    """Format a reputation report as stable JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_author_reputation_text(report: dict[str, Any]) -> str:
    """Format a reputation report for terminal review."""
    filters = report["filters"]
    lines = [
        "Reply author reputation",
        (
            f"Authors: {report['totals']['authors']} "
            f"interactions={report['totals']['interactions']}"
        ),
        (
            f"Lookback: {filters['days']}d "
            f"min_interactions={filters['min_interactions']} "
            f"platform={filters['platform'] or 'all'}"
        ),
        "",
    ]
    if not report["authors"]:
        lines.append("No reply authors matched.")
        return "\n".join(lines).rstrip()

    for author in report["authors"]:
        counts = author["counts"]
        rates = author["rates"]
        priorities = author["recent_priority_distribution"]
        label = _author_label(author)
        lines.append(
            f"{label} {author['tier']} score={author['score']} "
            f"total={counts['total']} last_seen_at={author['last_seen_at'] or 'unknown'}"
        )
        lines.append(
            "  counts: "
            f"accepted={counts['accepted']} posted={counts['posted']} "
            f"dismissed={counts['dismissed']} expired={counts['expired']} "
            f"pending={counts['pending']}"
        )
        lines.append(
            "  rates: "
            f"acceptance={rates['acceptance_rate']:.1%} "
            f"dismissal={rates['dismissal_rate']:.1%} "
            f"posted={rates['posted_rate']:.1%}"
        )
        lines.append(
            "  priorities: "
            + ", ".join(
                f"{priority}={priorities.get(priority, 0)}"
                for priority in ("high", "normal", "low")
            )
        )
    return "\n".join(lines).rstrip()


def normalize_author_value(value: Any) -> str | None:
    """Normalize author handle/name values for deterministic grouping."""
    normalized = str(value or "").strip().lstrip("@").casefold()
    return normalized or None


def _connection(db: Any) -> sqlite3.Connection:
    return db.conn if hasattr(db, "conn") else db


def _empty_report(
    days: int,
    min_interactions: int,
    platform: str | None,
    limit: int | None,
    now: datetime,
) -> dict[str, Any]:
    return {
        "generated_at": now.isoformat(),
        "filters": {
            "days": days,
            "min_interactions": min_interactions,
            "platform": platform,
            "limit": limit,
        },
        "totals": {"authors": 0, "interactions": 0},
        "tier_counts": {},
        "authors": [],
    }


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
    platform: str | None,
) -> list[dict[str, Any]]:
    select_columns = [
        _column_expr(columns, "id"),
        _column_expr(columns, "platform", "'x'"),
        _column_expr(columns, "inbound_author_handle"),
        _column_expr(columns, "inbound_author_name"),
        _column_expr(columns, "author_name"),
        _column_expr(columns, "display_name"),
        _column_expr(columns, "status", "'pending'"),
        _column_expr(columns, "priority", "'normal'"),
        _column_expr(columns, "detected_at"),
        _column_expr(columns, "reviewed_at"),
        _column_expr(columns, "posted_at"),
        _column_expr(columns, "posted_tweet_id"),
        _column_expr(columns, "posted_platform_id"),
    ]
    filters = []
    params: list[Any] = []
    if "detected_at" in columns:
        filters.append("(detected_at IS NULL OR datetime(detected_at) >= datetime(?))")
        params.append(cutoff.isoformat())
    if platform and "platform" in columns:
        filters.append("platform = ?")
        params.append(platform)

    query = f"SELECT {', '.join(select_columns)} FROM reply_queue"
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY " + _order_clause(columns)
    rows = [dict(row) for row in conn.execute(query, params).fetchall()]
    for row in rows:
        row["platform"] = str(row.get("platform") or "x")
        row["status"] = str(row.get("status") or "pending")
        row["priority"] = _normalize_priority(row.get("priority"))
    return rows


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    if column in columns:
        return column
    return f"{default} AS {column}"


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "detected_at" in columns:
        parts.append("datetime(detected_at) DESC")
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _events_by_reply(
    conn: sqlite3.Connection,
    reply_ids: list[int],
) -> dict[int, list[dict[str, Any]]]:
    if not reply_ids or not _table_columns(conn, "reply_review_events"):
        return {}
    placeholders = ",".join("?" for _ in reply_ids)
    rows = conn.execute(
        f"""SELECT reply_queue_id, event_type, old_status, new_status, created_at, id
              FROM reply_review_events
             WHERE reply_queue_id IN ({placeholders})
             ORDER BY reply_queue_id ASC, datetime(created_at) ASC, id ASC""",
        reply_ids,
    ).fetchall()
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[int(row["reply_queue_id"])].append(dict(row))
    return grouped


def _author_identity(row: dict[str, Any]) -> dict[str, str] | None:
    handle = normalize_author_value(row.get("inbound_author_handle"))
    if handle:
        return {"kind": "handle", "value": handle}
    for column in ("inbound_author_name", "author_name", "display_name"):
        name = normalize_author_value(row.get(column))
        if name:
            return {"kind": "name", "value": name}
    return None


def _author_summary(
    key: tuple[str, str, str],
    matches: list[dict[str, Any]],
    events_by_reply: dict[int, list[dict[str, Any]]],
) -> dict[str, Any]:
    platform, identity_kind, normalized_author = key
    outcomes = [
        _reply_outcome(row, events_by_reply.get(int(row["id"]), [])) for row in matches
    ]
    total = len(matches)
    counts = {
        "total": total,
        "accepted": sum(1 for outcome in outcomes if outcome["accepted"]),
        "posted": sum(1 for outcome in outcomes if outcome["posted"]),
        "dismissed": sum(1 for outcome in outcomes if outcome["dismissed"]),
        "expired": sum(1 for outcome in outcomes if outcome["expired"]),
        "pending": sum(1 for outcome in outcomes if outcome["pending"]),
    }
    rates = {
        "acceptance_rate": _rate(counts["accepted"] + counts["posted"], total),
        "dismissal_rate": _rate(counts["dismissed"], total),
        "posted_rate": _rate(counts["posted"], total),
        "expired_rate": _rate(counts["expired"], total),
        "pending_rate": _rate(counts["pending"], total),
    }
    priority_counts = dict(
        sorted(
            Counter(row["priority"] for row in matches).items(),
            key=lambda item: (PRIORITY_RANK.get(item[0], 9), item[0]),
        )
    )
    score = _score(counts, rates, priority_counts)
    return {
        "platform": platform,
        "identity_kind": identity_kind,
        "normalized_author": normalized_author,
        "raw_handles": _raw_values(matches, "inbound_author_handle"),
        "raw_names": _raw_names(matches),
        "tier": _tier(score, counts, rates),
        "score": score,
        "counts": counts,
        "rates": rates,
        "recent_priority_distribution": priority_counts,
        "last_seen_at": _latest_timestamp(
            *(
                timestamp
                for row in matches
                for timestamp in (
                    row.get("posted_at"),
                    row.get("reviewed_at"),
                    row.get("detected_at"),
                )
            )
        ),
    }


def _reply_outcome(row: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, bool]:
    event_types = {str(event.get("event_type") or "").casefold() for event in events}
    new_statuses = {str(event.get("new_status") or "").casefold() for event in events}
    status = str(row.get("status") or "pending").casefold()
    posted = (
        status in POSTED_STATUSES
        or bool(row.get("posted_tweet_id") or row.get("posted_platform_id"))
        or bool(POSTED_EVENT_TYPES & event_types)
        or "posted" in new_statuses
    )
    expired = bool(EXPIRED_EVENT_TYPES & event_types)
    dismissed = (
        status in DISMISSED_STATUSES
        or bool(DISMISS_EVENT_TYPES & event_types)
        or "dismissed" in new_statuses
    )
    accepted = (
        status in ACCEPTED_STATUSES
        or bool(ACCEPT_EVENT_TYPES & event_types)
        or "approved" in new_statuses
    )
    pending = status in PENDING_STATUSES and not (posted or accepted or dismissed or expired)
    return {
        "accepted": accepted,
        "posted": posted,
        "dismissed": dismissed,
        "expired": expired,
        "pending": pending,
    }


def _score(
    counts: dict[str, int],
    rates: dict[str, float],
    priority_counts: dict[str, int],
) -> int:
    total = counts["total"]
    high_rate = _rate(priority_counts.get("high", 0), total)
    volume_bonus = min(10.0, max(0, total - 1) * 2.0)
    score = (
        50.0
        + rates["posted_rate"] * 18.0
        + rates["acceptance_rate"] * 12.0
        + high_rate * 8.0
        + volume_bonus
        - rates["dismissal_rate"] * 30.0
        - rates["expired_rate"] * 25.0
        - rates["pending_rate"] * 5.0
    )
    return max(0, min(100, int(round(score))))


def _tier(score: int, counts: dict[str, int], rates: dict[str, float]) -> str:
    negative_rate = _rate(counts["dismissed"] + counts["expired"], counts["total"])
    if counts["total"] >= 3 and (score <= 20 or negative_rate >= 0.75):
        return "blocked_candidate"
    if score >= 70 and rates["acceptance_rate"] >= 0.5 and negative_rate < 0.35:
        return "trusted"
    if score <= 35 or rates["dismissal_rate"] >= 0.6:
        return "noisy"
    return "neutral"


def _author_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    tier_rank = {"trusted": 0, "neutral": 1, "noisy": 2, "blocked_candidate": 3}
    latest_dt = _parse_datetime(item.get("last_seen_at"))
    latest_ts = latest_dt.timestamp() if latest_dt else 0.0
    return (
        tier_rank.get(item["tier"], 9),
        -int(item["score"]),
        -int(item["counts"]["total"]),
        -latest_ts,
        item["platform"],
        item["normalized_author"],
    )


def _author_label(author: dict[str, Any]) -> str:
    prefix = "@" if author["identity_kind"] == "handle" else "name:"
    return f"{author['platform']} {prefix}{author['normalized_author']}"


def _raw_names(matches: list[dict[str, Any]]) -> list[str]:
    values = []
    for column in ("inbound_author_name", "author_name", "display_name"):
        values.extend(_raw_values(matches, column))
    return sorted(set(values), key=lambda value: value.casefold())


def _raw_values(matches: list[dict[str, Any]], column: str) -> list[str]:
    values = {
        str(match.get(column)).strip()
        for match in matches
        if str(match.get(column) or "").strip()
    }
    return sorted(values, key=lambda value: value.casefold())


def _normalize_priority(value: Any) -> str:
    priority = str(value or "normal").strip().casefold()
    return priority if priority in PRIORITY_RANK else "normal"


def _rate(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


def _latest_timestamp(*values: Any) -> str | None:
    parsed = [
        dt
        for value in values
        if value
        for dt in [_parse_datetime(value)]
        if dt is not None
    ]
    if not parsed:
        return None
    return max(parsed).isoformat()


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value)
    for parser in (
        lambda v: datetime.fromisoformat(v.replace("Z", "+00:00")),
        lambda v: datetime.strptime(v, "%Y-%m-%d %H:%M:%S"),
    ):
        try:
            parsed = parser(text)
        except ValueError:
            continue
        return _as_utc(parsed)
    return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
