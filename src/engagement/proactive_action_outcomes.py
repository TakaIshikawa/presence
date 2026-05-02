"""Export proactive action outcomes for review."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Sequence


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
PROACTIVE_COLUMNS = (
    "id",
    "action_type",
    "target_tweet_id",
    "target_tweet_text",
    "target_author_handle",
    "target_author_id",
    "discovery_source",
    "relevance_score",
    "draft_text",
    "status",
    "relationship_context",
    "knowledge_ids",
    "platform_metadata",
    "posted_tweet_id",
    "created_at",
    "reviewed_at",
    "posted_at",
)
REPLY_COLUMNS = (
    "id",
    "status",
    "platform",
    "inbound_tweet_id",
    "inbound_author_handle",
    "inbound_author_id",
    "inbound_url",
    "inbound_cid",
    "our_tweet_id",
    "our_platform_id",
    "our_content_id",
    "draft_text",
    "detected_at",
    "reviewed_at",
    "posted_at",
    "posted_tweet_id",
    "posted_platform_id",
)


@dataclass(frozen=True)
class LinkedReplyDraft:
    """A reply_queue row related to a proactive action."""

    id: int
    status: str
    platform: str | None
    inbound_id: str | None
    inbound_url: str | None
    inbound_author_handle: str | None
    inbound_author_id: str | None
    our_content_id: int | None
    detected_at: str | None
    reviewed_at: str | None
    posted_at: str | None
    posted_id: str | None
    has_draft: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status,
            "platform": self.platform,
            "inbound_id": self.inbound_id,
            "inbound_url": self.inbound_url,
            "inbound_author_handle": self.inbound_author_handle,
            "inbound_author_id": self.inbound_author_id,
            "our_content_id": self.our_content_id,
            "detected_at": self.detected_at,
            "reviewed_at": self.reviewed_at,
            "posted_at": self.posted_at,
            "posted_id": self.posted_id,
            "has_draft": self.has_draft,
        }


@dataclass(frozen=True)
class ProactiveActionOutcome:
    """Outcome packet for one proactive action."""

    id: int
    action_type: str
    status: str
    raw_status: str
    age_hours: float | None
    created_at: str | None
    resolved_at: str | None
    reviewed_at: str | None
    posted_at: str | None
    target: dict[str, Any]
    target_metadata: dict[str, Any]
    publication_resulted: bool
    reply_draft_resulted: bool
    linked_reply_counts: dict[str, int]
    linked_replies: tuple[LinkedReplyDraft, ...]
    recommended_next_step: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "action_type": self.action_type,
            "status": self.status,
            "raw_status": self.raw_status,
            "age_hours": self.age_hours,
            "created_at": self.created_at,
            "resolved_at": self.resolved_at,
            "reviewed_at": self.reviewed_at,
            "posted_at": self.posted_at,
            "target": self.target,
            "target_metadata": self.target_metadata,
            "publication_resulted": self.publication_resulted,
            "reply_draft_resulted": self.reply_draft_resulted,
            "linked_draft_counts": dict(sorted(self.linked_reply_counts.items())),
            "linked_reply_counts": dict(sorted(self.linked_reply_counts.items())),
            "linked_replies": [reply.to_dict() for reply in self.linked_replies],
            "recommended_next_step": self.recommended_next_step,
        }


@dataclass(frozen=True)
class ProactiveActionOutcomeReport:
    """Review packet for proactive action outcomes."""

    generated_at: str
    filters: dict[str, Any]
    total_actions: int
    summary: dict[str, Any]
    actions: tuple[ProactiveActionOutcome, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "proactive_action_outcomes",
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "total_actions": self.total_actions,
            "summary": self.summary,
            "actions": [action.to_dict() for action in self.actions],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_proactive_action_outcome_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    statuses: str | Sequence[str] | None = None,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ProactiveActionOutcomeReport:
    """Build a deterministic, read-only proactive action outcome report."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")

    conn = _connection(db_or_conn)
    now = _as_utc(now or datetime.now(timezone.utc))
    wanted_statuses = _normalise_statuses(statuses)
    filters = {
        "days": days,
        "status": list(wanted_statuses),
        "limit": limit,
    }

    schema = _schema(conn)
    if "proactive_actions" not in schema:
        return _empty_report(now, filters, missing_tables=("proactive_actions",))
    missing = tuple(column for column in PROACTIVE_COLUMNS if column not in schema["proactive_actions"])
    if missing:
        return _empty_report(now, filters, missing_columns={"proactive_actions": missing})

    reply_columns = schema.get("reply_queue", set())
    missing_columns: dict[str, tuple[str, ...]] = {}
    if reply_columns:
        reply_missing = tuple(column for column in REPLY_COLUMNS if column not in reply_columns)
        if reply_missing:
            missing_columns["reply_queue"] = reply_missing

    rows = _action_rows(conn, days=days, now=now)
    linked = _linked_replies(conn, rows, reply_columns) if reply_columns and not missing_columns else {}
    matched_actions = tuple(
        action
        for action in (_outcome_for_row(row, linked.get(int(row["id"]), ()), now) for row in rows)
        if _status_matches(action, wanted_statuses)
    )
    actions = matched_actions[:limit] if limit is not None else matched_actions
    return ProactiveActionOutcomeReport(
        generated_at=now.isoformat(),
        filters=filters,
        total_actions=len(actions),
        summary=_summary(actions),
        actions=actions,
        missing_columns=missing_columns or None,
    )


def format_proactive_action_outcomes_json(report: ProactiveActionOutcomeReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_proactive_action_outcomes_text(report: ProactiveActionOutcomeReport) -> str:
    """Render a compact human-readable proactive outcome packet."""
    lines = [
        "Proactive Action Outcomes",
        (
            "Filters: "
            f"days={report.filters.get('days')} "
            f"status={_display_status_filter(report.filters.get('status'))} "
            f"limit={report.filters.get('limit')}"
        ),
        f"Actions: {report.total_actions}",
    ]
    summary_rows = report.summary.get("by_action_type_status", [])
    if summary_rows:
        lines.append("Summary:")
        for row in summary_rows:
            lines.append(
                f"  - {row['action_type']} status={row['status']} count={row['count']}"
            )
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        lines.append(
            "Missing columns: "
            + ", ".join(
                f"{table}.{column}"
                for table, columns in sorted(report.missing_columns.items())
                for column in columns
            )
        )
    if not report.actions:
        lines.append("No proactive actions matched.")
        return "\n".join(lines)

    lines.append("Items:")
    for action in report.actions:
        target = action.target
        handle = target.get("author_handle") or "?"
        target_id = target.get("tweet_id") or "?"
        age = "n/a" if action.age_hours is None else f"{action.age_hours:.1f}h"
        lines.append(
            f"  - #{action.id} {action.action_type} @{handle} "
            f"target={target_id} status={action.status} raw={action.raw_status} age={age}"
        )
        lines.append(
            "    "
            f"created={action.created_at or 'n/a'} resolved={action.resolved_at or 'n/a'} "
            f"publication={str(action.publication_resulted).lower()} "
            f"reply_draft={str(action.reply_draft_resulted).lower()} "
            f"linked_replies={action.linked_reply_counts.get('total', 0)}"
        )
        lines.append(f"    next={action.recommended_next_step}")
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _empty_report(
    now: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> ProactiveActionOutcomeReport:
    return ProactiveActionOutcomeReport(
        generated_at=now.isoformat(),
        filters=filters,
        total_actions=0,
        summary=_summary(()),
        actions=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _action_rows(
    conn: sqlite3.Connection,
    *,
    days: int,
    now: datetime,
) -> list[dict[str, Any]]:
    cutoff = (now - timedelta(days=days)).isoformat()
    cursor = conn.execute(
        f"""SELECT {', '.join(PROACTIVE_COLUMNS)}
            FROM proactive_actions
            WHERE created_at IS NULL OR datetime(created_at) >= datetime(?)
            ORDER BY datetime(COALESCE(posted_at, reviewed_at, created_at)) DESC,
                     datetime(created_at) DESC,
                     id ASC""",
        [cutoff],
    )
    return [dict(row) for row in cursor.fetchall()]


def _linked_replies(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    reply_columns: set[str],
) -> dict[int, tuple[LinkedReplyDraft, ...]]:
    if not rows or not set(REPLY_COLUMNS).issubset(reply_columns):
        return {}

    linked: dict[int, list[LinkedReplyDraft]] = defaultdict(list)
    for row in rows:
        action_id = int(row["id"])
        ids = _link_ids(row)
        if not ids:
            continue
        placeholders = ", ".join("?" for _ in ids)
        cursor = conn.execute(
            f"""SELECT {', '.join(REPLY_COLUMNS)}
                FROM reply_queue
                WHERE inbound_tweet_id IN ({placeholders})
                   OR our_tweet_id IN ({placeholders})
                   OR our_platform_id IN ({placeholders})
                   OR posted_tweet_id IN ({placeholders})
                   OR posted_platform_id IN ({placeholders})
                ORDER BY datetime(detected_at) ASC, id ASC""",
            [*ids, *ids, *ids, *ids, *ids],
        )
        seen: set[int] = set()
        for reply_row in cursor.fetchall():
            reply = _linked_reply(dict(reply_row))
            if reply.id in seen:
                continue
            linked[action_id].append(reply)
            seen.add(reply.id)
    return {action_id: tuple(items) for action_id, items in linked.items()}


def _link_ids(row: dict[str, Any]) -> list[str]:
    metadata = _parse_json_object(row.get("platform_metadata"))
    values = [
        row.get("target_tweet_id"),
        row.get("posted_tweet_id"),
        metadata.get("target_tweet_id"),
        metadata.get("target_uri"),
        metadata.get("target_url"),
        metadata.get("posted_tweet_id"),
        metadata.get("posted_platform_id"),
    ]
    seen: set[str] = set()
    ids: list[str] = []
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text and text not in seen:
            ids.append(text)
            seen.add(text)
    return ids


def _linked_reply(row: dict[str, Any]) -> LinkedReplyDraft:
    return LinkedReplyDraft(
        id=int(row["id"]),
        status=_value(row.get("status"), "unknown"),
        platform=row.get("platform"),
        inbound_id=row.get("inbound_tweet_id") or row.get("inbound_cid"),
        inbound_url=row.get("inbound_url"),
        inbound_author_handle=row.get("inbound_author_handle"),
        inbound_author_id=row.get("inbound_author_id"),
        our_content_id=_int_or_none(row.get("our_content_id")),
        detected_at=row.get("detected_at"),
        reviewed_at=row.get("reviewed_at"),
        posted_at=row.get("posted_at"),
        posted_id=row.get("posted_platform_id") or row.get("posted_tweet_id"),
        has_draft=bool((row.get("draft_text") or "").strip()),
    )


def _outcome_for_row(
    row: dict[str, Any],
    replies: tuple[LinkedReplyDraft, ...],
    now: datetime,
) -> ProactiveActionOutcome:
    raw_status = _value(row.get("status"), "unknown")
    publication_resulted = _publication_resulted(row, replies)
    reply_draft_resulted = any(reply.has_draft for reply in replies)
    status = _normalised_outcome_status(raw_status, publication_resulted, reply_draft_resulted)
    created_at = row.get("created_at")
    reviewed_at = row.get("reviewed_at")
    posted_at = row.get("posted_at")
    resolved_at = posted_at or reviewed_at
    return ProactiveActionOutcome(
        id=int(row["id"]),
        action_type=_value(row.get("action_type"), "unknown"),
        status=status,
        raw_status=raw_status,
        age_hours=_age_hours(created_at, now),
        created_at=created_at,
        resolved_at=resolved_at,
        reviewed_at=reviewed_at,
        posted_at=posted_at,
        target={
            "tweet_id": row.get("target_tweet_id"),
            "text": row.get("target_tweet_text"),
            "author_handle": row.get("target_author_handle"),
            "author_id": row.get("target_author_id"),
            "discovery_source": row.get("discovery_source"),
            "relevance_score": row.get("relevance_score"),
        },
        target_metadata=_target_metadata(row),
        publication_resulted=publication_resulted,
        reply_draft_resulted=reply_draft_resulted,
        linked_reply_counts=_reply_counts(replies),
        linked_replies=replies,
        recommended_next_step=_recommended_next_step(status, publication_resulted, reply_draft_resulted, replies),
    )


def _target_metadata(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _parse_json_object(row.get("platform_metadata"))
    return {
        "platform": metadata.get("platform") or metadata.get("source_platform") or "x",
        "url": metadata.get("target_url") or metadata.get("url"),
        "cid": metadata.get("target_cid") or metadata.get("cid"),
        "posted_id": row.get("posted_tweet_id") or metadata.get("posted_platform_id"),
        "relationship_context_present": bool(row.get("relationship_context")),
        "knowledge_link_count": len(_parse_json_list(row.get("knowledge_ids"))),
        "platform_metadata": metadata,
    }


def _publication_resulted(row: dict[str, Any], replies: tuple[LinkedReplyDraft, ...]) -> bool:
    if row.get("posted_at") or row.get("posted_tweet_id"):
        return True
    return any(reply.posted_at or reply.posted_id or reply.status == "posted" for reply in replies)


def _normalised_outcome_status(
    raw_status: str,
    publication_resulted: bool,
    reply_draft_resulted: bool,
) -> str:
    raw = raw_status.lower()
    if publication_resulted or raw in {"posted", "completed", "published", "sent"}:
        return "completed"
    if raw in {"expired", "dismissed", "rejected"}:
        return "expired"
    if raw == "pending":
        return "pending"
    if raw == "approved" and not publication_resulted:
        return "unresolved"
    if reply_draft_resulted:
        return "unresolved"
    return "unresolved"


def _reply_counts(replies: tuple[LinkedReplyDraft, ...]) -> dict[str, int]:
    counts = Counter(reply.status for reply in replies)
    return {"total": len(replies), **dict(sorted(counts.items()))}


def _recommended_next_step(
    status: str,
    publication_resulted: bool,
    reply_draft_resulted: bool,
    replies: tuple[LinkedReplyDraft, ...],
) -> str:
    if status == "completed" and replies:
        return "Review linked replies and capture outcome signals for future targeting."
    if status == "completed":
        return "Record publication performance once engagement metrics are available."
    if status == "pending":
        return "Review or dismiss the pending proactive action."
    if status == "expired":
        return "Archive the expired action and reuse only if the target context is still fresh."
    if reply_draft_resulted and not publication_resulted:
        return "Resolve the linked reply draft before treating the action as complete."
    return "Investigate why the action has no publication or linked reply draft outcome."


def _summary(actions: Sequence[ProactiveActionOutcome]) -> dict[str, Any]:
    by_pair: Counter[tuple[str, str]] = Counter((action.action_type, action.status) for action in actions)
    by_action_type: Counter[str] = Counter(action.action_type for action in actions)
    by_status: Counter[str] = Counter(action.status for action in actions)
    return {
        "by_action_type": dict(sorted(by_action_type.items())),
        "by_status": dict(sorted(by_status.items())),
        "by_action_type_status": [
            {"action_type": action_type, "status": status, "count": count}
            for (action_type, status), count in sorted(by_pair.items())
        ],
        "publication_resulted": sum(1 for action in actions if action.publication_resulted),
        "reply_draft_resulted": sum(1 for action in actions if action.reply_draft_resulted),
        "linked_reply_count": sum(action.linked_reply_counts.get("total", 0) for action in actions),
    }


def _status_matches(
    action: ProactiveActionOutcome,
    statuses: tuple[str, ...],
) -> bool:
    if not statuses:
        return True
    return action.status in statuses or action.raw_status in statuses


def _normalise_statuses(statuses: str | Sequence[str] | None) -> tuple[str, ...]:
    if statuses is None:
        return ()
    if isinstance(statuses, str):
        statuses = (statuses,)
    return tuple(sorted({status.strip().lower() for status in statuses if status and status.strip()}))


def _display_status_filter(statuses: Sequence[str] | None) -> str:
    if not statuses:
        return "all"
    return ",".join(statuses)


def _parse_json_object(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_json_list(value: str | None) -> list[Any]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _age_hours(created_at: str | None, now: datetime) -> float | None:
    created = _parse_timestamp(created_at)
    if created is None:
        return None
    return round(max(0.0, (now - created).total_seconds() / 3600), 2)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _value(value: Any, default: str) -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
