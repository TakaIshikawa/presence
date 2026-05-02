"""Plan stale proactive draft refresh work."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Sequence


DEFAULT_STALE_DAYS = 7
DEFAULT_LIMIT = 100
DEFAULT_STATUSES = ("approved", "pending")
VALID_STATUSES = frozenset(DEFAULT_STATUSES)
VALID_ACTION_TYPES = frozenset(("like", "quote_tweet", "reply", "retweet"))

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
    "created_at",
    "reviewed_at",
)

REASON_STALE_DRAFT_TEXT = "stale_draft_text"
REASON_MISSING_TARGET_TWEET_TEXT = "missing_target_tweet_text"
REASON_MISSING_RELATIONSHIP_CONTEXT = "missing_relationship_context"
REASON_MISSING_KNOWLEDGE_IDS = "missing_knowledge_ids"

RECOMMEND_REFRESH_DRAFT = "refresh_draft"
RECOMMEND_ENRICH_CONTEXT = "enrich_context_before_review"
RECOMMEND_REFRESH_WITH_CONTEXT = "refresh_draft_with_context"


@dataclass(frozen=True)
class ProactiveDraftRefreshItem:
    """One proactive action that may need draft refresh work."""

    id: int
    status: str
    action_type: str
    target_tweet_id: str | None
    target_author_handle: str | None
    discovery_source: str | None
    relevance_score: float | None
    created_at: str | None
    reviewed_at: str | None
    age_anchor: str | None
    age_days: float | None
    has_draft_text: bool
    draft_preview: str
    refresh_reasons: tuple[str, ...]
    recommendation: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status,
            "action_type": self.action_type,
            "target_tweet_id": self.target_tweet_id,
            "target_author_handle": self.target_author_handle,
            "discovery_source": self.discovery_source,
            "relevance_score": self.relevance_score,
            "created_at": self.created_at,
            "reviewed_at": self.reviewed_at,
            "age_anchor": self.age_anchor,
            "age_days": self.age_days,
            "has_draft_text": self.has_draft_text,
            "draft_preview": self.draft_preview,
            "refresh_reasons": list(self.refresh_reasons),
            "recommendation": self.recommendation,
        }


@dataclass(frozen=True)
class ProactiveDraftRefreshReport:
    """Aggregated proactive draft refresh plan."""

    generated_at: str
    filters: dict[str, Any]
    total_actions: int
    summary: dict[str, Any]
    actions: tuple[ProactiveDraftRefreshItem, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "proactive_draft_refresh_plan",
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


def build_proactive_draft_refresh_report(
    db_or_conn: Any,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    statuses: str | Sequence[str] | None = DEFAULT_STATUSES,
    action_types: str | Sequence[str] | None = None,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ProactiveDraftRefreshReport:
    """Build a deterministic, read-only plan for refreshing proactive drafts."""
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")

    wanted_statuses = _normalise_values(statuses, field="status", allowed=VALID_STATUSES)
    wanted_action_types = _normalise_values(
        action_types,
        field="action_type",
        allowed=VALID_ACTION_TYPES,
    )
    if not wanted_statuses:
        wanted_statuses = DEFAULT_STATUSES

    conn = _connection(db_or_conn)
    now = _as_utc(now or datetime.now(timezone.utc))
    filters = {
        "stale_days": stale_days,
        "status": list(wanted_statuses),
        "action_type": list(wanted_action_types),
        "limit": limit,
    }

    schema = _schema(conn)
    if "proactive_actions" not in schema:
        return _empty_report(now, filters, missing_tables=("proactive_actions",))
    missing = tuple(column for column in PROACTIVE_COLUMNS if column not in schema["proactive_actions"])
    if missing:
        return _empty_report(now, filters, missing_columns={"proactive_actions": missing})

    rows = _action_rows(conn, statuses=wanted_statuses, action_types=wanted_action_types)
    matched = tuple(
        item
        for item in (_item_for_row(row, stale_days=stale_days, now=now) for row in rows)
        if item is not None
    )
    actions = matched[:limit] if limit is not None else matched
    return ProactiveDraftRefreshReport(
        generated_at=now.isoformat(),
        filters=filters,
        total_actions=len(actions),
        summary=_summary(actions),
        actions=actions,
    )


def format_proactive_draft_refresh_json(report: ProactiveDraftRefreshReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_proactive_draft_refresh_text(report: ProactiveDraftRefreshReport) -> str:
    """Render a compact human-readable proactive draft refresh plan."""
    lines = [
        "Proactive Draft Refresh Plan",
        (
            "Filters: "
            f"stale_days={report.filters.get('stale_days')} "
            f"status={_display_filter(report.filters.get('status'))} "
            f"action_type={_display_filter(report.filters.get('action_type'))} "
            f"limit={report.filters.get('limit')}"
        ),
        f"Actions: {report.total_actions}",
    ]
    totals = report.summary.get("by_recommendation", {})
    if totals:
        lines.append("Recommendations:")
        for recommendation, count in sorted(totals.items()):
            lines.append(f"  - {recommendation}: {count}")
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
        lines.append("No proactive drafts need refresh.")
        return "\n".join(lines)

    lines.append("Items:")
    for action in report.actions:
        handle = action.target_author_handle or "?"
        target_id = action.target_tweet_id or "?"
        age = "n/a" if action.age_days is None else f"{action.age_days:.1f}d"
        lines.append(
            f"  - #{action.id} {action.action_type} @{handle} target={target_id} "
            f"status={action.status} age={age} recommendation={action.recommendation}"
        )
        lines.append(
            "    "
            f"source={action.discovery_source or 'n/a'} "
            f"score={_display_score(action.relevance_score)} "
            f"anchor={action.age_anchor or 'n/a'} "
            f"draft={str(action.has_draft_text).lower()}"
        )
        lines.append("    reasons=" + ", ".join(action.refresh_reasons))
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
) -> ProactiveDraftRefreshReport:
    return ProactiveDraftRefreshReport(
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
    statuses: Sequence[str],
    action_types: Sequence[str],
) -> list[dict[str, Any]]:
    where = ["LOWER(COALESCE(status, 'pending')) IN (" + ",".join("?" for _ in statuses) + ")"]
    params: list[Any] = list(statuses)
    if action_types:
        where.append("LOWER(action_type) IN (" + ",".join("?" for _ in action_types) + ")")
        params.extend(action_types)
    cursor = conn.execute(
        f"""SELECT {', '.join(PROACTIVE_COLUMNS)}
            FROM proactive_actions
            WHERE {' AND '.join(where)}
            ORDER BY datetime(COALESCE(reviewed_at, created_at)) ASC,
                     relevance_score DESC,
                     id ASC""",
        params,
    )
    return [dict(row) for row in cursor.fetchall()]


def _item_for_row(
    row: dict[str, Any],
    *,
    stale_days: int,
    now: datetime,
) -> ProactiveDraftRefreshItem | None:
    has_draft = bool(_text(row.get("draft_text")))
    anchor = row.get("reviewed_at") or row.get("created_at")
    age_days = _age_days(anchor, now)

    reasons = []
    if has_draft and age_days is not None and age_days >= stale_days:
        reasons.append(REASON_STALE_DRAFT_TEXT)
    if not _text(row.get("target_tweet_text")):
        reasons.append(REASON_MISSING_TARGET_TWEET_TEXT)
    if not _text(row.get("relationship_context")):
        reasons.append(REASON_MISSING_RELATIONSHIP_CONTEXT)
    if not _parse_json_list(row.get("knowledge_ids")):
        reasons.append(REASON_MISSING_KNOWLEDGE_IDS)

    if not reasons:
        return None
    if not has_draft and not any(reason != REASON_STALE_DRAFT_TEXT for reason in reasons):
        return None

    return ProactiveDraftRefreshItem(
        id=int(row["id"]),
        status=_text(row.get("status")) or "pending",
        action_type=_text(row.get("action_type")) or "unknown",
        target_tweet_id=row.get("target_tweet_id"),
        target_author_handle=row.get("target_author_handle"),
        discovery_source=row.get("discovery_source"),
        relevance_score=_float_or_none(row.get("relevance_score")),
        created_at=row.get("created_at"),
        reviewed_at=row.get("reviewed_at"),
        age_anchor=anchor,
        age_days=age_days,
        has_draft_text=has_draft,
        draft_preview=_preview(row.get("draft_text")),
        refresh_reasons=tuple(reasons),
        recommendation=_recommendation(tuple(reasons)),
    )


def _recommendation(reasons: Sequence[str]) -> str:
    has_stale = REASON_STALE_DRAFT_TEXT in reasons
    has_context_gap = any(reason != REASON_STALE_DRAFT_TEXT for reason in reasons)
    if has_stale and has_context_gap:
        return RECOMMEND_REFRESH_WITH_CONTEXT
    if has_context_gap:
        return RECOMMEND_ENRICH_CONTEXT
    return RECOMMEND_REFRESH_DRAFT


def _summary(actions: Sequence[ProactiveDraftRefreshItem]) -> dict[str, Any]:
    by_recommendation = Counter(action.recommendation for action in actions)
    by_reason = Counter(reason for action in actions for reason in action.refresh_reasons)
    by_status = Counter(action.status for action in actions)
    by_action_type = Counter(action.action_type for action in actions)
    return {
        "by_recommendation": dict(sorted(by_recommendation.items())),
        "by_reason": dict(sorted(by_reason.items())),
        "by_status": dict(sorted(by_status.items())),
        "by_action_type": dict(sorted(by_action_type.items())),
    }


def _normalise_values(
    values: str | Sequence[str] | None,
    *,
    field: str,
    allowed: frozenset[str],
) -> tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, str):
        values = (values,)
    parsed = tuple(sorted({value.strip().casefold() for value in values if value and value.strip()}))
    invalid = tuple(value for value in parsed if value not in allowed)
    if invalid:
        raise ValueError(f"invalid {field}: {', '.join(invalid)}")
    return parsed


def _parse_json_list(value: Any) -> list[Any]:
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except (json.JSONDecodeError, TypeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _age_days(value: str | None, now: datetime) -> float | None:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return None
    return round(max(0.0, (now - parsed).total_seconds() / 86400), 2)


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


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _preview(value: Any, max_length: int = 120) -> str:
    text = " ".join(_text(value).split())
    if len(text) <= max_length:
        return text
    return text[: max_length - 3] + "..."


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _display_filter(values: Sequence[str] | None) -> str:
    if not values:
        return "all"
    return ",".join(values)


def _display_score(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f}"
