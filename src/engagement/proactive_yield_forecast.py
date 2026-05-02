"""Forecast expected engagement yield for pending proactive actions."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Sequence


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 25
DEFAULT_MIN_SCORE = 0.0

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
    "created_at",
    "reviewed_at",
    "posted_at",
)

SUCCESS_STATUSES = {"posted", "published", "completed", "sent"}
NEGATIVE_STATUSES = {"dismissed", "rejected", "expired", "dropped"}


@dataclass(frozen=True)
class ProactiveYieldForecastItem:
    """One pending proactive action scored for likely useful engagement."""

    id: int
    target_handle: str | None
    action_type: str
    score: float
    score_components: dict[str, Any]
    recommended_next_step: str
    context_gaps: tuple[str, ...]
    created_at: str | None
    age_days: float | None
    discovery_source: str | None
    target_tier: str | None
    knowledge_ids: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "target_handle": self.target_handle,
            "action_type": self.action_type,
            "score": self.score,
            "score_components": self.score_components,
            "recommended_next_step": self.recommended_next_step,
            "context_gaps": list(self.context_gaps),
            "created_at": self.created_at,
            "age_days": self.age_days,
            "discovery_source": self.discovery_source,
            "target_tier": self.target_tier,
            "knowledge_ids": list(self.knowledge_ids),
        }


@dataclass(frozen=True)
class ProactiveYieldForecastReport:
    """Deterministic read-only proactive action yield forecast."""

    generated_at: str
    filters: dict[str, Any]
    total_actions: int
    summary: dict[str, Any]
    actions: tuple[ProactiveYieldForecastItem, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "proactive_yield_forecast",
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


def build_proactive_yield_forecast_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    min_score: float = DEFAULT_MIN_SCORE,
    now: datetime | None = None,
) -> ProactiveYieldForecastReport:
    """Build a deterministic, read-only forecast for pending proactive actions."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if min_score < 0:
        raise ValueError("min_score must be non-negative")

    conn = _connection(db_or_conn)
    now = _as_utc(now or datetime.now(timezone.utc))
    filters = {
        "days": days,
        "limit": limit,
        "lookback_end": now.isoformat(),
        "lookback_start": (now - timedelta(days=days)).isoformat(),
        "min_score": min_score,
    }

    schema = _schema(conn)
    if "proactive_actions" not in schema:
        return _empty_report(now, filters, missing_tables=("proactive_actions",))
    missing = tuple(column for column in PROACTIVE_COLUMNS if column not in schema["proactive_actions"])
    if missing:
        return _empty_report(now, filters, missing_columns={"proactive_actions": missing})

    rows = _action_rows(conn, days=days, now=now)
    pending_rows = [row for row in rows if _status(row) == "pending"]
    prior = _prior_outcomes(rows)
    scored = tuple(
        _forecast_item(row, prior[_history_key(row)], now) for row in pending_rows
    )
    filtered = [item for item in scored if item.score >= min_score]
    filtered.sort(key=_forecast_sort_key)
    actions = tuple(filtered[:limit])

    return ProactiveYieldForecastReport(
        generated_at=now.isoformat(),
        filters=filters,
        total_actions=len(actions),
        summary=_summary(actions, rows_scanned=len(rows), rows_scored=len(scored)),
        actions=actions,
    )


def format_proactive_yield_forecast_json(report: ProactiveYieldForecastReport) -> str:
    """Render deterministic JSON suitable for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_proactive_yield_forecast_text(report: ProactiveYieldForecastReport) -> str:
    """Render a compact human-readable proactive yield forecast."""
    filters = report.filters
    lines = [
        "Proactive Yield Forecast",
        (
            "Filters: "
            f"days={filters.get('days')} limit={filters.get('limit')} "
            f"min_score={filters.get('min_score')}"
        ),
        f"Actions: {report.total_actions}",
    ]
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
        lines.append("No pending proactive actions matched.")
        return "\n".join(lines)

    lines.append("Items:")
    for action in report.actions:
        handle = action.target_handle or "?"
        age = "n/a" if action.age_days is None else f"{action.age_days:.1f}d"
        lines.append(
            f"  - #{action.id} {action.action_type} @{handle} score={action.score:.1f} "
            f"next={action.recommended_next_step} age={age}"
        )
        lines.append(
            "    "
            f"source={action.discovery_source or 'n/a'} tier={action.target_tier or 'unknown'} "
            f"knowledge={len(action.knowledge_ids)} gaps={_display_gaps(action.context_gaps)}"
        )
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
) -> ProactiveYieldForecastReport:
    return ProactiveYieldForecastReport(
        generated_at=now.isoformat(),
        filters=filters,
        total_actions=0,
        summary=_summary((), rows_scanned=0, rows_scored=0),
        actions=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _action_rows(conn: sqlite3.Connection, *, days: int, now: datetime) -> list[dict[str, Any]]:
    cutoff = (now - timedelta(days=days)).isoformat()
    cursor = conn.execute(
        f"""SELECT {', '.join(PROACTIVE_COLUMNS)}
            FROM proactive_actions
            WHERE datetime(COALESCE(created_at, ?)) >= datetime(?)
              AND LOWER(COALESCE(status, 'pending')) IN (
                  'pending', 'posted', 'dismissed', 'approved', 'rejected', 'expired'
              )
            ORDER BY datetime(COALESCE(created_at, ?)) ASC, id ASC""",
        [now.isoformat(), cutoff, now.isoformat()],
    )
    return [dict(row) for row in cursor.fetchall()]


def _prior_outcomes(rows: Sequence[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    history: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if _status(row) != "pending":
            history[_history_key(row)].append(row)
    return history


def _forecast_item(
    row: dict[str, Any],
    prior_rows: Sequence[dict[str, Any]],
    now: datetime,
) -> ProactiveYieldForecastItem:
    action_type = _text(row.get("action_type")) or "unknown"
    relationship = _parse_json_object(row.get("relationship_context"))
    platform_metadata = _parse_json_object(row.get("platform_metadata"))
    knowledge = _knowledge_metadata(row.get("knowledge_ids"))
    age_days = _age_days(row.get("created_at"), now)
    gaps = _context_gaps(row, relationship, knowledge)
    target_tier, tier_points = _target_tier_points(relationship, platform_metadata)
    components = {
        "action_type": _action_type_points(action_type),
        "target_tier": {
            "value": target_tier,
            "points": tier_points,
        },
        "discovery_source": _discovery_source_points(row.get("discovery_source")),
        "age": _age_points(age_days),
        "prior_outcomes": _prior_outcome_points(prior_rows),
        "knowledge_ids": knowledge,
        "relevance": _relevance_points(row.get("relevance_score")),
        "context": _context_points(gaps),
    }
    score = round(_clamp(sum(_component_points(value) for value in components.values())), 2)
    return ProactiveYieldForecastItem(
        id=int(row["id"]),
        target_handle=_clean_handle(row.get("target_author_handle")),
        action_type=action_type,
        score=score,
        score_components=components,
        recommended_next_step=_recommended_next_step(score, gaps, components["prior_outcomes"]),
        context_gaps=tuple(gaps),
        created_at=row.get("created_at"),
        age_days=age_days,
        discovery_source=_text(row.get("discovery_source")) or None,
        target_tier=target_tier,
        knowledge_ids=tuple(knowledge["ids"]),
    )


def _action_type_points(action_type: str) -> dict[str, Any]:
    weights = {
        "reply": 18.0,
        "quote_tweet": 15.0,
        "retweet": 8.0,
        "like": 5.0,
    }
    return {"value": action_type, "points": weights.get(action_type.casefold(), 2.0)}


def _target_tier_points(
    relationship: dict[str, Any],
    metadata: dict[str, Any],
) -> tuple[str | None, float]:
    tier = _first_present(relationship, "dunbar_tier", "tier", "target_tier")
    if tier is None:
        tier = _first_present(metadata, "dunbar_tier", "tier", "target_tier")
    tier_name = _first_present(relationship, "tier_name", "target_tier_name")
    if tier_name is None:
        tier_name = _first_present(metadata, "tier_name", "target_tier_name")

    numeric = _float_or_none(tier)
    if numeric is not None:
        label = f"tier {numeric:g}"
        if tier_name:
            label = f"{tier_name} ({label})"
        if numeric <= 1:
            return label, 18.0
        if numeric <= 2:
            return label, 14.0
        if numeric <= 3:
            return label, 9.0
        if numeric <= 4:
            return label, 4.0
        return label, 0.0

    if tier_name:
        label = str(tier_name)
        lower = label.casefold()
        if "core" in lower or "key" in lower or "gold" in lower:
            return label, 12.0
        if "warm" in lower or "silver" in lower:
            return label, 8.0
        if "outer" in lower or "bronze" in lower:
            return label, 3.0
        return label, 2.0
    return None, 0.0


def _discovery_source_points(value: Any) -> dict[str, Any]:
    source = _text(value).casefold()
    weights = {
        "quote_opportunities": 12.0,
        "curated_timeline": 10.0,
        "cultivate": 8.0,
        "search": 6.0,
        "following": 5.0,
        "proactive_mining": 4.0,
        "config": 3.0,
    }
    return {"value": source or None, "points": weights.get(source, 2.0 if source else 0.0)}


def _age_points(age_days: float | None) -> dict[str, Any]:
    if age_days is None:
        return {"value_days": None, "points": 0.0}
    if age_days <= 2:
        points = 10.0
    elif age_days <= 7:
        points = 7.0
    elif age_days <= 14:
        points = 3.0
    else:
        points = -4.0
    return {"value_days": age_days, "points": points}


def _prior_outcome_points(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(_status(row) for row in rows)
    positive = min(16.0, counts["posted"] * 8.0 + counts["approved"] * 2.0)
    negative = min(18.0, sum(counts[status] for status in NEGATIVE_STATUSES) * 7.0)
    return {
        "counts": dict(sorted(counts.items())),
        "points": positive - negative,
    }


def _knowledge_metadata(value: Any) -> dict[str, Any]:
    parsed = _parse_json_list(value)
    ids: list[int] = []
    relevances: list[float] = []
    for item in parsed:
        knowledge_id: Any
        relevance: Any = None
        if isinstance(item, (list, tuple)) and item:
            knowledge_id = item[0]
            relevance = item[1] if len(item) > 1 else None
        elif isinstance(item, dict):
            knowledge_id = item.get("knowledge_id") or item.get("id")
            relevance = item.get("relevance") or item.get("score")
        else:
            knowledge_id = item
        int_id = _int_or_none(knowledge_id)
        if int_id is not None:
            ids.append(int_id)
        score = _float_or_none(relevance)
        if score is not None:
            relevances.append(score)

    average = round(sum(relevances) / len(relevances), 3) if relevances else None
    relevance_bonus = (average or 0.0) * 4.0
    points = min(10.0, len(ids) * 3.0 + relevance_bonus)
    return {
        "ids": ids,
        "count": len(ids),
        "average_relevance": average,
        "points": round(points, 2),
    }


def _relevance_points(value: Any) -> dict[str, Any]:
    score = _float_or_none(value)
    if score is None:
        return {"value": None, "points": 0.0}
    normalized = score if score <= 1.0 else score / 10.0
    return {"value": score, "points": round(max(0.0, min(10.0, normalized * 10.0)), 2)}


def _context_points(gaps: Sequence[str]) -> dict[str, Any]:
    return {"gaps": list(gaps), "points": -10.0 if gaps else 5.0}


def _context_gaps(
    row: dict[str, Any],
    relationship: dict[str, Any],
    knowledge: dict[str, Any],
) -> list[str]:
    gaps = []
    if not _clean_handle(row.get("target_author_handle")):
        gaps.append("missing_target_handle")
    if not _text(row.get("target_tweet_text")):
        gaps.append("missing_target_text")
    if not relationship:
        gaps.append("missing_relationship_context")
    if knowledge["count"] <= 0:
        gaps.append("missing_knowledge_ids")
    return gaps


def _recommended_next_step(
    score: float,
    gaps: Sequence[str],
    prior_component: dict[str, Any],
) -> str:
    negative_count = sum(
        count
        for status, count in prior_component.get("counts", {}).items()
        if status in NEGATIVE_STATUSES
    )
    if gaps:
        return "enrich_context"
    if negative_count >= 2 and score < 45:
        return "drop"
    if score >= 55:
        return "execute"
    if score < 30:
        return "drop"
    return "defer"


def _summary(
    actions: Sequence[ProactiveYieldForecastItem],
    *,
    rows_scanned: int,
    rows_scored: int,
) -> dict[str, Any]:
    return {
        "rows_scanned": rows_scanned,
        "rows_scored": rows_scored,
        "by_recommended_next_step": dict(
            sorted(Counter(action.recommended_next_step for action in actions).items())
        ),
        "by_action_type": dict(sorted(Counter(action.action_type for action in actions).items())),
    }


def _forecast_sort_key(item: ProactiveYieldForecastItem) -> tuple[float, str, int]:
    return (-item.score, item.created_at or "", item.id)


def _history_key(row: dict[str, Any]) -> tuple[str, str]:
    return (_clean_handle(row.get("target_author_handle")) or "", _text(row.get("action_type")).casefold())


def _status(row: dict[str, Any]) -> str:
    status = _text(row.get("status")).casefold() or "pending"
    if status in SUCCESS_STATUSES:
        return "posted"
    return status


def _component_points(value: Any) -> float:
    if isinstance(value, dict):
        return float(value.get("points") or 0.0)
    return 0.0


def _parse_json_object(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


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


def _clean_handle(value: Any) -> str | None:
    text = _text(value).lstrip("@").casefold()
    return text or None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value) if value is not None and str(value).strip() else None
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None and str(value).strip() else None
    except (TypeError, ValueError):
        return None


def _first_present(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value is not None and str(value).strip():
            return value
    return None


def _clamp(value: float) -> float:
    return max(0.0, min(100.0, value))


def _display_gaps(gaps: Sequence[str]) -> str:
    return "none" if not gaps else ",".join(gaps)
