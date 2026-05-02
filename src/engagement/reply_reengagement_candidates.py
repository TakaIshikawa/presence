"""Find reply authors worth deliberate re-engagement."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_MIN_AGE_DAYS = 14
DEFAULT_LIMIT = 25

REPLY_TABLE = "reply_queue"
REMINDER_TABLE = "reply_followup_reminders"
PROACTIVE_TABLE = "proactive_actions"
OPTIONAL_TABLES = (REMINDER_TABLE, PROACTIVE_TABLE)
INTERACTION_STATUSES = {"approved", "posted"}
QUESTION_INTENTS = {"question", "bug_report"}
HIGH_PRIORITIES = {"high", "urgent"}
TIER_BONUSES = {
    "trusted": 12,
    "champion": 12,
    "customer": 10,
    "partner": 8,
    "warm": 8,
    "neutral": 2,
    "noisy": -10,
    "blocked_candidate": -25,
    "blocked": -30,
}


@dataclass(frozen=True)
class ReplyReengagementCandidate:
    """One inbound author with useful prior reply history."""

    handle: str
    platform: str
    last_interaction_at: str | None
    interaction_count: int
    score: int
    reasons: tuple[str, ...]
    excluded_by_cooldown: bool
    avg_quality_score: float | None = None
    high_priority_count: int = 0
    question_intent_count: int = 0
    relationship_tier: str | None = None
    relationship_strength: float | None = None
    cooldown_reasons: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reasons"] = list(self.reasons)
        payload["cooldown_reasons"] = list(self.cooldown_reasons)
        return payload


@dataclass(frozen=True)
class ReplyReengagementCandidatesReport:
    """Ranked reply-author re-engagement candidates."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    candidates: tuple[ReplyReengagementCandidate, ...]
    source_table: str | None = REPLY_TABLE
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_reengagement_candidates",
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "source_table": self.source_table,
            "totals": dict(sorted(self.totals.items())),
        }


def build_reply_reengagement_candidates_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_age_days: int = DEFAULT_MIN_AGE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ReplyReengagementCandidatesReport:
    """Build a deterministic, read-only report of reply authors to revisit."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_age_days <= 0:
        raise ValueError("min_age_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    lookback_start = generated_at - timedelta(days=days)
    min_last_interaction_at = generated_at - timedelta(days=min_age_days)
    filters = {
        "days": days,
        "limit": limit,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": lookback_start.isoformat(),
        "min_age_days": min_age_days,
        "min_last_interaction_at": min_last_interaction_at.isoformat(),
    }
    missing_columns: dict[str, tuple[str, ...]] = {}
    missing_tables: tuple[str, ...] = ()
    pending_reminders: set[str] = set()
    recent_actions: set[str] = set()

    if _looks_like_rows(db_or_rows):
        rows = [_normalize_reply_row(_mapping(row), now=generated_at) for row in db_or_rows]
        source_table: str | None = "rows"
    else:
        conn = _connection(db_or_rows)
        columns = _table_columns(conn, REPLY_TABLE)
        if not columns:
            return ReplyReengagementCandidatesReport(
                generated_at=generated_at.isoformat(),
                filters=filters,
                totals=_empty_totals(),
                candidates=(),
                source_table=None,
                missing_tables=(REPLY_TABLE,),
                missing_columns={},
            )
        missing = _missing_reply_columns(columns)
        if missing:
            missing_columns[REPLY_TABLE] = missing
        rows = _load_reply_rows(conn, columns, now=generated_at)
        source_table = REPLY_TABLE

        missing_optional = [table for table in OPTIONAL_TABLES if not _table_columns(conn, table)]
        missing_tables = tuple(missing_optional)
        if REMINDER_TABLE not in missing_optional:
            reminder_columns = _table_columns(conn, REMINDER_TABLE)
            reminder_missing = tuple(
                column
                for column in ("target_handle", "status")
                if column not in reminder_columns
            )
            if reminder_missing:
                missing_columns[REMINDER_TABLE] = reminder_missing
            else:
                pending_reminders = _pending_reminder_handles(conn, reminder_columns)
        if PROACTIVE_TABLE not in missing_optional:
            action_columns = _table_columns(conn, PROACTIVE_TABLE)
            action_missing = tuple(
                column
                for column in ("target_author_handle", "created_at")
                if column not in action_columns
            )
            if action_missing:
                missing_columns[PROACTIVE_TABLE] = action_missing
            else:
                recent_actions = _recent_proactive_action_handles(
                    conn,
                    action_columns,
                    since=min_last_interaction_at,
                )

    rows = [
        row
        for row in rows
        if row["is_prior_interaction"]
        and row["handle"]
        and lookback_start <= row["interaction_at"] <= generated_at
    ]
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["platform"], row["handle"])].append(row)

    all_candidates = [
        _candidate(
            platform,
            handle,
            matches,
            now=generated_at,
            min_last_interaction_at=min_last_interaction_at,
            pending_reminders=pending_reminders,
            recent_actions=recent_actions,
        )
        for (platform, handle), matches in grouped.items()
    ]
    all_candidates.sort(key=_candidate_sort_key)
    candidates = all_candidates[:limit]

    return ReplyReengagementCandidatesReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "rows_scanned": len(rows),
            "sources_scanned": len(grouped),
            "candidates_ranked": len(all_candidates),
            "candidates_reported": len(candidates),
            "actionable_candidates": sum(
                1 for candidate in all_candidates if not candidate.excluded_by_cooldown
            ),
            "cooldown_excluded": sum(
                1 for candidate in all_candidates if candidate.excluded_by_cooldown
            ),
        },
        candidates=tuple(candidates),
        source_table=source_table,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_reply_reengagement_candidates_json(
    report: ReplyReengagementCandidatesReport,
) -> str:
    """Serialize the re-engagement candidate report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_reengagement_candidates_text(
    report: ReplyReengagementCandidatesReport,
) -> str:
    """Render a concise operator-facing re-engagement report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Reply Re-engagement Candidates",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={filters['days']} min_age_days={filters['min_age_days']} "
            f"limit={filters['limit']}"
        ),
        (
            "Totals: "
            f"rows={totals['rows_scanned']} sources={totals['sources_scanned']} "
            f"ranked={totals['candidates_ranked']} reported={totals['candidates_reported']} "
            f"actionable={totals['actionable_candidates']} "
            f"cooldown={totals['cooldown_excluded']}"
        ),
    ]
    if report.source_table:
        lines.append(f"Source table: {report.source_table}")
    if report.missing_tables:
        lines.append("Missing optional tables: " + ", ".join(report.missing_tables))
    missing_columns = [
        f"{table}({', '.join(columns)})"
        for table, columns in (report.missing_columns or {}).items()
        if columns
    ]
    if missing_columns:
        lines.append("Missing optional columns: " + "; ".join(missing_columns))
    if not report.candidates:
        lines.extend(["", "No reply authors matched."])
        return "\n".join(lines)

    lines.extend(["", "Candidates:"])
    for candidate in report.candidates:
        marker = "cooldown" if candidate.excluded_by_cooldown else "actionable"
        quality = "n/a" if candidate.avg_quality_score is None else f"{candidate.avg_quality_score:.1f}"
        lines.append(
            f"- {candidate.platform}/@{candidate.handle} score={candidate.score} "
            f"interactions={candidate.interaction_count} "
            f"last={candidate.last_interaction_at or 'unknown'} "
            f"quality={quality} {marker}"
        )
        lines.append("  reasons: " + ", ".join(candidate.reasons))
        if candidate.cooldown_reasons:
            lines.append("  cooldown: " + ", ".join(candidate.cooldown_reasons))
    return "\n".join(lines)


def _candidate(
    platform: str,
    handle: str,
    rows: list[dict[str, Any]],
    *,
    now: datetime,
    min_last_interaction_at: datetime,
    pending_reminders: set[str],
    recent_actions: set[str],
) -> ReplyReengagementCandidate:
    ordered = sorted(rows, key=_row_sort_key)
    last_interaction_at = ordered[-1]["interaction_at"] if ordered else None
    quality_scores = [
        row["quality_score"] for row in ordered if row.get("quality_score") is not None
    ]
    avg_quality = round(sum(quality_scores) / len(quality_scores), 2) if quality_scores else None
    high_priority_count = sum(1 for row in ordered if row["priority"] in HIGH_PRIORITIES)
    question_count = sum(1 for row in ordered if row["intent"] in QUESTION_INTENTS)
    relationship = _best_relationship(ordered)
    cooldown_reasons: list[str] = []
    if last_interaction_at and last_interaction_at > min_last_interaction_at:
        cooldown_reasons.append("last_interaction_too_recent")
    if handle in pending_reminders:
        cooldown_reasons.append("pending_followup_reminder")
    if handle in recent_actions:
        cooldown_reasons.append("recent_proactive_action")

    score = _score(
        interaction_count=len(ordered),
        days_since_last=(now - last_interaction_at).days if last_interaction_at else 0,
        avg_quality=avg_quality,
        high_priority_count=high_priority_count,
        question_count=question_count,
        relationship=relationship,
    )
    reasons = _reasons(
        interaction_count=len(ordered),
        days_since_last=(now - last_interaction_at).days if last_interaction_at else None,
        avg_quality=avg_quality,
        high_priority_count=high_priority_count,
        question_count=question_count,
        relationship=relationship,
    )
    return ReplyReengagementCandidate(
        handle=handle,
        platform=platform,
        last_interaction_at=last_interaction_at.isoformat() if last_interaction_at else None,
        interaction_count=len(ordered),
        score=score,
        reasons=tuple(reasons),
        excluded_by_cooldown=bool(cooldown_reasons),
        avg_quality_score=avg_quality,
        high_priority_count=high_priority_count,
        question_intent_count=question_count,
        relationship_tier=relationship.get("tier"),
        relationship_strength=relationship.get("strength"),
        cooldown_reasons=tuple(cooldown_reasons),
    )


def _score(
    *,
    interaction_count: int,
    days_since_last: int,
    avg_quality: float | None,
    high_priority_count: int,
    question_count: int,
    relationship: dict[str, Any],
) -> int:
    age_bonus = min(20.0, max(0, days_since_last - 7) * 0.8)
    count_bonus = min(22.0, interaction_count * 5.5)
    quality_bonus = 0.0 if avg_quality is None else max(0.0, (avg_quality - 5.0) * 4.0)
    intent_bonus = min(16.0, high_priority_count * 5.0 + question_count * 4.0)
    tier_bonus = TIER_BONUSES.get(str(relationship.get("tier") or "").casefold(), 0)
    strength = relationship.get("strength")
    strength_bonus = 0.0 if strength is None else max(-8.0, min(12.0, (float(strength) - 0.5) * 24.0))
    raw = 20.0 + age_bonus + count_bonus + quality_bonus + intent_bonus + tier_bonus + strength_bonus
    return max(0, min(100, int(round(raw))))


def _reasons(
    *,
    interaction_count: int,
    days_since_last: int | None,
    avg_quality: float | None,
    high_priority_count: int,
    question_count: int,
    relationship: dict[str, Any],
) -> list[str]:
    reasons = [f"{interaction_count}_prior_interactions"]
    if days_since_last is not None:
        reasons.append(f"{days_since_last}_days_since_last_interaction")
    if avg_quality is not None:
        reasons.append(f"avg_quality_score_{avg_quality:.1f}")
    if high_priority_count:
        reasons.append(f"{high_priority_count}_high_priority_replies")
    if question_count:
        reasons.append(f"{question_count}_question_or_bug_replies")
    if relationship.get("tier"):
        reasons.append(f"relationship_tier_{relationship['tier']}")
    if relationship.get("strength") is not None:
        reasons.append(f"relationship_strength_{relationship['strength']:.2f}")
    return reasons


def _load_reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    now: datetime,
) -> list[dict[str, Any]]:
    select_columns = [
        _column_expr(columns, "id"),
        _column_expr(columns, "platform", "'x'"),
        _column_expr(columns, "inbound_author_handle"),
        _column_expr(columns, "intent", "'other'"),
        _column_expr(columns, "priority", "'normal'"),
        _column_expr(columns, "quality_score"),
        _column_expr(columns, "status", "'pending'"),
        _column_expr(columns, "relationship_context"),
        _column_expr(columns, "detected_at"),
        _column_expr(columns, "reviewed_at"),
        _column_expr(columns, "posted_at"),
        _column_expr(columns, "posted_tweet_id"),
        _column_expr(columns, "posted_platform_id"),
    ]
    order = "datetime(detected_at) ASC, id ASC" if "detected_at" in columns and "id" in columns else "rowid ASC"
    cursor = conn.execute(f"SELECT {', '.join(select_columns)} FROM {REPLY_TABLE} ORDER BY {order}")
    return [_normalize_reply_row(dict(row), now=now) for row in cursor.fetchall()]


def _normalize_reply_row(row: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    status = _clean_label(row.get("status")) or "pending"
    posted = bool(row.get("posted_tweet_id") or row.get("posted_platform_id") or row.get("posted_at"))
    interaction_at = (
        _parse_timestamp(row.get("posted_at"))
        or _parse_timestamp(row.get("reviewed_at"))
        or _parse_timestamp(row.get("detected_at"))
        or now
    )
    return {
        "reply_queue_id": _int_or_none(row.get("id")),
        "platform": _clean_label(row.get("platform")) or "x",
        "handle": _normalize_handle(row.get("inbound_author_handle")),
        "intent": _clean_label(row.get("intent")) or "other",
        "priority": _clean_label(row.get("priority")) or "normal",
        "quality_score": _float_or_none(row.get("quality_score")),
        "status": status,
        "relationship_context": _parse_relationship_context(row.get("relationship_context")),
        "interaction_at": interaction_at,
        "is_prior_interaction": status in INTERACTION_STATUSES or posted,
    }


def _pending_reminder_handles(conn: sqlite3.Connection, columns: set[str]) -> set[str]:
    status_expr = _column_expr(columns, "status", "'pending'")
    rows = conn.execute(
        f"SELECT target_handle, {status_expr} FROM {REMINDER_TABLE}"
    ).fetchall()
    return {
        handle
        for row in rows
        for handle in [_normalize_handle(row["target_handle"])]
        if handle and str(row["status"] or "").casefold() == "pending"
    }


def _recent_proactive_action_handles(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    since: datetime,
) -> set[str]:
    select_columns = [
        _column_expr(columns, "target_author_handle"),
        _column_expr(columns, "created_at"),
        _column_expr(columns, "reviewed_at"),
        _column_expr(columns, "posted_at"),
        _column_expr(columns, "status", "'pending'"),
    ]
    rows = conn.execute(
        f"SELECT {', '.join(select_columns)} FROM {PROACTIVE_TABLE}"
    ).fetchall()
    handles = set()
    for row in rows:
        handle = _normalize_handle(row["target_author_handle"])
        if not handle:
            continue
        timestamp = (
            _parse_timestamp(row["posted_at"])
            or _parse_timestamp(row["reviewed_at"])
            or _parse_timestamp(row["created_at"])
        )
        if timestamp and timestamp >= since and str(row["status"] or "").casefold() != "dismissed":
            handles.add(handle)
    return handles


def _best_relationship(rows: list[dict[str, Any]]) -> dict[str, Any]:
    contexts = [row["relationship_context"] for row in rows if row["relationship_context"]]
    if not contexts:
        return {}
    with_strength = [ctx for ctx in contexts if ctx.get("strength") is not None]
    if with_strength:
        return max(with_strength, key=lambda ctx: float(ctx["strength"]))
    return contexts[-1]


def _parse_relationship_context(raw: Any) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, json.JSONDecodeError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    tier = parsed.get("tier") or parsed.get("stage")
    strength = _float_or_none(parsed.get("strength"))
    return {
        "tier": _clean_label(tier),
        "strength": strength,
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _missing_reply_columns(columns: set[str]) -> tuple[str, ...]:
    expected = (
        "id",
        "platform",
        "inbound_author_handle",
        "intent",
        "priority",
        "quality_score",
        "status",
        "relationship_context",
        "detected_at",
    )
    return tuple(column for column in expected if column not in columns)


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    if column in columns:
        return column
    return f"{default} AS {column}"


def _normalize_handle(value: Any) -> str | None:
    normalized = str(value or "").strip().lstrip("@").casefold()
    return normalized or None


def _clean_label(value: Any) -> str | None:
    normalized = str(value or "").strip().casefold()
    return normalized or None


def _parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
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


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _row_sort_key(row: Mapping[str, Any]) -> tuple[str, int]:
    timestamp = row.get("interaction_at")
    timestamp_text = timestamp.isoformat() if isinstance(timestamp, datetime) else ""
    return (timestamp_text, int(row.get("reply_queue_id") or 0))


def _candidate_sort_key(candidate: ReplyReengagementCandidate) -> tuple[Any, ...]:
    cooldown_rank = 1 if candidate.excluded_by_cooldown else 0
    last = _parse_timestamp(candidate.last_interaction_at)
    last_ts = last.timestamp() if last else 0.0
    return (
        cooldown_rank,
        -candidate.score,
        -candidate.interaction_count,
        -last_ts,
        candidate.platform,
        candidate.handle,
    )


def _empty_totals() -> dict[str, int]:
    return {
        "rows_scanned": 0,
        "sources_scanned": 0,
        "candidates_ranked": 0,
        "candidates_reported": 0,
        "actionable_candidates": 0,
        "cooldown_excluded": 0,
    }


def _looks_like_rows(value: Any) -> bool:
    if isinstance(value, (sqlite3.Connection, str, bytes)) or hasattr(value, "conn"):
        return False
    return isinstance(value, Iterable)


def _mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row)
