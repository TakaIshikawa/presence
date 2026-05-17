"""Report queued replies with stale draft, relationship, or source context."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_STALE_CONTEXT_HOURS = 168.0
DEFAULT_STALE_SOURCE_HOURS = 72.0
DEFAULT_DRAFT_REVIEW_HOURS = 48.0


@dataclass(frozen=True)
class ReplyQueueContextStalenessRow:
    reply_id: int
    draft_age_hours: float | None
    context_age_hours: float | None
    source_mention_age_hours: float | None
    staleness_status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReplyQueueContextStalenessReport:
    generated_at: str
    filters: dict[str, Any]
    rows: tuple[ReplyQueueContextStalenessRow, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_queue_context_staleness",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "rows": [row.to_dict() for row in self.rows],
        }


def build_reply_queue_context_staleness_report(
    db_or_conn: Any,
    *,
    stale_context_hours: float = DEFAULT_STALE_CONTEXT_HOURS,
    stale_source_hours: float = DEFAULT_STALE_SOURCE_HOURS,
    draft_review_hours: float = DEFAULT_DRAFT_REVIEW_HOURS,
    now: datetime | None = None,
) -> ReplyQueueContextStalenessReport:
    if stale_context_hours <= 0:
        raise ValueError("stale_context_hours must be positive")
    if stale_source_hours <= 0:
        raise ValueError("stale_source_hours must be positive")
    if draft_review_hours <= 0:
        raise ValueError("draft_review_hours must be positive")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    rows = [
        _row(
            raw,
            now=generated_at,
            stale_context_hours=stale_context_hours,
            stale_source_hours=stale_source_hours,
            draft_review_hours=draft_review_hours,
        )
        for raw in _load_reply_rows(conn)
    ]
    rows.sort(key=lambda row: (_severity_rank(row.staleness_status), row.reply_id))
    return ReplyQueueContextStalenessReport(
        generated_at=generated_at.isoformat(),
        filters={
            "stale_context_hours": stale_context_hours,
            "stale_source_hours": stale_source_hours,
            "draft_review_hours": draft_review_hours,
        },
        rows=tuple(rows),
    )


def format_reply_queue_context_staleness_json(report: ReplyQueueContextStalenessReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_queue_context_staleness_table(report: ReplyQueueContextStalenessReport) -> str:
    lines = [
        "Reply Queue Context Staleness",
        f"Generated: {report.generated_at}",
        "",
        "reply_id | draft_age_hours | context_age_hours | source_mention_age_hours | staleness_status",
    ]
    if not report.rows:
        lines.append("No queued reply drafts found.")
        return "\n".join(lines)
    for row in report.rows:
        lines.append(
            " | ".join(
                [
                    str(row.reply_id),
                    _fmt(row.draft_age_hours),
                    _fmt(row.context_age_hours),
                    _fmt(row.source_mention_age_hours),
                    row.staleness_status,
                ]
            )
        )
    return "\n".join(lines)


def _load_reply_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    if not _has_table(conn, "reply_queue"):
        return []
    columns = _columns(conn, "reply_queue")
    if {"id", "status"} - columns:
        return []
    draft_ts = _coalesce_expr(columns, ("detected_at", "created_at"), "NULL", "rq")
    relationship_context = "rq.relationship_context" if "relationship_context" in columns else "NULL"
    platform_metadata = "rq.platform_metadata" if "platform_metadata" in columns else "NULL"
    return conn.execute(
        f"""SELECT rq.id,
                  {draft_ts} AS draft_timestamp,
                  {relationship_context} AS relationship_context,
                  {platform_metadata} AS platform_metadata
           FROM reply_queue rq
           WHERE rq.status IN ('pending', 'queued')
           ORDER BY rq.id ASC"""
    ).fetchall()


def _row(
    raw: sqlite3.Row,
    *,
    now: datetime,
    stale_context_hours: float,
    stale_source_hours: float,
    draft_review_hours: float,
) -> ReplyQueueContextStalenessRow:
    draft_at = _parse_datetime(raw["draft_timestamp"])
    context_at = _timestamp_from_json(raw["relationship_context"], ("updated_at", "refreshed_at", "fetched_at", "generated_at"))
    source_at = _timestamp_from_json(raw["platform_metadata"], ("mention_fetched_at", "source_mention_fetched_at", "fetched_at", "snapshot_at"))
    draft_age = _age_hours(now, draft_at)
    context_age = _age_hours(now, context_at)
    source_age = _age_hours(now, source_at)
    statuses: list[str] = []
    if context_age is None or context_age > stale_context_hours:
        statuses.append("stale_context")
    if source_age is None or source_age > stale_source_hours:
        statuses.append("stale_source")
    if draft_age is not None and draft_age > draft_review_hours:
        statuses.append("stale_draft_review")
    return ReplyQueueContextStalenessRow(
        reply_id=int(raw["id"]),
        draft_age_hours=draft_age,
        context_age_hours=context_age,
        source_mention_age_hours=source_age,
        staleness_status=",".join(statuses) if statuses else "fresh",
    )


def _timestamp_from_json(raw: Any, keys: tuple[str, ...]) -> datetime | None:
    data = _json_object(raw)
    for key in keys:
        parsed = _parse_datetime(data.get(key))
        if parsed is not None:
            return parsed
    return None


def _json_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _age_hours(now: datetime, value: datetime | None) -> float | None:
    if value is None:
        return None
    return round(max(0.0, (now - value).total_seconds() / 3600), 2)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (table,)).fetchone() is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def _coalesce_expr(columns: set[str], names: tuple[str, ...], fallback: str, alias: str) -> str:
    present = [f"{alias}.{name}" for name in names if name in columns]
    if len(present) == 1:
        return present[0]
    return f"COALESCE({', '.join(present)})" if present else fallback


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _severity_rank(status: str) -> int:
    if "stale_source" in status:
        return 0
    if "stale_context" in status:
        return 1
    if "stale_draft_review" in status:
        return 2
    return 3


def _fmt(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}"
