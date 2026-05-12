"""Find reviewed or published replies that may warrant follow-up."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_MIN_PRIORITY = 1
UNRESOLVED_INTENTS = {"question", "disagreement", "bug_report", "request"}


@dataclass(frozen=True)
class ReplyFollowupOpportunity:
    """One reply follow-up opportunity."""

    target_mention_id: str
    reply_id: int
    age_hours: float
    priority_score: int
    intent: str
    target_handle: str | None
    relationship_stage: str | None
    reason_codes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reason_codes"] = list(self.reason_codes)
        return payload


@dataclass(frozen=True)
class ReplyFollowupOpportunitiesReport:
    """Reply follow-up opportunities report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    opportunities: tuple[ReplyFollowupOpportunity, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_followup_opportunities",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "opportunities": [item.to_dict() for item in self.opportunities],
            "opportunity_count": len(self.opportunities),
            "totals": dict(self.totals),
        }


def build_reply_followup_opportunities_report(
    db_or_conn: Any,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_priority: int = DEFAULT_MIN_PRIORITY,
    stale_after_hours: int = 48,
    now: datetime | None = None,
) -> ReplyFollowupOpportunitiesReport:
    """Build a read-only report of reply follow-up opportunities."""
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if min_priority < 0:
        raise ValueError("min_priority must be non-negative")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    rows = _load_rows(_connection(db_or_conn), cutoff)
    scored = [_score_row(row, generated_at, stale_after_hours) for row in rows]
    deduped = _dedupe(scored)
    opportunities = [item for item in deduped if item.priority_score >= min_priority]
    opportunities.sort(key=lambda item: (-item.priority_score, -item.age_hours, item.reply_id))
    return ReplyFollowupOpportunitiesReport(
        generated_at=generated_at.isoformat(),
        filters={
            "lookback_days": lookback_days,
            "cutoff": cutoff.isoformat(),
            "min_priority": min_priority,
            "stale_after_hours": stale_after_hours,
        },
        totals={
            "replies_scanned": len(rows),
            "deduped_count": len(deduped),
            "opportunity_count": len(opportunities),
        },
        opportunities=tuple(opportunities),
    )


def format_reply_followup_opportunities_json(report: ReplyFollowupOpportunitiesReport) -> str:
    """Serialize as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_followup_opportunities_text(report: ReplyFollowupOpportunitiesReport) -> str:
    """Render follow-up opportunities for operators."""
    lines = [
        "Reply Follow-up Opportunities",
        f"Generated: {report.generated_at}",
        (
            f"Window: lookback_days={report.filters['lookback_days']} "
            f"min_priority={report.filters['min_priority']}"
        ),
        (
            f"Totals: scanned={report.totals['replies_scanned']} "
            f"opportunities={report.totals['opportunity_count']}"
        ),
    ]
    if not report.opportunities:
        lines.extend(["", "No reply follow-up opportunities found."])
        return "\n".join(lines)
    lines.extend(["", "Opportunities:"])
    for item in report.opportunities:
        lines.append(
            f"- target={item.target_mention_id} reply_id={item.reply_id} "
            f"age_hours={item.age_hours:.1f} priority={item.priority_score} "
            f"intent={item.intent} reasons={','.join(item.reason_codes)}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, cutoff: datetime) -> list[sqlite3.Row]:
    tables = {row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "reply_queue" not in tables:
        return []
    return conn.execute(
        """SELECT id, inbound_tweet_id, inbound_author_handle, inbound_text, intent, priority,
                  relationship_context, status, detected_at, reviewed_at, posted_at
             FROM reply_queue
             WHERE status IN ('approved', 'posted')
               AND datetime(COALESCE(detected_at, reviewed_at, posted_at)) >= datetime(?)
             ORDER BY datetime(COALESCE(detected_at, reviewed_at, posted_at)) DESC, id DESC""",
        (cutoff.isoformat(),),
    ).fetchall()


def _score_row(row: sqlite3.Row, now: datetime, stale_after_hours: int) -> ReplyFollowupOpportunity:
    detected = _parse_dt(row["detected_at"] or row["reviewed_at"] or row["posted_at"]) or now
    age_hours = max(0.0, (now - detected).total_seconds() / 3600)
    context = _json_obj(row["relationship_context"])
    stage = context.get("stage") or context.get("relationship_stage")
    strength = _float(context.get("strength"), 0.0)
    tier = str(context.get("tier") or "").lower()
    intent = str(row["intent"] or "other")
    reasons: list[str] = []
    score = 0
    if age_hours >= stale_after_hours:
        reasons.append("stale_inbound_response")
        score += 35 + min(25, int((age_hours - stale_after_hours) // 24) * 5)
    if intent in UNRESOLVED_INTENTS:
        reasons.append("unresolved_intent")
        score += 25
    if stage in {"champion", "customer", "partner"} or tier in {"1", "high", "vip"} or strength >= 0.7:
        reasons.append("high_value_relationship")
        score += 25
    if row["priority"] == "high":
        reasons.append("high_original_priority")
        score += 10
    return ReplyFollowupOpportunity(
        target_mention_id=str(row["inbound_tweet_id"] or ""),
        reply_id=int(row["id"]),
        age_hours=round(age_hours, 1),
        priority_score=min(100, score),
        intent=intent,
        target_handle=str(row["inbound_author_handle"]) if row["inbound_author_handle"] else None,
        relationship_stage=str(stage) if stage else None,
        reason_codes=tuple(reasons),
    )


def _dedupe(items: list[ReplyFollowupOpportunity]) -> list[ReplyFollowupOpportunity]:
    best: dict[tuple[str, str], ReplyFollowupOpportunity] = {}
    for item in items:
        key = (item.target_handle or item.target_mention_id, item.intent)
        current = best.get(key)
        if current is None or (item.priority_score, item.age_hours, -item.reply_id) > (
            current.priority_score,
            current.age_hours,
            -current.reply_id,
        ):
            best[key] = item
    return list(best.values())


def _json_obj(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
