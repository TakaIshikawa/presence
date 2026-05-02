"""Campaign CTA rotation reporting."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_REPEAT = 3

CTA_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "subscribe",
        (
            r"\bsubscribe\b",
            r"\bsign up\b",
            r"\bjoin (?:the )?(?:newsletter|list)\b",
            r"\bget (?:weekly )?updates\b",
        ),
    ),
    (
        "reply",
        (
            r"\breply\b",
            r"\brespond\b",
            r"\btell me\b",
            r"\blet me know\b",
            r"\bcomment\b",
        ),
    ),
    (
        "read",
        (
            r"\bread (?:more|the|this|our)\b",
            r"\blearn more\b",
            r"\bsee the full\b",
            r"\bcheck out\b",
        ),
    ),
    (
        "try",
        (
            r"\btry\b",
            r"\bgive (?:it|this|that) a try\b",
            r"\bstart (?:with|by|today)\b",
            r"\buse this\b",
            r"\brun this\b",
            r"\btest this\b",
        ),
    ),
    (
        "share",
        (
            r"\bshare\b",
            r"\bforward this\b",
            r"\bsend this\b",
            r"\brepost\b",
        ),
    ),
)


@dataclass(frozen=True)
class CampaignCtaMatch:
    """A deterministic CTA family found in one generated content item."""

    content_id: int
    planned_topic_id: int
    topic: str | None
    content_type: str | None
    created_at: str | None
    family: str
    phrase: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "content_id": self.content_id,
            "planned_topic_id": self.planned_topic_id,
            "topic": self.topic,
            "content_type": self.content_type,
            "created_at": self.created_at,
            "family": self.family,
            "phrase": self.phrase,
        }


@dataclass(frozen=True)
class CampaignCtaFamilySummary:
    """Per-campaign CTA family count and examples."""

    family: str
    count: int
    repeated: bool
    content_ids: tuple[int, ...]
    examples: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "family": self.family,
            "count": self.count,
            "repeated": self.repeated,
            "content_ids": list(self.content_ids),
            "examples": list(self.examples),
        }


@dataclass(frozen=True)
class CampaignCtaRotationCampaign:
    """CTA rotation summary for one campaign."""

    campaign_id: int
    campaign_name: str | None
    campaign_status: str | None
    generated_count: int
    cta_count: int
    repeated_families: tuple[CampaignCtaFamilySummary, ...]
    families: tuple[CampaignCtaFamilySummary, ...]
    matches: tuple[CampaignCtaMatch, ...]

    @property
    def flagged(self) -> bool:
        return bool(self.repeated_families)

    def to_dict(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "campaign_name": self.campaign_name,
            "campaign_status": self.campaign_status,
            "generated_count": self.generated_count,
            "cta_count": self.cta_count,
            "flagged": self.flagged,
            "repeated_families": [family.to_dict() for family in self.repeated_families],
            "families": [family.to_dict() for family in self.families],
            "matches": [match.to_dict() for match in self.matches],
        }


@dataclass(frozen=True)
class CampaignCtaRotationReport:
    """Campaign CTA rotation report plus applied filters."""

    days: int
    campaign_id: int | None
    min_repeat: int
    generated_at: str
    period_start: str
    campaigns: tuple[CampaignCtaRotationCampaign, ...]
    content_without_campaign_count: int
    missing_required_tables: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "campaign_cta_rotation",
            "campaign_id": self.campaign_id,
            "days": self.days,
            "generated_at": self.generated_at,
            "period_start": self.period_start,
            "min_repeat": self.min_repeat,
            "campaign_count": len(self.campaigns),
            "flagged_campaign_count": sum(1 for campaign in self.campaigns if campaign.flagged),
            "content_without_campaign_count": self.content_without_campaign_count,
            "missing_required_tables": list(self.missing_required_tables),
            "campaigns": [campaign.to_dict() for campaign in self.campaigns],
        }


def build_campaign_cta_rotation_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    campaign_id: int | None = None,
    min_repeat: int = DEFAULT_MIN_REPEAT,
    now: datetime | None = None,
) -> CampaignCtaRotationReport:
    """Return a read-only report of repeated CTA families by campaign."""
    if days <= 0:
        raise ValueError("days must be positive")
    if campaign_id is not None and campaign_id <= 0:
        raise ValueError("campaign_id must be positive")
    if min_repeat <= 0:
        raise ValueError("min_repeat must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_aware(now or datetime.now(timezone.utc))
    period_start = generated_at - timedelta(days=days)
    missing = tuple(
        table
        for table in ("generated_content", "planned_topics")
        if table not in schema
    )
    if missing:
        return CampaignCtaRotationReport(
            days=days,
            campaign_id=campaign_id,
            min_repeat=min_repeat,
            generated_at=generated_at.isoformat(),
            period_start=period_start.isoformat(),
            campaigns=(),
            content_without_campaign_count=0,
            missing_required_tables=missing,
        )

    rows = _load_generated_rows(
        conn,
        schema,
        campaign_id=campaign_id,
        period_start=period_start,
    )
    without_campaign = sum(1 for row in rows if row.get("campaign_id") is None)
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        if row.get("campaign_id") is None:
            continue
        grouped.setdefault(int(row["campaign_id"]), []).append(row)

    campaigns = tuple(
        _campaign_report(campaign_rows, min_repeat=min_repeat)
        for _campaign_id, campaign_rows in sorted(grouped.items())
    )
    return CampaignCtaRotationReport(
        days=days,
        campaign_id=campaign_id,
        min_repeat=min_repeat,
        generated_at=generated_at.isoformat(),
        period_start=period_start.isoformat(),
        campaigns=campaigns,
        content_without_campaign_count=without_campaign,
        missing_required_tables=missing,
    )


def extract_cta_families(content: str) -> tuple[tuple[str, str], ...]:
    """Extract deterministic CTA family and phrase pairs from generated copy."""
    matches: list[tuple[str, str]] = []
    seen: set[str] = set()
    for phrase in _candidate_phrases(content):
        normalized = phrase.lower()
        for family, patterns in CTA_PATTERNS:
            if family in seen:
                continue
            if any(re.search(pattern, normalized) for pattern in patterns):
                matches.append((family, phrase))
                seen.add(family)
    return tuple(matches)


def format_campaign_cta_rotation_json(report: CampaignCtaRotationReport) -> str:
    """Render a campaign CTA rotation report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_campaign_cta_rotation_text(report: CampaignCtaRotationReport) -> str:
    """Render a compact terminal report."""
    lines = [
        "Campaign CTA Rotation",
        f"Generated: {report.generated_at}",
        f"Window: {report.days} days",
        f"Campaign: {report.campaign_id if report.campaign_id is not None else 'all'}",
        f"Min repeat: {report.min_repeat}",
    ]
    if report.missing_required_tables:
        lines.append(f"Missing required tables: {', '.join(report.missing_required_tables)}")
        return "\n".join(lines)

    lines.append(f"Content without campaign metadata: {report.content_without_campaign_count}")
    if not report.campaigns:
        lines.append("No campaign-linked generated content found.")
        return "\n".join(lines)

    for campaign in report.campaigns:
        status = "flagged" if campaign.flagged else "ok"
        lines.extend(
            [
                "",
                f"Campaign #{campaign.campaign_id} {campaign.campaign_name or '-'} [{status}]",
                f"Generated: {campaign.generated_count}  CTAs: {campaign.cta_count}",
            ]
        )
        if not campaign.families:
            lines.append("  No CTA families found.")
            continue
        for family in campaign.families:
            marker = "!" if family.repeated else "-"
            example = f"  e.g. {_shorten(family.examples[0], 72)}" if family.examples else ""
            lines.append(f"  {marker} {family.family}: {family.count} content item(s){example}")
    return "\n".join(lines)


def _load_generated_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    campaign_id: int | None,
    period_start: datetime,
) -> list[dict[str, Any]]:
    gc_cols = schema["generated_content"]
    pt_cols = schema["planned_topics"]
    if not {"id", "content"}.issubset(gc_cols) or not {"content_id", "campaign_id"}.issubset(pt_cols):
        return []

    select = [
        "gc.id AS content_id",
        "gc.content",
        "gc.content_type" if "content_type" in gc_cols else "NULL AS content_type",
        "gc.created_at" if "created_at" in gc_cols else "NULL AS created_at",
        "pt.id AS planned_topic_id" if "id" in pt_cols else "NULL AS planned_topic_id",
        "pt.campaign_id",
        "pt.topic" if "topic" in pt_cols else "NULL AS topic",
    ]
    joins = "LEFT JOIN planned_topics pt ON pt.content_id = gc.id"
    if "content_campaigns" in schema:
        joins += " LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id"
        select.extend(
            [
                "cc.name AS campaign_name" if "name" in schema["content_campaigns"] else "NULL AS campaign_name",
                "cc.status AS campaign_status" if "status" in schema["content_campaigns"] else "NULL AS campaign_status",
            ]
        )
    else:
        select.extend(["NULL AS campaign_name", "NULL AS campaign_status"])

    where = []
    params: list[Any] = []
    if "created_at" in gc_cols:
        where.append("datetime(gc.created_at) >= datetime(?)")
        params.append(period_start.isoformat())
    if campaign_id is not None:
        where.append("pt.campaign_id = ?")
        params.append(campaign_id)
    sql = f"""SELECT {', '.join(select)}
              FROM generated_content gc
              {joins}"""
    if where:
        sql += f" WHERE {' AND '.join(where)}"
    sql += " ORDER BY pt.campaign_id ASC NULLS LAST, datetime(gc.created_at) DESC, gc.id DESC"
    cursor = conn.execute(sql, params)
    return _cursor_rows_to_dicts(cursor)


def _campaign_report(
    rows: list[dict[str, Any]],
    *,
    min_repeat: int,
) -> CampaignCtaRotationCampaign:
    matches: list[CampaignCtaMatch] = []
    content_ids = {int(row["content_id"]) for row in rows}
    for row in rows:
        for family, phrase in extract_cta_families(str(row.get("content") or "")):
            matches.append(
                CampaignCtaMatch(
                    content_id=int(row["content_id"]),
                    planned_topic_id=int(row.get("planned_topic_id") or 0),
                    topic=row.get("topic"),
                    content_type=row.get("content_type"),
                    created_at=row.get("created_at"),
                    family=family,
                    phrase=phrase,
                )
            )

    families = tuple(_family_summaries(matches, min_repeat=min_repeat))
    repeated = tuple(family for family in families if family.repeated)
    first = rows[0]
    return CampaignCtaRotationCampaign(
        campaign_id=int(first["campaign_id"]),
        campaign_name=first.get("campaign_name"),
        campaign_status=first.get("campaign_status"),
        generated_count=len(content_ids),
        cta_count=len(matches),
        repeated_families=repeated,
        families=families,
        matches=tuple(matches),
    )


def _family_summaries(
    matches: list[CampaignCtaMatch],
    *,
    min_repeat: int,
) -> list[CampaignCtaFamilySummary]:
    by_family: dict[str, list[CampaignCtaMatch]] = {}
    for match in matches:
        by_family.setdefault(match.family, []).append(match)

    summaries: list[CampaignCtaFamilySummary] = []
    for family in sorted(by_family):
        family_matches = by_family[family]
        content_ids = tuple(sorted({match.content_id for match in family_matches}))
        examples = _dedupe_preserving_order([match.phrase for match in family_matches])[:3]
        summaries.append(
            CampaignCtaFamilySummary(
                family=family,
                count=len(content_ids),
                repeated=len(content_ids) >= min_repeat,
                content_ids=content_ids,
                examples=tuple(examples),
            )
        )
    return sorted(summaries, key=lambda item: (-item.count, item.family))


def _candidate_phrases(content: str) -> list[str]:
    cleaned = re.sub(r"```.*?```", " ", content, flags=re.DOTALL)
    cleaned = re.sub(r"https?://\S+", " ", cleaned)
    pieces = re.split(r"(?:\n+|(?<=[.!?])\s+)", cleaned)
    phrases: list[str] = []
    for piece in pieces:
        phrase = re.sub(r"^[>\-*+\d.)\s]+", "", piece.strip())
        phrase = re.sub(r"\s+", " ", phrase).strip()
        if phrase:
            phrases.append(_shorten(phrase, 160))
    return phrases


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if hasattr(row, "keys") else row[0]
        schema[str(table)] = {
            str(column["name"] if hasattr(column, "keys") else column[1])
            for column in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
    return schema


def _cursor_rows_to_dicts(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    columns = [description[0] for description in cursor.description]
    rows = cursor.fetchall()
    hydrated: list[dict[str, Any]] = []
    for row in rows:
        if hasattr(row, "keys"):
            hydrated.append({key: row[key] for key in row.keys()})
        else:
            hydrated.append(dict(zip(columns, row, strict=False)))
    return hydrated


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _dedupe_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        key = " ".join(value.lower().split())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def _shorten(value: str, width: int) -> str:
    text = " ".join(str(value).split())
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)].rstrip() + "..."
