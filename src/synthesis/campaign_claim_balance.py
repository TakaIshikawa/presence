"""Report claim-style balance across campaign-linked content."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 90
CLAIM_TYPES = (
    "metric_claim",
    "lesson_claim",
    "process_claim",
    "caveat_claim",
    "story_claim",
    "question_claim",
)

_METRIC_RE = re.compile(
    r"(\b\d+(?:\.\d+)?\s?(?:%|x\b|k\b|m\b|hours?\b|days?\b|weeks?\b|months?\b|"
    r"users?\b|posts?\b|commits?\b|prs?\b|issues?\b|runs?\b|minutes?\b)|"
    r"\$ ?\d+|\b\d+/\d+\b)",
    re.IGNORECASE,
)
_QUESTION_RE = re.compile(
    r"\?\s*$|^\s*(why|how|what|where|which|should|could|can)\b",
    re.IGNORECASE,
)
_LESSON_RE = re.compile(
    r"\b(learned|lesson|takeaway|mistake|realized|taught|what worked|what failed|"
    r"would do differently)\b",
    re.IGNORECASE,
)
_PROCESS_RE = re.compile(
    r"\b(process|workflow|checklist|playbook|framework|steps?|sequence|pipeline|"
    r"system|method|runbook|how we|how i)\b",
    re.IGNORECASE,
)
_CAVEAT_RE = re.compile(
    r"\b(but|however|although|though|unless|except|trade-?off|caveat|risk|"
    r"limitation|constraint|downside|not always|depends)\b",
    re.IGNORECASE,
)
_STORY_RE = re.compile(
    r"\b(when we|when i|once|today|yesterday|last week|last month|we tried|"
    r"i tried|we shipped|i shipped|story|behind the scenes)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class CampaignClaimBalance:
    """Claim-style distribution for one campaign."""

    campaign_id: int
    campaign_name: str
    content_count: int
    claim_type_distribution: dict[str, int]
    claim_type_shares: dict[str, float]
    dominant_claim_type: str | None
    imbalance_score: float
    sample_content_ids: tuple[int, ...]
    suggested_next_claim_types: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["sample_content_ids"] = list(self.sample_content_ids)
        payload["suggested_next_claim_types"] = list(self.suggested_next_claim_types)
        return payload


@dataclass(frozen=True)
class CampaignClaimBalanceReport:
    """Read-only campaign claim-balance report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    campaigns: tuple[CampaignClaimBalance, ...]
    missing_required_tables: tuple[str, ...] = ()
    missing_required_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": self.filters,
            "totals": self.totals,
            "campaigns": [campaign.to_dict() for campaign in self.campaigns],
            "missing_required_tables": list(self.missing_required_tables),
            "missing_required_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_required_columns or {}).items())
            },
        }


def build_campaign_claim_balance_report(
    db_or_conn: Any,
    *,
    campaign_id: int | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    now: datetime | None = None,
) -> CampaignClaimBalanceReport:
    """Evaluate claim-style balance for campaign-linked generated content."""
    if campaign_id is not None and campaign_id <= 0:
        raise ValueError("campaign_id must be positive")
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")

    current = _as_utc(now or datetime.now(timezone.utc))
    cutoff = current - timedelta(days=lookback_days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _missing_requirements(schema)
    filters = {"campaign_id": campaign_id, "lookback_days": lookback_days}
    if missing_tables or missing_columns:
        return CampaignClaimBalanceReport(
            generated_at=current.isoformat(),
            filters=filters,
            totals={"campaign_count": 0, "content_count": 0},
            campaigns=(),
            missing_required_tables=tuple(sorted(missing_tables)),
            missing_required_columns=missing_columns,
        )

    campaigns = _load_campaigns(conn, campaign_id)
    if campaign_id is not None and not campaigns:
        raise ValueError(f"Campaign {campaign_id} does not exist")

    rows_by_campaign = _load_content_rows(
        conn,
        campaign_ids=[int(campaign["id"]) for campaign in campaigns],
        cutoff=cutoff,
        now=current,
    )
    campaign_reports = tuple(
        _campaign_balance(campaign, rows_by_campaign.get(int(campaign["id"]), []))
        for campaign in campaigns
    )
    return CampaignClaimBalanceReport(
        generated_at=current.isoformat(),
        filters=filters,
        totals={
            "campaign_count": len(campaign_reports),
            "content_count": sum(item.content_count for item in campaign_reports),
        },
        campaigns=campaign_reports,
    )


def classify_claim_type(text: str | None, content_format: str | None = None) -> str:
    """Classify content into one deterministic claim-style bucket."""
    combined = f"{content_format or ''} {text or ''}".strip()
    if not combined:
        return "process_claim"
    normalized = re.sub(r"\s+", " ", combined).strip().lower()
    content_format = str(content_format or "").strip().lower()
    if "question" in content_format or _QUESTION_RE.search(normalized):
        return "question_claim"
    if _METRIC_RE.search(normalized):
        return "metric_claim"
    if _LESSON_RE.search(normalized):
        return "lesson_claim"
    if _PROCESS_RE.search(normalized):
        return "process_claim"
    if _CAVEAT_RE.search(normalized):
        return "caveat_claim"
    if _STORY_RE.search(normalized):
        return "story_claim"
    return "process_claim"


def format_campaign_claim_balance_json(report: CampaignClaimBalanceReport) -> str:
    """Serialize a campaign claim-balance report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_campaign_claim_balance_text(report: CampaignClaimBalanceReport) -> str:
    """Render a campaign claim-balance report for terminal review."""
    lines = [
        "Campaign Claim Balance",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['lookback_days']} days",
        f"Campaign: {report.filters['campaign_id'] or 'all'}",
        (
            "Totals: "
            f"campaigns={report.totals['campaign_count']} "
            f"content={report.totals['content_count']}"
        ),
    ]
    if report.missing_required_tables:
        lines.append("Missing required tables: " + ", ".join(report.missing_required_tables))
    if report.missing_required_columns:
        details = ", ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_required_columns.items())
        )
        lines.append("Missing required columns: " + details)
    if not report.campaigns:
        lines.append("No campaigns found for the selected filters.")
        return "\n".join(lines)

    lines.append("Campaigns:")
    for campaign in report.campaigns:
        distribution = ", ".join(
            f"{claim_type}={campaign.claim_type_distribution[claim_type]}"
            for claim_type in CLAIM_TYPES
            if campaign.claim_type_distribution[claim_type]
        ) or "none"
        suggestions = ", ".join(campaign.suggested_next_claim_types) or "none"
        samples = ", ".join(str(content_id) for content_id in campaign.sample_content_ids) or "none"
        lines.append(
            f"- #{campaign.campaign_id} {campaign.campaign_name}: "
            f"content={campaign.content_count} "
            f"dominant={campaign.dominant_claim_type or 'none'} "
            f"imbalance={campaign.imbalance_score:.3f}"
        )
        lines.append(f"  distribution: {distribution}")
        lines.append(f"  suggested_next_claim_types: {suggestions}")
        lines.append(f"  sample_content_ids: {samples}")
    return "\n".join(lines)


def _campaign_balance(
    campaign: dict[str, Any],
    rows: list[dict[str, Any]],
) -> CampaignClaimBalance:
    counts = Counter({claim_type: 0 for claim_type in CLAIM_TYPES})
    samples_by_type: dict[str, list[int]] = {claim_type: [] for claim_type in CLAIM_TYPES}
    for row in rows:
        claim_type = classify_claim_type(row.get("content"), row.get("content_format"))
        counts[claim_type] += 1
        samples_by_type[claim_type].append(int(row["content_id"]))

    total = sum(counts.values())
    shares = {
        claim_type: round(counts[claim_type] / total, 4) if total else 0.0
        for claim_type in CLAIM_TYPES
    }
    dominant = None
    if total:
        dominant = max(CLAIM_TYPES, key=lambda claim_type: (counts[claim_type], claim_type))
    suggestions = _suggested_next_claim_types(counts, total)
    sample_ids = _sample_content_ids(samples_by_type, dominant, suggestions)
    return CampaignClaimBalance(
        campaign_id=int(campaign["id"]),
        campaign_name=str(campaign.get("name") or f"Campaign {campaign['id']}"),
        content_count=total,
        claim_type_distribution={claim_type: counts[claim_type] for claim_type in CLAIM_TYPES},
        claim_type_shares=shares,
        dominant_claim_type=dominant,
        imbalance_score=_imbalance_score(counts, total),
        sample_content_ids=sample_ids,
        suggested_next_claim_types=suggestions,
    )


def _suggested_next_claim_types(counts: Counter[str], total: int) -> tuple[str, ...]:
    if total == 0:
        return CLAIM_TYPES[:3]
    ordered = sorted(
        CLAIM_TYPES,
        key=lambda claim_type: (counts[claim_type], CLAIM_TYPES.index(claim_type)),
    )
    return tuple(ordered[:3])


def _imbalance_score(counts: Counter[str], total: int) -> float:
    if total == 0:
        return 0.0
    dominant_share = max(counts.values()) / total
    ideal_share = 1 / len(CLAIM_TYPES)
    return round(max(0.0, (dominant_share - ideal_share) / (1 - ideal_share)), 3)


def _sample_content_ids(
    samples_by_type: dict[str, list[int]],
    dominant: str | None,
    suggestions: tuple[str, ...],
) -> tuple[int, ...]:
    prioritized = list(suggestions)
    if dominant:
        prioritized.append(dominant)
    sample_ids: list[int] = []
    for claim_type in prioritized:
        for content_id in samples_by_type.get(claim_type, []):
            if content_id not in sample_ids:
                sample_ids.append(content_id)
            if len(sample_ids) >= 5:
                return tuple(sample_ids)
    for content_ids in samples_by_type.values():
        for content_id in content_ids:
            if content_id not in sample_ids:
                sample_ids.append(content_id)
            if len(sample_ids) >= 5:
                return tuple(sample_ids)
    return tuple(sample_ids)


def _load_campaigns(
    conn: sqlite3.Connection,
    campaign_id: int | None,
) -> list[dict[str, Any]]:
    if campaign_id is not None:
        return _fetch_dicts(
            conn,
            "SELECT id, name, goal, status, created_at FROM content_campaigns WHERE id = ?",
            (campaign_id,),
        )
    return _fetch_dicts(
        conn,
        """SELECT id, name, goal, status, created_at
           FROM content_campaigns
           ORDER BY status ASC, created_at ASC, id ASC""",
        (),
    )


def _load_content_rows(
    conn: sqlite3.Connection,
    *,
    campaign_ids: list[int],
    cutoff: datetime,
    now: datetime,
) -> dict[int, list[dict[str, Any]]]:
    if not campaign_ids:
        return {}
    placeholders = ",".join("?" for _ in campaign_ids)
    rows = _fetch_dicts(
        conn,
        f"""SELECT pt.campaign_id,
                   gc.id AS content_id,
                   gc.content,
                   gc.content_format,
                   gc.created_at,
                   gc.published_at
            FROM planned_topics pt
            INNER JOIN generated_content gc ON gc.id = pt.content_id
            WHERE pt.campaign_id IN ({placeholders})
              AND (
                  datetime(gc.created_at) BETWEEN datetime(?) AND datetime(?)
                  OR datetime(gc.published_at) BETWEEN datetime(?) AND datetime(?)
              )
            ORDER BY pt.campaign_id ASC,
                     datetime(COALESCE(gc.published_at, gc.created_at)) DESC,
                     gc.id ASC""",
        (
            *campaign_ids,
            cutoff.isoformat(),
            now.isoformat(),
            cutoff.isoformat(),
            now.isoformat(),
        ),
    )
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(int(row["campaign_id"]), []).append(row)
    return grouped


def _missing_requirements(
    schema: dict[str, set[str]],
) -> tuple[list[str], dict[str, tuple[str, ...]]]:
    required = {
        "content_campaigns": ("id", "name"),
        "planned_topics": ("campaign_id", "content_id"),
        "generated_content": ("id", "content", "created_at", "published_at", "content_format"),
    }
    missing_tables = [table for table in required if table not in schema]
    missing_columns = {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in required.items()
        if table in schema
        and any(column not in schema.get(table, set()) for column in columns)
    }
    return missing_tables, missing_columns


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


def _fetch_dicts(
    conn: sqlite3.Connection,
    query: str,
    params: tuple[Any, ...] | list[Any],
) -> list[dict[str, Any]]:
    cursor = conn.execute(query, params)
    names = [column[0] for column in cursor.description or ()]
    return [
        {
            name: row[name] if hasattr(row, "keys") else row[index]
            for index, name in enumerate(names)
        }
        for row in cursor.fetchall()
    ]


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
