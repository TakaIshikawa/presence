"""Read-only freshness report for approved semantic knowledge items."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse
import sqlite3


RECOMMEND_REVIEW_SOURCE = "review_source"
RECOMMEND_REFRESH = "refresh"
RECOMMEND_RETIRE = "retire"

RECOMMENDATION_ORDER = (
    RECOMMEND_REVIEW_SOURCE,
    RECOMMEND_REFRESH,
    RECOMMEND_RETIRE,
)

CURATED_SOURCE_TYPES = {
    "curated_x": "x_account",
    "curated_article": "blog",
    "curated_newsletter": "newsletter",
}


@dataclass(frozen=True)
class FreshnessFinding:
    knowledge_id: int
    source_type: str
    source_id: str | None
    source_url: str | None
    author: str | None
    insight: str | None
    content_preview: str
    published_at: str | None
    ingested_at: str | None
    created_at: str | None
    age_days: float | None
    last_used_at: str | None
    usage_count: int
    unused_days: float | None
    stale: bool
    unused: bool
    inactive_source: bool
    recommendations: list[str] = field(default_factory=list)
    inactive_source_metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "knowledge_id": self.knowledge_id,
            "source_type": self.source_type,
            "source_id": self.source_id,
            "source_url": self.source_url,
            "author": self.author,
            "insight": self.insight,
            "content_preview": self.content_preview,
            "published_at": self.published_at,
            "ingested_at": self.ingested_at,
            "created_at": self.created_at,
            "age_days": self.age_days,
            "last_used_at": self.last_used_at,
            "usage_count": self.usage_count,
            "unused_days": self.unused_days,
            "stale": self.stale,
            "unused": self.unused,
            "inactive_source": self.inactive_source,
            "recommendations": list(self.recommendations),
            "inactive_source_metadata": self.inactive_source_metadata,
        }


def parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    else:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def age_days(value: Any, now: datetime | None = None) -> float | None:
    parsed = parse_timestamp(value)
    if parsed is None:
        return None
    effective_now = now or datetime.now(timezone.utc)
    if effective_now.tzinfo is None:
        effective_now = effective_now.replace(tzinfo=timezone.utc)
    else:
        effective_now = effective_now.astimezone(timezone.utc)
    return max((effective_now - parsed).total_seconds(), 0.0) / 86400.0


def build_freshness_report(
    conn: sqlite3.Connection,
    *,
    stale_days: int = 180,
    unused_days: int = 90,
    source_type: str | None = None,
    limit: int | None = None,
    now: datetime | None = None,
) -> list[FreshnessFinding]:
    """Return freshness findings for approved knowledge without mutating rows."""
    if stale_days < 1:
        raise ValueError("stale_days must be at least 1")
    if unused_days < 1:
        raise ValueError("unused_days must be at least 1")
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1 when provided")

    conn.row_factory = sqlite3.Row
    effective_now = now or datetime.now(timezone.utc)
    inactive_sources = _load_inactive_sources(conn)
    usage_by_knowledge = _load_usage(conn)

    sql = "SELECT * FROM knowledge WHERE approved = 1"
    params: list[Any] = []
    if source_type:
        sql += " AND source_type = ?"
        params.append(source_type)
    sql += " ORDER BY id ASC"

    findings: list[FreshnessFinding] = []
    for row in conn.execute(sql, params).fetchall():
        source_timestamp = row["published_at"] or row["ingested_at"]
        source_age = age_days(source_timestamp, effective_now)
        is_stale = source_age is None or source_age >= stale_days

        usage = usage_by_knowledge.get(row["id"], {"usage_count": 0, "last_used_at": None})
        usage_count = int(usage["usage_count"] or 0)
        last_used_at = usage["last_used_at"]
        unused_age = age_days(last_used_at or row["ingested_at"] or row["created_at"], effective_now)
        is_unused = usage_count == 0 and (unused_age is None or unused_age >= unused_days)

        inactive_source = _match_inactive_source(row, inactive_sources)
        has_inactive_source = inactive_source is not None

        recommendations = _recommendations(
            stale=is_stale,
            unused=is_unused,
            inactive_source=has_inactive_source,
        )
        if not recommendations:
            continue

        findings.append(
            FreshnessFinding(
                knowledge_id=row["id"],
                source_type=row["source_type"],
                source_id=row["source_id"],
                source_url=row["source_url"],
                author=row["author"],
                insight=row["insight"],
                content_preview=_preview(row["content"]),
                published_at=row["published_at"],
                ingested_at=row["ingested_at"],
                created_at=row["created_at"],
                age_days=_round_days(source_age),
                last_used_at=last_used_at,
                usage_count=usage_count,
                unused_days=_round_days(unused_age),
                stale=is_stale,
                unused=is_unused,
                inactive_source=has_inactive_source,
                recommendations=recommendations,
                inactive_source_metadata=inactive_source,
            )
        )

    findings.sort(key=_finding_sort_key)
    if limit is not None:
        return findings[:limit]
    return findings


def report_to_dict(
    findings: list[FreshnessFinding],
    *,
    stale_days: int,
    unused_days: int,
    source_type: str | None,
    limit: int | None,
) -> dict[str, Any]:
    return {
        "stale_days": stale_days,
        "unused_days": unused_days,
        "source_type": source_type,
        "limit": limit,
        "finding_count": len(findings),
        "findings": [finding.to_dict() for finding in findings],
    }


def _load_usage(conn: sqlite3.Connection) -> dict[int, dict[str, Any]]:
    usage: dict[int, dict[str, Any]] = {}
    for table in ("content_knowledge_links", "reply_knowledge_links"):
        if not _table_exists(conn, table):
            continue
        for row in conn.execute(
            f"""SELECT knowledge_id, COUNT(*) AS usage_count, MAX(created_at) AS last_used_at
                FROM {table}
                GROUP BY knowledge_id"""
        ).fetchall():
            current = usage.setdefault(
                row["knowledge_id"], {"usage_count": 0, "last_used_at": None}
            )
            current["usage_count"] += int(row["usage_count"] or 0)
            current["last_used_at"] = _max_timestamp(
                current["last_used_at"], row["last_used_at"]
            )
    return usage


def _load_inactive_sources(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(conn, "curated_sources"):
        return []
    rows = conn.execute(
        """SELECT id, source_type, identifier, name, license, feed_url, active, status,
                  last_fetch_status, consecutive_failures, last_success_at,
                  last_failure_at, last_error
           FROM curated_sources
           WHERE COALESCE(active, 1) = 0 OR COALESCE(status, 'active') != 'active'"""
    ).fetchall()
    return [dict(row) for row in rows]


def _match_inactive_source(
    row: sqlite3.Row, inactive_sources: list[dict[str, Any]]
) -> dict[str, Any] | None:
    curated_type = CURATED_SOURCE_TYPES.get(row["source_type"])
    if not curated_type:
        return None

    candidate_values = {
        _normalize_identifier(row["author"]),
        _normalize_identifier(row["source_id"]),
        _normalize_identifier(_host(row["source_url"])),
        _normalize_identifier(_host(row["source_id"])),
    }
    candidate_values.discard("")

    for source in inactive_sources:
        if source["source_type"] != curated_type:
            continue
        identifier = _normalize_identifier(source["identifier"])
        if identifier and identifier in candidate_values:
            return source
    return None


def _recommendations(
    *, stale: bool, unused: bool, inactive_source: bool
) -> list[str]:
    labels: list[str] = []
    if inactive_source:
        labels.append(RECOMMEND_REVIEW_SOURCE)
    if stale:
        labels.append(RECOMMEND_REFRESH)
    if unused:
        labels.append(RECOMMEND_RETIRE)
    return [label for label in RECOMMENDATION_ORDER if label in labels]


def _finding_sort_key(finding: FreshnessFinding) -> tuple[int, float, int]:
    primary_rank = min(
        RECOMMENDATION_ORDER.index(label) for label in finding.recommendations
    )
    return (
        primary_rank,
        -(finding.age_days or 0.0),
        finding.knowledge_id,
    )


def _normalize_identifier(value: str | None) -> str:
    text = (value or "").strip().lower()
    if text.startswith("@"):
        text = text[1:]
    if text.startswith("www."):
        text = text[4:]
    return text.rstrip("/")


def _host(value: str | None) -> str:
    if not value:
        return ""
    parsed = urlparse(value if "://" in value else f"https://{value}")
    return parsed.netloc


def _max_timestamp(left: str | None, right: str | None) -> str | None:
    if left is None:
        return right
    if right is None:
        return left
    left_dt = parse_timestamp(left)
    right_dt = parse_timestamp(right)
    if left_dt is None:
        return right
    if right_dt is None:
        return left
    return left if left_dt >= right_dt else right


def _preview(value: str | None, limit: int = 140) -> str:
    text = (value or "").replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _round_days(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 2)


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None
