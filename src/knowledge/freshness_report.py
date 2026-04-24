"""Freshness reporting for semantic knowledge sources."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import sqlite3
from typing import Any

DEFAULT_STALE_AFTER_DAYS = 14.0


@dataclass(frozen=True)
class KnowledgeSourceFreshness:
    source_type: str
    source_identifier: str
    item_count: int
    newest_item_timestamp: str | None
    oldest_item_timestamp: str | None
    days_since_newest_item: float | None
    license_mix: dict[str, int]
    stale: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_type": self.source_type,
            "source_identifier": self.source_identifier,
            "item_count": self.item_count,
            "newest_item_timestamp": self.newest_item_timestamp,
            "oldest_item_timestamp": self.oldest_item_timestamp,
            "days_since_newest_item": self.days_since_newest_item,
            "license_mix": self.license_mix,
            "stale": self.stale,
        }


@dataclass(frozen=True)
class KnowledgeFreshnessReport:
    stale_after_days: float
    source_type: str | None
    generated_at: str
    stale_source_count: int
    source_count: int
    sources: list[KnowledgeSourceFreshness]

    def to_dict(self) -> dict[str, Any]:
        return {
            "stale_after_days": self.stale_after_days,
            "source_type": self.source_type,
            "generated_at": self.generated_at,
            "stale_source_count": self.stale_source_count,
            "source_count": self.source_count,
            "sources": [source.to_dict() for source in self.sources],
        }


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _days_since(timestamp: str | None, now: datetime) -> float | None:
    parsed = _parse_timestamp(timestamp)
    if parsed is None:
        return None
    return max((now - parsed).total_seconds(), 0.0) / 86400


def _knowledge_columns(conn: sqlite3.Connection) -> set[str]:
    return {row[1] for row in conn.execute("PRAGMA table_info(knowledge)")}


def _timestamp_expression(columns: set[str]) -> str:
    parts = [
        column
        for column in ("published_at", "ingested_at", "created_at")
        if column in columns
    ]
    if not parts:
        return "NULL"
    if len(parts) == 1:
        return parts[0]
    return f"COALESCE({', '.join(parts)})"


def _license_expression(columns: set[str]) -> str:
    if "license" in columns:
        return "COALESCE(NULLIF(TRIM(license), ''), 'attribution_required')"
    return "'attribution_required'"


def _source_identifier_expression(columns: set[str]) -> str:
    parts = []
    if "author" in columns:
        parts.append("NULLIF(TRIM(author), '')")
    if "source_id" in columns:
        parts.append("NULLIF(TRIM(source_id), '')")
    if not parts:
        return "'unknown'"
    return f"COALESCE({', '.join(parts)}, 'unknown')"


def build_knowledge_freshness_report(
    conn: sqlite3.Connection,
    stale_after_days: float = DEFAULT_STALE_AFTER_DAYS,
    source_type: str | None = None,
    now: datetime | None = None,
) -> KnowledgeFreshnessReport:
    """Summarize knowledge freshness grouped by source type and source."""
    if stale_after_days < 0:
        raise ValueError("stale_after_days must be non-negative")

    generated_at = now or datetime.now(timezone.utc)
    if generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=timezone.utc)
    else:
        generated_at = generated_at.astimezone(timezone.utc)

    columns = _knowledge_columns(conn)
    if not columns:
        sources: list[KnowledgeSourceFreshness] = []
        return KnowledgeFreshnessReport(
            stale_after_days=stale_after_days,
            source_type=source_type,
            generated_at=generated_at.isoformat(),
            stale_source_count=0,
            source_count=0,
            sources=sources,
        )

    timestamp_expr = _timestamp_expression(columns)
    license_expr = _license_expression(columns)
    identifier_expr = _source_identifier_expression(columns)

    where = []
    params: list[Any] = []
    if source_type is not None:
        where.append("source_type = ?")
        params.append(source_type)
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""

    cursor = conn.execute(
        f"""SELECT source_type,
                   {identifier_expr} AS source_identifier,
                   {timestamp_expr} AS item_timestamp,
                   {license_expr} AS license
            FROM knowledge
            {where_clause}
            ORDER BY source_type, source_identifier""",
        params,
    )

    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in cursor.fetchall():
        key = (row["source_type"], row["source_identifier"])
        group = grouped.setdefault(
            key,
            {
                "timestamps": [],
                "license_mix": {},
                "item_count": 0,
            },
        )
        group["item_count"] += 1
        if row["item_timestamp"]:
            group["timestamps"].append(row["item_timestamp"])
        license_value = row["license"] or "attribution_required"
        group["license_mix"][license_value] = group["license_mix"].get(license_value, 0) + 1

    sources = []
    for (row_source_type, identifier), group in grouped.items():
        parsed_pairs = [
            (parsed, raw)
            for raw in group["timestamps"]
            if (parsed := _parse_timestamp(raw)) is not None
        ]
        parsed_pairs.sort(key=lambda pair: pair[0])
        oldest = parsed_pairs[0][1] if parsed_pairs else None
        newest = parsed_pairs[-1][1] if parsed_pairs else None
        age_days = _days_since(newest, generated_at)
        stale = age_days is not None and age_days > stale_after_days
        sources.append(
            KnowledgeSourceFreshness(
                source_type=row_source_type,
                source_identifier=identifier,
                item_count=group["item_count"],
                newest_item_timestamp=newest,
                oldest_item_timestamp=oldest,
                days_since_newest_item=age_days,
                license_mix=dict(sorted(group["license_mix"].items())),
                stale=stale,
            )
        )

    sources.sort(
        key=lambda source: (
            not source.stale,
            source.days_since_newest_item is None,
            -(source.days_since_newest_item or 0.0),
            source.source_type,
            source.source_identifier,
        )
    )

    return KnowledgeFreshnessReport(
        stale_after_days=stale_after_days,
        source_type=source_type,
        generated_at=generated_at.isoformat(),
        stale_source_count=sum(1 for source in sources if source.stale),
        source_count=len(sources),
        sources=sources,
    )
