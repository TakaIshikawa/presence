"""Report campaign evidence freshness and sparsity."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Sequence


DEFAULT_MAX_AGE_DAYS = 90
STATUSES = ("fresh", "aging", "stale", "insufficient")
OPTIONAL_TABLES = (
    "content_topics",
    "generated_content",
    "content_knowledge_links",
    "knowledge",
)


@dataclass(frozen=True)
class CampaignEvidenceAgingItem:
    """Evidence freshness for one campaign."""

    campaign_id: int
    campaign_name: str | None
    campaign_status: str | None
    planned_topic_count: int
    generated_content_count: int
    linked_knowledge_count: int
    oldest_evidence_date: str | None
    newest_evidence_date: str | None
    evidence_age_days: int | None
    topic_coverage: dict[str, Any]
    status: str
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "campaign_name": self.campaign_name,
            "campaign_status": self.campaign_status,
            "evidence_age_days": self.evidence_age_days,
            "generated_content_count": self.generated_content_count,
            "linked_knowledge_count": self.linked_knowledge_count,
            "newest_evidence_date": self.newest_evidence_date,
            "oldest_evidence_date": self.oldest_evidence_date,
            "planned_topic_count": self.planned_topic_count,
            "reasons": list(self.reasons),
            "status": self.status,
            "topic_coverage": dict(self.topic_coverage),
        }


@dataclass(frozen=True)
class CampaignEvidenceAgingReport:
    """Deterministic read-only campaign evidence aging report."""

    generated_at: str
    filters: dict[str, Any]
    total_items: int
    summary: dict[str, Any]
    items: tuple[CampaignEvidenceAgingItem, ...]
    missing_required_tables: tuple[str, ...] = ()
    missing_required_columns: dict[str, tuple[str, ...]] | None = None
    missing_optional_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "campaign_evidence_aging",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "items": [item.to_dict() for item in self.items],
            "missing_optional_tables": list(self.missing_optional_tables),
            "missing_required_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_required_columns or {}).items())
            },
            "missing_required_tables": list(self.missing_required_tables),
            "summary": self.summary,
            "total_items": self.total_items,
        }


def build_campaign_evidence_aging_report(
    db_or_conn: Any,
    *,
    max_age_days: int = DEFAULT_MAX_AGE_DAYS,
    campaign_id: int | None = None,
    status: str | None = None,
    now: datetime | None = None,
) -> CampaignEvidenceAgingReport:
    """Build evidence aging status for active or explicitly selected campaigns."""
    if max_age_days < 0:
        raise ValueError("max_age_days must be non-negative")
    normalized_status = _normalize_status(status)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_aware(now or datetime.now(timezone.utc))
    filters = {
        "campaign_id": campaign_id,
        "campaign_status": None if campaign_id is not None else "active",
        "max_age_days": max_age_days,
        "status": normalized_status,
    }

    required = {
        "content_campaigns": {"id"},
        "planned_topics": {"id", "campaign_id"},
    }
    missing_required_tables = tuple(table for table in required if table not in schema)
    missing_required_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and not columns.issubset(schema[table])
    }
    missing_optional_tables = tuple(table for table in OPTIONAL_TABLES if table not in schema)
    if missing_required_tables or missing_required_columns:
        return _empty_report(
            generated_at,
            filters,
            missing_required_tables=missing_required_tables,
            missing_required_columns=missing_required_columns,
            missing_optional_tables=missing_optional_tables,
        )

    campaigns = _campaign_rows(conn, schema, campaign_id=campaign_id)
    if campaign_id is not None and not campaigns:
        raise ValueError(f"Campaign {campaign_id} does not exist")

    items = tuple(
        item
        for item in (
            _campaign_item(
                conn,
                schema,
                campaign,
                max_age_days=max_age_days,
                now=generated_at,
            )
            for campaign in campaigns
        )
        if normalized_status is None or item.status == normalized_status
    )
    return CampaignEvidenceAgingReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        total_items=len(items),
        summary=_summary(items, campaigns_scanned=len(campaigns)),
        items=items,
        missing_optional_tables=missing_optional_tables,
    )


def format_campaign_evidence_aging_json(report: CampaignEvidenceAgingReport) -> str:
    """Render deterministic JSON suitable for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_campaign_evidence_aging_text(report: CampaignEvidenceAgingReport) -> str:
    """Render a compact human-readable evidence aging report."""
    filters = report.filters
    lines = [
        "Campaign Evidence Aging",
        (
            "Filters: "
            f"max_age_days={filters.get('max_age_days')} "
            f"campaign_id={filters.get('campaign_id') or 'all'} "
            f"campaign_status={filters.get('campaign_status') or 'any'} "
            f"status={filters.get('status') or 'any'}"
        ),
        "Items: "
        f"{report.total_items} fresh={report.summary.get('fresh', 0)} "
        f"aging={report.summary.get('aging', 0)} "
        f"stale={report.summary.get('stale', 0)} "
        f"insufficient={report.summary.get('insufficient', 0)}",
    ]
    if report.missing_required_tables:
        lines.append("Missing required tables: " + ", ".join(report.missing_required_tables))
    if report.missing_required_columns:
        lines.append(
            "Missing required columns: "
            + ", ".join(
                f"{table}.{column}"
                for table, columns in sorted(report.missing_required_columns.items())
                for column in columns
            )
        )
    if report.missing_optional_tables:
        lines.append("Missing optional tables: " + ", ".join(report.missing_optional_tables))
    if not report.items:
        lines.append("No campaign evidence aging items found.")
        return "\n".join(lines)

    lines.append("Campaigns:")
    for item in report.items:
        coverage = item.topic_coverage
        age = item.evidence_age_days if item.evidence_age_days is not None else "n/a"
        covered = coverage.get("topics_with_linked_knowledge", 0)
        planned = coverage.get("planned_topic_count", 0)
        lines.append(
            "  - "
            f"campaign_id={item.campaign_id} name={item.campaign_name or 'n/a'} "
            f"status={item.status} evidence_age_days={age} "
            f"planned={item.planned_topic_count} generated={item.generated_content_count} "
            f"knowledge={item.linked_knowledge_count} "
            f"topics_with_evidence={covered}/{planned}"
        )
        for reason in item.reasons:
            lines.append(f"    - {reason}")
    return "\n".join(lines)


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    missing_required_tables: tuple[str, ...] = (),
    missing_required_columns: dict[str, tuple[str, ...]] | None = None,
    missing_optional_tables: tuple[str, ...] = (),
) -> CampaignEvidenceAgingReport:
    return CampaignEvidenceAgingReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        total_items=0,
        summary=_summary((), campaigns_scanned=0),
        items=(),
        missing_required_tables=missing_required_tables,
        missing_required_columns=missing_required_columns,
        missing_optional_tables=missing_optional_tables,
    )


def _campaign_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    campaign_id: int | None,
) -> list[dict[str, Any]]:
    columns = schema["content_campaigns"]
    selected = [
        "id",
        _column_expr(columns, "name"),
        _column_expr(columns, "status"),
        _column_expr(columns, "start_date"),
        _column_expr(columns, "end_date"),
        _column_expr(columns, "created_at"),
    ]
    params: list[Any] = []
    where = []
    if campaign_id is not None:
        where.append("id = ?")
        params.append(campaign_id)
    elif "status" in columns:
        where.append("status = 'active'")
    sql = f"SELECT {', '.join(selected)} FROM content_campaigns"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY start_date ASC NULLS LAST, created_at ASC, id ASC"
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _campaign_item(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    campaign: dict[str, Any],
    *,
    max_age_days: int,
    now: datetime,
) -> CampaignEvidenceAgingItem:
    campaign_id = int(campaign["id"])
    topic_rows = _topic_rows(conn, schema, campaign_id)
    content_ids = _content_ids(topic_rows)
    generated_count = _generated_content_count(conn, schema, content_ids)
    linked_knowledge_count, knowledge_dates = _knowledge_evidence(conn, schema, content_ids)
    oldest = min(knowledge_dates) if knowledge_dates else None
    newest = max(knowledge_dates) if knowledge_dates else None
    evidence_age_days = (now.date() - newest.date()).days if newest else None
    coverage = _topic_coverage(conn, schema, topic_rows, content_ids)
    status, reasons = _status_and_reasons(
        planned_topic_count=len(topic_rows),
        generated_content_count=generated_count,
        linked_knowledge_count=linked_knowledge_count,
        evidence_age_days=evidence_age_days,
        max_age_days=max_age_days,
        topics_with_linked_knowledge=int(coverage["topics_with_linked_knowledge"]),
    )
    return CampaignEvidenceAgingItem(
        campaign_id=campaign_id,
        campaign_name=campaign.get("name"),
        campaign_status=campaign.get("status"),
        planned_topic_count=len(topic_rows),
        generated_content_count=generated_count,
        linked_knowledge_count=linked_knowledge_count,
        oldest_evidence_date=oldest.isoformat() if oldest else None,
        newest_evidence_date=newest.isoformat() if newest else None,
        evidence_age_days=evidence_age_days,
        topic_coverage=coverage,
        status=status,
        reasons=reasons,
    )


def _topic_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    campaign_id: int,
) -> list[dict[str, Any]]:
    columns = schema["planned_topics"]
    selected = [
        "id",
        _column_expr(columns, "topic"),
        _column_expr(columns, "content_id"),
        _column_expr(columns, "status"),
    ]
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM planned_topics
            WHERE campaign_id = ?
            ORDER BY id ASC""",
        (campaign_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _content_ids(topic_rows: Sequence[dict[str, Any]]) -> tuple[int, ...]:
    return tuple(
        sorted({
            int(row["content_id"])
            for row in topic_rows
            if row.get("content_id") is not None
        })
    )


def _generated_content_count(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: Sequence[int],
) -> int:
    if not content_ids:
        return 0
    if "generated_content" not in schema or "id" not in schema["generated_content"]:
        return len(content_ids)
    placeholders = ", ".join("?" for _ in content_ids)
    row = conn.execute(
        f"SELECT COUNT(DISTINCT id) FROM generated_content WHERE id IN ({placeholders})",
        list(content_ids),
    ).fetchone()
    return int(row[0] or 0)


def _knowledge_evidence(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: Sequence[int],
) -> tuple[int, tuple[datetime, ...]]:
    if not content_ids or not {"content_knowledge_links", "knowledge"}.issubset(schema):
        return 0, ()
    ckl_columns = schema["content_knowledge_links"]
    knowledge_columns = schema["knowledge"]
    if not {"content_id", "knowledge_id"}.issubset(ckl_columns) or "id" not in knowledge_columns:
        return 0, ()
    date_expr = _knowledge_date_expr(knowledge_columns)
    placeholders = ", ".join("?" for _ in content_ids)
    rows = conn.execute(
        f"""SELECT DISTINCT k.id AS knowledge_id,
                   {date_expr} AS evidence_date
            FROM content_knowledge_links ckl
            INNER JOIN knowledge k ON k.id = ckl.knowledge_id
            WHERE ckl.content_id IN ({placeholders})
            ORDER BY datetime({date_expr}) ASC, k.id ASC""",
        list(content_ids),
    ).fetchall()
    parsed = tuple(
        timestamp
        for timestamp in (_parse_timestamp(row["evidence_date"]) for row in rows)
        if timestamp is not None
    )
    return len(rows), parsed


def _topic_coverage(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    topic_rows: Sequence[dict[str, Any]],
    content_ids: Sequence[int],
) -> dict[str, Any]:
    planned_topics = tuple(
        str(row.get("topic")).strip()
        for row in topic_rows
        if row.get("topic") is not None and str(row.get("topic")).strip()
    )
    content_topics = _content_topics(conn, schema, content_ids)
    linked_content_ids = _linked_content_ids(conn, schema, content_ids)
    topics_with_generated = {
        str(row.get("topic")).strip().lower()
        for row in topic_rows
        if row.get("content_id") is not None and row.get("topic") is not None
    }
    topics_with_evidence = {
        str(row.get("topic")).strip().lower()
        for row in topic_rows
        if _content_id(row) in linked_content_ids and row.get("topic") is not None
    }
    planned_topic_keys = {topic.lower() for topic in planned_topics}
    generated_content_topic_keys = {topic.lower() for topic in content_topics}
    covered_keys = topics_with_evidence | (planned_topic_keys & generated_content_topic_keys)
    planned_count = len(topic_rows)
    return {
        "content_topics": sorted(content_topics),
        "coverage_ratio": round(len(covered_keys) / planned_count, 3) if planned_count else 0.0,
        "planned_topic_count": planned_count,
        "planned_topics": sorted(set(planned_topics), key=str.lower),
        "topics_with_generated_content": len(topics_with_generated),
        "topics_with_linked_knowledge": len(topics_with_evidence),
        "topics_covered": len(covered_keys),
    }


def _content_topics(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: Sequence[int],
) -> tuple[str, ...]:
    if not content_ids or "content_topics" not in schema:
        return ()
    columns = schema["content_topics"]
    if not {"content_id", "topic"}.issubset(columns):
        return ()
    placeholders = ", ".join("?" for _ in content_ids)
    rows = conn.execute(
        f"""SELECT DISTINCT topic
            FROM content_topics
            WHERE content_id IN ({placeholders})
              AND topic IS NOT NULL
            ORDER BY lower(topic) ASC""",
        list(content_ids),
    ).fetchall()
    return tuple(str(row["topic"]) for row in rows if str(row["topic"]).strip())


def _linked_content_ids(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: Sequence[int],
) -> set[int]:
    if not content_ids or not {"content_knowledge_links", "knowledge"}.issubset(schema):
        return set()
    if not {"content_id", "knowledge_id"}.issubset(schema["content_knowledge_links"]):
        return set()
    placeholders = ", ".join("?" for _ in content_ids)
    rows = conn.execute(
        f"""SELECT DISTINCT ckl.content_id
            FROM content_knowledge_links ckl
            INNER JOIN knowledge k ON k.id = ckl.knowledge_id
            WHERE ckl.content_id IN ({placeholders})""",
        list(content_ids),
    ).fetchall()
    return {int(row["content_id"]) for row in rows}


def _status_and_reasons(
    *,
    planned_topic_count: int,
    generated_content_count: int,
    linked_knowledge_count: int,
    evidence_age_days: int | None,
    max_age_days: int,
    topics_with_linked_knowledge: int,
) -> tuple[str, tuple[str, ...]]:
    reasons: list[str] = []
    if planned_topic_count == 0:
        reasons.append("Add planned topics before assessing campaign evidence freshness.")
    if generated_content_count == 0 and planned_topic_count > 0:
        reasons.append("Generate content for planned topics so evidence can be linked.")
    if linked_knowledge_count == 0:
        reasons.append("Link current knowledge evidence to generated campaign content.")
    if topics_with_linked_knowledge < planned_topic_count and planned_topic_count > 0:
        reasons.append("Refresh evidence coverage for planned topics without linked knowledge.")

    if reasons:
        return "insufficient", tuple(reasons)
    if evidence_age_days is None:
        return "insufficient", ("Link dated knowledge evidence to campaign content.",)
    if evidence_age_days > max_age_days:
        return "stale", (
            f"Newest linked evidence is {evidence_age_days} days old; "
            "refresh evidence before continuing.",
        )
    if evidence_age_days > max_age_days // 2:
        return "aging", (
            f"Newest linked evidence is {evidence_age_days} days old; "
            "queue evidence refresh soon.",
        )
    return "fresh", ("Linked evidence is within the freshness window.",)


def _summary(
    items: Sequence[CampaignEvidenceAgingItem],
    *,
    campaigns_scanned: int,
) -> dict[str, Any]:
    counts = Counter(item.status for item in items)
    return {
        "aging": counts.get("aging", 0),
        "campaigns_scanned": campaigns_scanned,
        "fresh": counts.get("fresh", 0),
        "insufficient": counts.get("insufficient", 0),
        "stale": counts.get("stale", 0),
        "status_counts": {status: counts.get(status, 0) for status in STATUSES},
    }


def _knowledge_date_expr(columns: set[str]) -> str:
    candidates = [
        column
        for column in ("published_at", "ingested_at", "created_at")
        if column in columns
    ]
    if not candidates:
        return "NULL"
    return "COALESCE(" + ", ".join(f"k.{column}" for column in candidates) + ")"


def _column_expr(columns: set[str], column: str) -> str:
    return f"{column} AS {column}" if column in columns else f"NULL AS {column}"


def _content_id(row: dict[str, Any]) -> int | None:
    value = row.get("content_id")
    if value is None:
        return None
    return int(value)


def _normalize_status(status: str | None) -> str | None:
    if status is None:
        return None
    normalized = str(status).strip().lower()
    if normalized not in STATUSES:
        raise ValueError(f"status must be one of: {', '.join(STATUSES)}")
    return normalized


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _ensure_aware(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _ensure_aware(parsed)


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or Database-like object with conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        str(row["name"]): {
            str(column["name"])
            for column in conn.execute(f"PRAGMA table_info({row['name']})").fetchall()
        }
        for row in tables
    }
