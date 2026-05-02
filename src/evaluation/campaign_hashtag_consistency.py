"""Campaign hashtag consistency reporting."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_MAX_HASHTAGS = 3
QUEUE_STATUSES = {"queued", "held"}
FINDING_TYPES = ("missing_required", "variant", "over_limit")
HASHTAG_RE = re.compile(r"(?<![\w])#([A-Za-z][A-Za-z0-9_]*)")


@dataclass(frozen=True)
class CampaignHashtagFinding:
    """One hashtag consistency finding for a campaign content item."""

    finding_type: str
    campaign_id: int
    campaign_name: str | None
    content_id: int
    planned_topic_id: int | None
    content_type: str | None
    queue_status: str | None
    hashtag: str | None
    canonical_hashtag: str | None
    hashtag_count: int
    examples: tuple[str, ...]
    detail: str

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["examples"] = list(self.examples)
        return data


@dataclass(frozen=True)
class CampaignHashtagSummary:
    """Per-campaign hashtag usage and findings."""

    campaign_id: int
    campaign_name: str | None
    campaign_status: str | None
    content_count: int
    required_hashtags: tuple[str, ...]
    hashtag_usage: dict[str, int]
    examples: dict[str, tuple[str, ...]]
    findings: tuple[CampaignHashtagFinding, ...]

    @property
    def flagged(self) -> bool:
        return bool(self.findings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "campaign_name": self.campaign_name,
            "campaign_status": self.campaign_status,
            "content_count": self.content_count,
            "examples": {
                hashtag: list(values)
                for hashtag, values in sorted(self.examples.items())
            },
            "finding_count": len(self.findings),
            "findings": [finding.to_dict() for finding in self.findings],
            "flagged": self.flagged,
            "hashtag_usage": dict(sorted(self.hashtag_usage.items())),
            "required_hashtags": list(self.required_hashtags),
        }


@dataclass(frozen=True)
class CampaignHashtagConsistencyReport:
    """Campaign hashtag consistency report plus applied filters."""

    generated_at: str
    campaign: str | None
    max_hashtags: int
    campaigns: tuple[CampaignHashtagSummary, ...]
    missing_required_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        by_type = {finding_type: 0 for finding_type in FINDING_TYPES}
        for campaign in self.campaigns:
            for finding in campaign.findings:
                by_type[finding.finding_type] = by_type.get(finding.finding_type, 0) + 1
        return {
            "artifact_type": "campaign_hashtag_consistency",
            "campaign": self.campaign,
            "campaign_count": len(self.campaigns),
            "campaigns": [campaign.to_dict() for campaign in self.campaigns],
            "flagged_campaign_count": sum(1 for campaign in self.campaigns if campaign.flagged),
            "finding_count": sum(len(campaign.findings) for campaign in self.campaigns),
            "findings_by_type": dict(sorted(by_type.items())),
            "generated_at": self.generated_at,
            "max_hashtags": self.max_hashtags,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_required_tables": list(self.missing_required_tables),
        }


def build_campaign_hashtag_consistency_report(
    db_or_conn: Any,
    *,
    campaign: str | int | None = None,
    max_hashtags: int = DEFAULT_MAX_HASHTAGS,
    now: datetime | None = None,
) -> CampaignHashtagConsistencyReport:
    """Return hashtag consistency findings for generated or queued campaign posts."""
    if max_hashtags <= 0:
        raise ValueError("max_hashtags must be positive")
    campaign_filter = _clean(campaign)
    generated_at = _ensure_aware(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or _missing_required_columns(missing_columns):
        return CampaignHashtagConsistencyReport(
            generated_at=generated_at.isoformat(),
            campaign=campaign_filter,
            max_hashtags=max_hashtags,
            campaigns=(),
            missing_required_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_campaign_rows(conn, schema, campaign=campaign_filter)
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(int(row["campaign_id"]), []).append(row)
    campaigns = tuple(
        _campaign_summary(campaign_rows, max_hashtags=max_hashtags)
        for _campaign_id, campaign_rows in sorted(grouped.items())
    )
    return CampaignHashtagConsistencyReport(
        generated_at=generated_at.isoformat(),
        campaign=campaign_filter,
        max_hashtags=max_hashtags,
        campaigns=campaigns,
        missing_required_tables=(),
        missing_columns=missing_columns,
    )


def extract_hashtags(content: str) -> tuple[str, ...]:
    """Extract hashtags case-insensitively while preserving original spellings."""
    seen: set[str] = set()
    hashtags: list[str] = []
    for match in HASHTAG_RE.finditer(content or ""):
        original = "#" + match.group(1)
        canonical = canonicalize_hashtag(original)
        if canonical in seen:
            continue
        seen.add(canonical)
        hashtags.append(original)
    return tuple(hashtags)


def canonicalize_hashtag(hashtag: str) -> str:
    """Return the case-insensitive canonical form used for exact hashtag matching."""
    text = str(hashtag or "").strip()
    if not text:
        return ""
    return "#" + text.lstrip("#").lower()


def format_campaign_hashtag_consistency_json(report: CampaignHashtagConsistencyReport) -> str:
    """Render the report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_campaign_hashtag_consistency_text(report: CampaignHashtagConsistencyReport) -> str:
    """Render a compact terminal report."""
    lines = [
        "Campaign Hashtag Consistency",
        f"Generated: {report.generated_at}",
        f"Campaign: {report.campaign or 'all'}",
        f"Max hashtags: {report.max_hashtags}",
    ]
    if report.missing_required_tables:
        lines.append(f"Missing required tables: {', '.join(report.missing_required_tables)}")
        return "\n".join(lines)
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        ]
        lines.append("Missing columns: " + "; ".join(missing))
    if not report.campaigns:
        lines.append("No generated or queued campaign posts found.")
        return "\n".join(lines)

    for campaign in report.campaigns:
        status = "flagged" if campaign.flagged else "ok"
        required = ", ".join(campaign.required_hashtags) or "-"
        lines.extend(
            [
                "",
                f"Campaign #{campaign.campaign_id} {campaign.campaign_name or '-'} [{status}]",
                f"Content: {campaign.content_count}  Required: {required}",
            ]
        )
        if not campaign.findings:
            lines.append("  No hashtag findings.")
            continue
        for finding in campaign.findings:
            hashtag = finding.hashtag or finding.canonical_hashtag or "-"
            lines.append(
                f"  - content #{finding.content_id}: {finding.finding_type} "
                f"{hashtag} ({finding.detail})"
            )
    return "\n".join(lines)


def _campaign_summary(
    rows: list[dict[str, Any]],
    *,
    max_hashtags: int,
) -> CampaignHashtagSummary:
    content_hashtags: dict[int, tuple[str, ...]] = {}
    content_canonicals: dict[int, set[str]] = {}
    family_usage: dict[str, Counter[str]] = {}
    examples: dict[str, list[str]] = {}
    content_ids_by_family: dict[str, set[int]] = {}

    for row in rows:
        content_id = int(row["content_id"])
        hashtags = extract_hashtags(str(row.get("content") or ""))
        content_hashtags[content_id] = hashtags
        canonicals = {canonicalize_hashtag(hashtag) for hashtag in hashtags}
        content_canonicals[content_id] = canonicals
        for hashtag in hashtags:
            canonical = canonicalize_hashtag(hashtag)
            family = _hashtag_family(canonical)
            family_usage.setdefault(family, Counter())[canonical] += 1
            content_ids_by_family.setdefault(family, set()).add(content_id)
            examples.setdefault(canonical, [])
            if hashtag not in examples[canonical]:
                examples[canonical].append(hashtag)

    required_by_family = _required_hashtags_by_family(family_usage, content_ids_by_family)
    required_hashtags = tuple(
        sorted(required_by_family.values(), key=lambda value: (-len(content_ids_by_family[_hashtag_family(value)]), value))
    )
    findings: list[CampaignHashtagFinding] = []
    for row in rows:
        content_id = int(row["content_id"])
        hashtags = content_hashtags[content_id]
        canonicals = content_canonicals[content_id]
        hashtag_count = len(hashtags)
        if hashtag_count > max_hashtags:
            findings.append(
                _finding(
                    row,
                    finding_type="over_limit",
                    hashtag=None,
                    canonical_hashtag=None,
                    hashtag_count=hashtag_count,
                    examples=hashtags,
                    detail=f"uses {hashtag_count} hashtags; limit is {max_hashtags}",
                )
            )

        for family, required in sorted(required_by_family.items()):
            family_canonicals = {
                canonical
                for canonical in canonicals
                if _hashtag_family(canonical) == family
            }
            if not family_canonicals:
                findings.append(
                    _finding(
                        row,
                        finding_type="missing_required",
                        hashtag=None,
                        canonical_hashtag=required,
                        hashtag_count=hashtag_count,
                        examples=tuple(examples.get(required, [])),
                        detail=f"missing required hashtag {required}",
                    )
                )
                continue
            for canonical in sorted(family_canonicals):
                if canonical != required and family_usage[family][canonical] == 1:
                    findings.append(
                        _finding(
                            row,
                            finding_type="variant",
                            hashtag=_original_for(hashtags, canonical),
                            canonical_hashtag=required,
                            hashtag_count=hashtag_count,
                            examples=tuple(examples.get(required, [])),
                            detail=f"one-off variant of {required}",
                        )
                    )

    first = rows[0]
    usage = {
        canonical: sum(1 for values in content_canonicals.values() if canonical in values)
        for canonical in sorted(examples)
    }
    return CampaignHashtagSummary(
        campaign_id=int(first["campaign_id"]),
        campaign_name=first.get("campaign_name"),
        campaign_status=first.get("campaign_status"),
        content_count=len(content_hashtags),
        required_hashtags=required_hashtags,
        hashtag_usage=usage,
        examples={key: tuple(value[:3]) for key, value in examples.items()},
        findings=tuple(sorted(findings, key=_finding_sort_key)),
    )


def _required_hashtags_by_family(
    family_usage: dict[str, Counter[str]],
    content_ids_by_family: dict[str, set[int]],
) -> dict[str, str]:
    required: dict[str, str] = {}
    for family, counter in family_usage.items():
        if len(content_ids_by_family.get(family, set())) < 2:
            continue
        required[family] = sorted(counter.items(), key=lambda item: (-item[1], item[0]))[0][0]
    return required


def _finding(
    row: dict[str, Any],
    *,
    finding_type: str,
    hashtag: str | None,
    canonical_hashtag: str | None,
    hashtag_count: int,
    examples: tuple[str, ...],
    detail: str,
) -> CampaignHashtagFinding:
    return CampaignHashtagFinding(
        finding_type=finding_type,
        campaign_id=int(row["campaign_id"]),
        campaign_name=row.get("campaign_name"),
        content_id=int(row["content_id"]),
        planned_topic_id=_int_or_none(row.get("planned_topic_id")),
        content_type=row.get("content_type"),
        queue_status=row.get("queue_status"),
        hashtag=hashtag,
        canonical_hashtag=canonical_hashtag,
        hashtag_count=hashtag_count,
        examples=examples,
        detail=detail,
    )


def _load_campaign_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    campaign: str | None,
) -> list[dict[str, Any]]:
    gc_cols = schema["generated_content"]
    pt_cols = schema["planned_topics"]
    cc_cols = schema.get("content_campaigns", set())
    select = [
        "gc.id AS content_id",
        "gc.content",
        "gc.content_type" if "content_type" in gc_cols else "NULL AS content_type",
        "gc.created_at" if "created_at" in gc_cols else "NULL AS created_at",
        "pt.id AS planned_topic_id" if "id" in pt_cols else "NULL AS planned_topic_id",
        "pt.campaign_id",
        "pt.topic" if "topic" in pt_cols else "NULL AS topic",
        "cc.name AS campaign_name" if "name" in cc_cols else "NULL AS campaign_name",
        "cc.status AS campaign_status" if "status" in cc_cols else "NULL AS campaign_status",
        _queue_status_sql(schema),
    ]
    joins = [
        "INNER JOIN planned_topics pt ON pt.content_id = gc.id",
        "LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id" if "content_campaigns" in schema else "",
    ]
    where = ["pt.campaign_id IS NOT NULL"]
    params: list[Any] = []
    if "published" in gc_cols:
        where.append("COALESCE(gc.published, 0) = 0")
    if campaign:
        parsed_id = _parse_positive_int(campaign)
        if parsed_id is not None:
            where.append("pt.campaign_id = ?")
            params.append(parsed_id)
        elif "name" in cc_cols:
            where.append("LOWER(cc.name) = LOWER(?)")
            params.append(campaign)
        else:
            where.append("0 = 1")
    sql = f"""SELECT {', '.join(select)}
              FROM generated_content gc
              {' '.join(join for join in joins if join)}
              WHERE {' AND '.join(where)}
              ORDER BY pt.campaign_id ASC, datetime(gc.created_at) ASC, gc.id ASC"""
    return _fetch_dicts(conn, sql, params)


def _queue_status_sql(schema: dict[str, set[str]]) -> str:
    pieces: list[str] = []
    publish_queue_columns = schema.get("publish_queue", set())
    if {"content_id", "status"}.issubset(publish_queue_columns):
        order_by = "pq.created_at DESC" if "created_at" in publish_queue_columns else "pq.rowid DESC"
        pieces.append(
            "SELECT pq.status FROM publish_queue pq "
            "WHERE pq.content_id = gc.id AND pq.status IN ('queued', 'held') "
            f"ORDER BY {order_by} LIMIT 1"
        )
    publication_columns = schema.get("content_publications", set())
    if {"content_id", "status"}.issubset(publication_columns):
        order_by = "cp.updated_at DESC" if "updated_at" in publication_columns else "cp.rowid DESC"
        pieces.append(
            "SELECT cp.status FROM content_publications cp "
            "WHERE cp.content_id = gc.id AND cp.status IN ('queued', 'held') "
            f"ORDER BY {order_by} LIMIT 1"
        )
    if not pieces:
        return "'generated' AS queue_status"
    return "COALESCE(" + ", ".join(f"({piece})" for piece in pieces) + ", 'generated') AS queue_status"


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "generated_content": {"id", "content"},
        "planned_topics": {"content_id", "campaign_id"},
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    return missing_tables, missing_columns


def _missing_required_columns(missing_columns: dict[str, tuple[str, ...]]) -> bool:
    return any(missing_columns.values())


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


def _fetch_dicts(
    conn: sqlite3.Connection,
    sql: str,
    params: list[Any],
) -> list[dict[str, Any]]:
    cursor = conn.execute(sql, params)
    columns = [description[0] for description in cursor.description]
    rows = cursor.fetchall()
    hydrated: list[dict[str, Any]] = []
    for row in rows:
        if hasattr(row, "keys"):
            hydrated.append({key: row[key] for key in row.keys()})
        else:
            hydrated.append(dict(zip(columns, row, strict=False)))
    return hydrated


def _hashtag_family(hashtag: str) -> str:
    return re.sub(r"[^a-z0-9]", "", canonicalize_hashtag(hashtag).lstrip("#"))


def _original_for(hashtags: tuple[str, ...], canonical: str) -> str | None:
    for hashtag in hashtags:
        if canonicalize_hashtag(hashtag) == canonical:
            return hashtag
    return canonical


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_positive_int(value: str) -> int | None:
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _finding_sort_key(finding: CampaignHashtagFinding) -> tuple[int, int, str, str]:
    order = {name: index for index, name in enumerate(FINDING_TYPES)}
    return (
        finding.content_id,
        order.get(finding.finding_type, 99),
        finding.canonical_hashtag or "",
        finding.hashtag or "",
    )
