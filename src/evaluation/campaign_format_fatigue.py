"""Detect campaign or topic groups overusing one content format."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Mapping


DEFAULT_DAYS = 30
DEFAULT_MIN_COUNT = 3
DEFAULT_DOMINANT_SHARE = 0.75
GROUP_KEYS = ("campaign", "campaign_id", "topic", "theme")
FORMAT_KEYS = ("format", "content_format")


@dataclass(frozen=True)
class CampaignFormatFatigueExample:
    """One generated content row contributing to a format fatigue group."""

    content_id: int
    content_type: str | None
    content_format: str
    created_at: str | None
    published_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CampaignFormatFatigueGroup:
    """One campaign/topic group dominated by a single format."""

    group_type: str
    group_key: str
    content_count: int
    format_counts: dict[str, int]
    dominant_format: str
    dominant_share: float
    examples: tuple[CampaignFormatFatigueExample, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "content_count": self.content_count,
            "dominant_format": self.dominant_format,
            "dominant_share": self.dominant_share,
            "examples": [example.to_dict() for example in self.examples],
            "format_counts": dict(sorted(self.format_counts.items())),
            "group_key": self.group_key,
            "group_type": self.group_type,
        }


@dataclass(frozen=True)
class CampaignFormatFatigueReport:
    """Campaign format fatigue report plus schema/filter metadata."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    groups: tuple[CampaignFormatFatigueGroup, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    @property
    def has_fatigue(self) -> bool:
        return bool(self.groups)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "campaign_format_fatigue",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "groups": [group.to_dict() for group in self.groups],
            "has_fatigue": self.has_fatigue,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(self.totals),
        }


def build_campaign_format_fatigue_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_count: int = DEFAULT_MIN_COUNT,
    dominant_share: float = DEFAULT_DOMINANT_SHARE,
    now: datetime | None = None,
) -> CampaignFormatFatigueReport:
    """Return groups whose recent content is dominated by one format."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_count <= 0:
        raise ValueError("min_count must be positive")
    if dominant_share <= 0 or dominant_share > 1:
        raise ValueError("dominant_share must be > 0 and <= 1")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "dominant_share": dominant_share,
        "lookback_start": cutoff.isoformat(),
        "lookback_end": generated_at.isoformat(),
        "min_count": min_count,
    }
    missing_tables, missing_columns = _schema_gaps(schema)
    if "generated_content" not in schema or "id" not in schema.get("generated_content", set()):
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    rows = _load_rows(conn, schema, cutoff=cutoff)
    group_items: dict[tuple[str, str], list[dict[str, Any]]] = {}
    malformed_metadata_count = 0
    ungrouped_count = 0
    for row in rows:
        metadata, malformed = _merged_metadata(row)
        malformed_metadata_count += malformed
        group_type, group_key = _group_key(row, metadata)
        if not group_key:
            ungrouped_count += 1
            continue
        item = dict(row)
        item["content_format"] = _content_format(row, metadata)
        group_items.setdefault((group_type, group_key), []).append(item)

    groups = [
        group
        for group_rows in group_items.values()
        if (
            group := _fatigue_group(
                group_rows,
                min_count=min_count,
                dominant_share=dominant_share,
            )
        )
        is not None
    ]
    groups.sort(key=lambda group: (-group.dominant_share, -group.content_count, group.group_type, group.group_key))
    return CampaignFormatFatigueReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "rows_scanned": len(rows),
            "group_count": len(group_items),
            "fatigue_group_count": len(groups),
            "flagged_content_count": sum(group.content_count for group in groups),
            "malformed_metadata_count": malformed_metadata_count,
            "ungrouped_count": ungrouped_count,
        },
        groups=tuple(groups),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_campaign_format_fatigue_json(report: CampaignFormatFatigueReport) -> str:
    """Serialize the campaign format fatigue report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_campaign_format_fatigue_text(report: CampaignFormatFatigueReport) -> str:
    """Render campaign format fatigue for command-line review."""
    totals = report.totals
    lines = [
        "Campaign Format Fatigue",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['days']} days "
            f"min_count={report.filters['min_count']} "
            f"dominant_share={report.filters['dominant_share']}"
        ),
        (
            "Totals: "
            f"rows_scanned={totals['rows_scanned']} "
            f"groups={totals['group_count']} "
            f"fatigue_groups={totals['fatigue_group_count']}"
        ),
    ]
    if totals["malformed_metadata_count"]:
        lines.append(f"Malformed metadata rows: {totals['malformed_metadata_count']}")
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
            if columns
        ]
        if missing:
            lines.append("Missing columns: " + "; ".join(missing))

    if not report.groups:
        lines.extend(["", "No campaign format fatigue found."])
        return "\n".join(lines)

    lines.extend(["", "Fatigue groups:"])
    for group in report.groups:
        counts = ", ".join(f"{name}={count}" for name, count in sorted(group.format_counts.items()))
        lines.append(
            f"  - {group.group_type}={group.group_key} "
            f"dominant_format={group.dominant_format} "
            f"share={group.dominant_share:.2f} count={group.content_count} "
            f"formats={counts}"
        )
        examples = ", ".join(str(example.content_id) for example in group.examples)
        lines.append(f"      content_ids={examples}")
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    gc = schema["generated_content"]
    select_columns = [
        "gc.id AS content_id",
        _column_expr(gc, "content_type", "gc", "content_type"),
        _column_expr(gc, "content_format", "gc", "content_format"),
        _column_expr(gc, "created_at", "gc", "created_at"),
        _column_expr(gc, "published_at", "gc", "published_at"),
        _column_expr(gc, "metadata", "gc", "generated_metadata"),
        _column_expr(schema.get("planned_topics", set()), "topic", "pt", "planned_topic"),
        _column_expr(schema.get("content_campaigns", set()), "id", "cc", "campaign_id"),
        _column_expr(schema.get("content_campaigns", set()), "name", "cc", "campaign_name"),
        _column_expr(schema.get("content_topics", set()), "topic", "ct", "content_topic"),
        _column_expr(schema.get("content_publications", set()), "published_at", "cp", "publication_published_at"),
        _column_expr(schema.get("content_variants", set()), "metadata", "cv", "variant_metadata"),
    ]
    joins = []
    if _can_join(schema, "planned_topics", {"content_id"}):
        joins.append("LEFT JOIN planned_topics pt ON pt.content_id = gc.id")
    else:
        joins.append("LEFT JOIN (SELECT NULL AS content_id, NULL AS topic, NULL AS campaign_id) pt ON 0")
    if _can_join(schema, "content_campaigns", {"id"}) and "campaign_id" in schema.get("planned_topics", set()):
        joins.append("LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id")
    else:
        joins.append("LEFT JOIN (SELECT NULL AS id, NULL AS name) cc ON 0")
    if _can_join(schema, "content_topics", {"content_id"}):
        joins.append("LEFT JOIN content_topics ct ON ct.content_id = gc.id")
    else:
        joins.append("LEFT JOIN (SELECT NULL AS content_id, NULL AS topic) ct ON 0")
    if _can_join(schema, "content_publications", {"content_id"}):
        joins.append("LEFT JOIN content_publications cp ON cp.content_id = gc.id")
    else:
        joins.append("LEFT JOIN (SELECT NULL AS content_id, NULL AS published_at) cp ON 0")
    if _can_join(schema, "content_variants", {"content_id"}):
        selected_filter = "AND cv.selected = 1" if "selected" in schema["content_variants"] else ""
        joins.append(f"LEFT JOIN content_variants cv ON cv.content_id = gc.id {selected_filter}")
    else:
        joins.append("LEFT JOIN (SELECT NULL AS content_id, NULL AS metadata) cv ON 0")

    where_parts = []
    params: list[Any] = []
    if "created_at" in gc:
        where_parts.append("gc.created_at >= ?")
        params.append(cutoff.isoformat())
    if "published_at" in gc:
        where_parts.append("gc.published_at >= ?")
        params.append(cutoff.isoformat())
    if "content_publications" in schema and "published_at" in schema["content_publications"]:
        where_parts.append("cp.published_at >= ?")
        params.append(cutoff.isoformat())
    where_sql = f"WHERE {' OR '.join(where_parts)}" if where_parts else ""

    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM generated_content gc
            {' '.join(joins)}
            {where_sql}
            ORDER BY gc.id ASC""",
        tuple(params),
    ).fetchall()
    return _dedupe_rows([dict(row) for row in rows])


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[int, dict[str, Any]] = {}
    for row in rows:
        content_id = int(row["content_id"])
        if content_id not in merged:
            merged[content_id] = row
            continue
        current = merged[content_id]
        for key in ("planned_topic", "campaign_id", "campaign_name", "content_topic", "publication_published_at", "variant_metadata"):
            if not current.get(key) and row.get(key):
                current[key] = row[key]
    return list(merged.values())


def _fatigue_group(
    rows: list[dict[str, Any]],
    *,
    min_count: int,
    dominant_share: float,
) -> CampaignFormatFatigueGroup | None:
    if len(rows) < min_count:
        return None
    counts = Counter(str(row["content_format"]) for row in rows)
    dominant_format, dominant_count = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0]
    share = dominant_count / len(rows)
    if share < dominant_share:
        return None
    first = rows[0]
    examples = tuple(
        CampaignFormatFatigueExample(
            content_id=int(row["content_id"]),
            content_type=row.get("content_type"),
            content_format=str(row["content_format"]),
            created_at=row.get("created_at"),
            published_at=row.get("published_at") or row.get("publication_published_at"),
        )
        for row in sorted(rows, key=lambda item: int(item["content_id"]))[:5]
    )
    metadata, _malformed = _merged_metadata(first)
    group_type, group_key = _group_key(first, metadata)
    return CampaignFormatFatigueGroup(
        group_type=group_type,
        group_key=group_key,
        content_count=len(rows),
        format_counts=dict(counts),
        dominant_format=dominant_format,
        dominant_share=round(share, 4),
        examples=examples,
    )


def _group_key(row: dict[str, Any], metadata: Mapping[str, Any]) -> tuple[str, str]:
    for key in ("campaign", "campaign_id"):
        value = _clean(metadata.get(key))
        if value:
            return "campaign", value
    if row.get("campaign_id"):
        name = _clean(row.get("campaign_name"))
        suffix = f":{name}" if name else ""
        return "campaign", f"{row['campaign_id']}{suffix}"
    for key in ("topic", "theme"):
        value = _clean(metadata.get(key))
        if value:
            return key, value
    value = _clean(row.get("planned_topic") or row.get("content_topic"))
    return ("topic", value) if value else ("ungrouped", "")


def _content_format(row: dict[str, Any], metadata: Mapping[str, Any]) -> str:
    for key in FORMAT_KEYS:
        value = _clean(metadata.get(key))
        if value:
            return value
    return _clean(row.get("content_format") or row.get("content_type")) or "unknown"


def _merged_metadata(row: dict[str, Any]) -> tuple[dict[str, Any], int]:
    merged: dict[str, Any] = {}
    malformed = 0
    for field in ("generated_metadata", "variant_metadata"):
        metadata, bad = _metadata_object(row.get(field))
        malformed += int(bad)
        merged.update(metadata)
    return merged, malformed


def _metadata_object(raw_value: Any) -> tuple[Mapping[str, Any], bool]:
    if raw_value in (None, ""):
        return {}, False
    if isinstance(raw_value, Mapping):
        return raw_value, False
    try:
        parsed = json.loads(raw_value)
    except (TypeError, json.JSONDecodeError):
        return {}, True
    if not isinstance(parsed, Mapping):
        return {}, True
    return parsed, False


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    optional_tables = ("planned_topics", "content_campaigns", "content_topics", "content_publications", "content_variants")
    missing_tables = tuple(table for table in ("generated_content", *optional_tables) if table not in schema)
    required_columns = {
        "generated_content": {"id", "content_type", "content_format", "created_at"},
        "planned_topics": {"content_id", "topic", "campaign_id"},
        "content_campaigns": {"id", "name"},
        "content_topics": {"content_id", "topic"},
        "content_publications": {"content_id", "published_at"},
        "content_variants": {"content_id", "metadata"},
    }
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required_columns.items()
        if table in schema and columns - schema[table]
    }
    return missing_tables, missing_columns


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> CampaignFormatFatigueReport:
    return CampaignFormatFatigueReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "rows_scanned": 0,
            "group_count": 0,
            "fatigue_group_count": 0,
            "flagged_content_count": 0,
            "malformed_metadata_count": 0,
            "ungrouped_count": 0,
        },
        groups=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3 connection or database wrapper with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    }
    return {
        table: {
            str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
            for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")
        }
        for table in tables
    }


def _column_expr(columns: set[str], column: str, alias: str, output: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"NULL AS {output}"


def _can_join(schema: dict[str, set[str]], table: str, columns: set[str]) -> bool:
    return table in schema and columns.issubset(schema[table])


def _clean(value: Any) -> str:
    return " ".join(str(value).split()) if value not in (None, "") else ""


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
