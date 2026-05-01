"""Read-only planner for stale or incomplete link metadata."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any
from urllib.parse import parse_qsl, urlsplit

from .link_metadata_enricher import (
    SOURCE_TYPES,
    TRACKING_QUERY_PARAMS,
    TRACKING_QUERY_PREFIXES,
    normalize_canonical_url,
)


DEFAULT_STALE_DAYS = 30
DEFAULT_LIMIT = 25
METADATA_FIELDS = ("canonical_url", "title", "site_name", "image", "published_at")
REFRESHED_AT_KEYS = (
    "refreshed_at",
    "fetched_at",
    "enriched_at",
    "updated_at",
    "last_refreshed_at",
    "metadata_refreshed_at",
)


@dataclass(frozen=True)
class LinkMetadataRefreshCandidate:
    """One stored URL that should be refreshed by the enrichment workflow."""

    source_table: str
    row_id: int
    source_type: str
    source_id: str
    url: str
    canonical_url: str | None
    priority: int
    missing_fields: tuple[str, ...]
    stale_fields: tuple[str, ...]
    refresh_reason: str
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def plan_link_metadata_refresh(
    db_or_conn: Any,
    *,
    source_type: str = "all",
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return rows whose link metadata should be refreshed without fetching URLs."""
    if source_type not in SOURCE_TYPES:
        raise ValueError(f"source_type must be one of: {', '.join(SOURCE_TYPES)}")
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    now = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=stale_days)

    scanned_count = 0
    skipped_no_url_count = 0
    candidates: list[LinkMetadataRefreshCandidate] = []

    for row in _source_rows(conn, schema, source_type):
        scanned_count += 1
        candidate, skipped = _candidate_for_row(row, cutoff)
        if skipped:
            skipped_no_url_count += 1
        elif candidate is not None:
            candidates.append(candidate)

    candidates.sort(
        key=lambda item: (
            -item.priority,
            item.source_table,
            item.source_type,
            item.row_id,
        )
    )
    limited_candidates = candidates[:limit] if limit is not None else candidates
    reason_counts: dict[str, int] = {}
    for candidate in candidates:
        for reason in candidate.reasons:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

    return {
        "generated_at": now.isoformat(),
        "filters": {
            "source_type": source_type,
            "stale_days": stale_days,
            "limit": limit,
        },
        "summary": {
            "scanned_count": scanned_count,
            "candidate_count": len(candidates),
            "returned_count": len(limited_candidates),
            "skipped_no_url_count": skipped_no_url_count,
            "reason_counts": dict(sorted(reason_counts.items())),
        },
        "candidates": [candidate.to_dict() for candidate in limited_candidates],
    }


def format_link_metadata_refresh_json(report: dict[str, Any]) -> str:
    """Render a refresh plan as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_link_metadata_refresh_text(report: dict[str, Any]) -> str:
    """Render a compact operator-facing refresh plan."""
    filters = report["filters"]
    summary = report["summary"]
    lines = [
        "Link metadata refresh plan",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: source_type={filters['source_type']} "
            f"stale_days={filters['stale_days']} "
            f"limit={filters['limit'] if filters['limit'] is not None else '-'}"
        ),
        (
            "Totals: "
            f"scanned={summary['scanned_count']} "
            f"candidates={summary['candidate_count']} "
            f"returned={summary['returned_count']} "
            f"skipped_no_url={summary['skipped_no_url_count']}"
        ),
        "",
    ]
    if not report["candidates"]:
        lines.append("No stale or incomplete link metadata found.")
        return "\n".join(lines)

    columns = [
        ("source_table", "TABLE", 15),
        ("row_id", "ROW", 6),
        ("source_type", "TYPE", 18),
        ("priority", "PRI", 4),
        ("refresh_reason", "REASON", 26),
        ("missing", "MISSING", 38),
        ("url", "URL", 52),
    ]
    lines.append("  ".join(label.ljust(width) for _, label, width in columns))
    lines.append("  ".join("-" * width for _, _, width in columns))
    for candidate in report["candidates"]:
        rendered = dict(candidate)
        rendered["missing"] = ",".join(candidate["missing_fields"]) or "-"
        lines.append(
            "  ".join(
                _clip(rendered.get(key) if rendered.get(key) is not None else "-", width).ljust(width)
                for key, _, width in columns
            )
        )
    return "\n".join(lines)


def _source_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    source_type: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if source_type in {"all", "knowledge", "curated_article", "curated_newsletter"}:
        rows.extend(_knowledge_rows(conn, schema, source_type))
    if source_type in {"all", "curated_sources", "blog", "newsletter"}:
        rows.extend(_curated_source_rows(conn, schema, source_type))
    return rows


def _knowledge_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    source_type: str,
) -> list[dict[str, Any]]:
    columns = schema.get("knowledge", set())
    required = {"id", "source_type", "source_id"}
    if not required.issubset(columns):
        return []
    selected = (
        ("curated_article", "curated_newsletter")
        if source_type in {"all", "knowledge"}
        else (source_type,)
    )
    placeholders = ",".join("?" for _ in selected)
    select = {
        "id": "id",
        "source_type": "source_type",
        "source_id": "source_id",
        "source_url": _column_expr(columns, "source_url"),
        "published_at": _column_expr(columns, "published_at"),
        "metadata": _column_expr(columns, "metadata"),
        "created_at": _column_expr(columns, "created_at"),
    }
    return [
        {
            **dict(row),
            "source_table": "knowledge",
        }
        for row in conn.execute(
            f"""SELECT {select['id']} AS id,
                       {select['source_type']} AS source_type,
                       {select['source_id']} AS source_id,
                       {select['source_url']} AS source_url,
                       {select['published_at']} AS published_at,
                       {select['metadata']} AS metadata,
                       {select['created_at']} AS created_at
                FROM knowledge
                WHERE source_type IN ({placeholders})
                ORDER BY {select['created_at']} ASC, id ASC""",
            selected,
        ).fetchall()
    ]


def _curated_source_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    source_type: str,
) -> list[dict[str, Any]]:
    columns = schema.get("curated_sources", set())
    required = {"id", "source_type", "identifier"}
    if not required.issubset(columns):
        return []
    selected = ("blog", "newsletter") if source_type in {"all", "curated_sources"} else (source_type,)
    placeholders = ",".join("?" for _ in selected)
    filters = [f"source_type IN ({placeholders})"]
    if "status" in columns:
        filters.append("status = 'active'")
    elif "active" in columns:
        filters.append("active = 1")
    select = {
        "id": "id",
        "source_type": "source_type",
        "source_id": "identifier",
        "source_url": _column_expr(columns, "feed_url"),
        "canonical_url": _column_expr(columns, "canonical_url"),
        "title": _column_expr(columns, "link_title"),
        "site_name": _column_expr(columns, "site_name"),
        "published_at": _column_expr(columns, "published_at"),
        "metadata": _column_expr(columns, "metadata"),
        "created_at": _column_expr(columns, "created_at"),
    }
    return [
        {
            **dict(row),
            "source_table": "curated_sources",
        }
        for row in conn.execute(
            f"""SELECT {select['id']} AS id,
                       {select['source_type']} AS source_type,
                       {select['source_id']} AS source_id,
                       {select['source_url']} AS source_url,
                       {select['canonical_url']} AS canonical_url,
                       {select['title']} AS title,
                       {select['site_name']} AS site_name,
                       {select['published_at']} AS published_at,
                       {select['metadata']} AS metadata,
                       {select['created_at']} AS created_at
                FROM curated_sources
                WHERE {" AND ".join(filters)}
                ORDER BY {select['created_at']} ASC, id ASC""",
            selected,
        ).fetchall()
    ]


def _candidate_for_row(
    row: dict[str, Any],
    stale_cutoff: datetime,
) -> tuple[LinkMetadataRefreshCandidate | None, bool]:
    url = _row_url(row)
    if not url:
        return None, True

    metadata = _parse_metadata(row.get("metadata"))
    link_metadata = metadata.get("link_metadata")
    if not isinstance(link_metadata, dict):
        link_metadata = {}

    field_values = _field_values(row, link_metadata)
    missing_fields = tuple(field for field in METADATA_FIELDS if not _has_text(field_values.get(field)))
    reasons: list[str] = []
    stale_fields: tuple[str, ...] = ()

    if missing_fields:
        reasons.append("missing_metadata")

    canonical_url = field_values.get("canonical_url")
    if _canonical_conflict(url, canonical_url):
        reasons.append("canonical_conflict")

    if _has_tracking_query_param(url):
        reasons.append("tracking_source_url")

    refreshed_at = _metadata_refreshed_at(row, link_metadata, metadata)
    if refreshed_at is not None and refreshed_at < stale_cutoff:
        stale_fields = tuple(field for field in METADATA_FIELDS if _has_text(field_values.get(field)))
        reasons.append("stale_metadata")

    if not reasons:
        return None, False

    reason_order = (
        "canonical_conflict",
        "missing_metadata",
        "stale_metadata",
        "tracking_source_url",
    )
    ordered_reasons = tuple(reason for reason in reason_order if reason in reasons)
    return (
        LinkMetadataRefreshCandidate(
            source_table=str(row["source_table"]),
            row_id=int(row["id"]),
            source_type=str(row.get("source_type") or ""),
            source_id=str(row.get("source_id") or ""),
            url=url,
            canonical_url=str(canonical_url).strip() if _has_text(canonical_url) else None,
            priority=_priority(ordered_reasons, missing_fields),
            missing_fields=missing_fields,
            stale_fields=stale_fields,
            refresh_reason=ordered_reasons[0],
            reasons=ordered_reasons,
        ),
        False,
    )


def _field_values(row: dict[str, Any], link_metadata: dict[str, Any]) -> dict[str, Any]:
    if row["source_table"] == "knowledge":
        return {
            "canonical_url": link_metadata.get("canonical_url"),
            "title": link_metadata.get("title"),
            "site_name": link_metadata.get("site_name"),
            "image": link_metadata.get("image"),
            "published_at": row.get("published_at") or link_metadata.get("published_at"),
        }
    return {
        "canonical_url": row.get("canonical_url") or link_metadata.get("canonical_url"),
        "title": row.get("title") or link_metadata.get("title"),
        "site_name": row.get("site_name") or link_metadata.get("site_name"),
        "image": link_metadata.get("image") or row.get("image"),
        "published_at": row.get("published_at") or link_metadata.get("published_at"),
    }


def _row_url(row: dict[str, Any]) -> str:
    source_url = str(row.get("source_url") or "").strip()
    if source_url:
        return source_url
    source_id = str(row.get("source_id") or "").strip()
    if source_id.startswith(("http://", "https://")):
        return source_id
    return ""


def _canonical_conflict(url: str, canonical_url: Any) -> bool:
    if not _has_text(canonical_url):
        return False
    return normalize_canonical_url(url) != normalize_canonical_url(str(canonical_url))


def _metadata_refreshed_at(
    row: dict[str, Any],
    link_metadata: dict[str, Any],
    metadata: dict[str, Any],
) -> datetime | None:
    for source in (link_metadata, metadata, row):
        for key in REFRESHED_AT_KEYS:
            parsed = _parse_datetime(source.get(key))
            if parsed is not None:
                return parsed
    return None


def _priority(reasons: tuple[str, ...], missing_fields: tuple[str, ...]) -> int:
    priority = 0
    if "canonical_conflict" in reasons:
        priority += 90
    if "missing_metadata" in reasons:
        priority += 50 + len(missing_fields)
    if "stale_metadata" in reasons:
        priority += 30
    if "tracking_source_url" in reasons:
        priority += 10
    return priority


def _has_tracking_query_param(url: str) -> bool:
    return any(_is_tracking_query_param(key) for key, _value in parse_qsl(urlsplit(url).query))


def _is_tracking_query_param(name: str) -> bool:
    normalized = name.lower()
    return normalized in TRACKING_QUERY_PARAMS or any(
        normalized.startswith(prefix) for prefix in TRACKING_QUERY_PREFIXES
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("db_or_conn must be a sqlite3 connection or Database-like object")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {
        _row_value(row, "name", 0): {
            _row_value(info, "name", 1)
            for info in conn.execute(f"PRAGMA table_info({_row_value(row, 'name', 0)})")
        }
        for row in rows
    }


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    return column if column in columns else default


def _parse_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_datetime(value: Any) -> datetime | None:
    if not _has_text(value):
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _row_value(row: Any, key: str, index: int) -> Any:
    try:
        return row[key]
    except (IndexError, KeyError, TypeError):
        return row[index]


def _has_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _clip(value: Any, width: int) -> str:
    text = str(value)
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."
