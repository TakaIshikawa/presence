"""Report generated content with incomplete or stale platform variants."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Sequence


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 25
DEFAULT_PLATFORMS = ("x", "bluesky", "linkedin")
DEFAULT_STALE_THRESHOLD_DAYS = 0


@dataclass(frozen=True)
class ContentVariantPlatformParityItem:
    """One generated content row with missing or stale platform coverage."""

    content_id: int
    content_type: str | None
    source_created_at: str | None
    source_edit_at: str | None
    existing_platforms: tuple[str, ...]
    missing_platforms: tuple[str, ...]
    stale_variants: tuple[dict[str, Any], ...]
    recommended_generation_targets: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "content_id": self.content_id,
            "content_type": self.content_type,
            "existing_platforms": list(self.existing_platforms),
            "missing_platforms": list(self.missing_platforms),
            "recommended_generation_targets": list(self.recommended_generation_targets),
            "source_created_at": self.source_created_at,
            "source_edit_at": self.source_edit_at,
            "stale_variants": [dict(item) for item in self.stale_variants],
        }


@dataclass(frozen=True)
class ContentVariantPlatformParityReport:
    """Deterministic read-only platform parity report."""

    generated_at: str
    filters: dict[str, Any]
    total_items: int
    summary: dict[str, Any]
    items: tuple[ContentVariantPlatformParityItem, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "content_variant_platform_parity",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "items": [item.to_dict() for item in self.items],
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "summary": self.summary,
            "total_items": self.total_items,
        }


def build_content_variant_platform_parity_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platforms: Sequence[str] | None = None,
    limit: int = DEFAULT_LIMIT,
    stale_threshold_days: int = DEFAULT_STALE_THRESHOLD_DAYS,
    now: datetime | None = None,
) -> ContentVariantPlatformParityReport:
    """Compare generated_content rows with expected content_variants platforms."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if stale_threshold_days < 0:
        raise ValueError("stale_threshold_days must be non-negative")

    expected = _normalize_platforms(platforms or DEFAULT_PLATFORMS)
    if not expected:
        raise ValueError("at least one platform is required")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_aware(now or datetime.now(timezone.utc))
    filters = {
        "days": days,
        "limit": limit,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": (generated_at - timedelta(days=days)).isoformat(),
        "platforms": list(expected),
        "stale_threshold_days": stale_threshold_days,
    }

    required = {
        "generated_content": {"id"},
        "content_variants": {"id", "content_id", "platform", "variant_type"},
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and not columns.issubset(schema[table])
    }
    if missing_tables or missing_columns:
        return _empty_report(
            generated_at,
            filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _content_rows(conn, schema, cutoff=generated_at - timedelta(days=days))
    variants = _variants_by_content(conn, schema, content_ids=[int(row["id"]) for row in rows])
    threshold = timedelta(days=stale_threshold_days)
    items = []
    for row in rows:
        item = _parity_item(row, variants.get(int(row["id"]), ()), expected, threshold)
        if item.missing_platforms or item.stale_variants:
            items.append(item)

    items.sort(key=_item_sort_key)
    limited = tuple(items[:limit])
    return ContentVariantPlatformParityReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        total_items=len(limited),
        summary=_summary(limited, rows_scanned=len(rows), rows_matched=len(items)),
        items=limited,
    )


def format_content_variant_platform_parity_json(
    report: ContentVariantPlatformParityReport,
) -> str:
    """Render deterministic JSON suitable for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_content_variant_platform_parity_text(
    report: ContentVariantPlatformParityReport,
) -> str:
    """Render a compact human-readable parity report."""
    filters = report.filters
    lines = [
        "Content Variant Platform Parity",
        (
            "Filters: "
            f"days={filters.get('days')} limit={filters.get('limit')} "
            f"platforms={','.join(filters.get('platforms', []))} "
            f"stale_threshold_days={filters.get('stale_threshold_days')}"
        ),
        (
            f"Items: {report.total_items} "
            f"missing={report.summary.get('items_with_missing', 0)} "
            f"stale={report.summary.get('items_with_stale', 0)}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        lines.append(
            "Missing columns: "
            + ", ".join(
                f"{table}.{column}"
                for table, columns in sorted(report.missing_columns.items())
                for column in columns
            )
        )
    if not report.items:
        lines.append("No content variant platform parity gaps found.")
        return "\n".join(lines)

    lines.append("Items:")
    for item in report.items:
        lines.append(
            "  - "
            f"content_id={item.content_id} type={item.content_type or 'n/a'} "
            f"existing={_join_or_dash(item.existing_platforms)} "
            f"missing={_join_or_dash(item.missing_platforms)} "
            f"targets={_join_or_dash(item.recommended_generation_targets)}"
        )
        if item.stale_variants:
            stale = ", ".join(
                f"{variant['platform']}/{variant['variant_type']}#{variant['variant_id']}"
                for variant in item.stale_variants
            )
            lines.append(f"    stale_variants={stale}")
    return "\n".join(lines)


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> ContentVariantPlatformParityReport:
    return ContentVariantPlatformParityReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        total_items=0,
        summary=_summary((), rows_scanned=0, rows_matched=0),
        items=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _content_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    source_edit_column = _source_edit_column(columns)
    selected = [
        "id",
        _column_expr(columns, "content_type"),
        _column_expr(columns, "created_at"),
        (
            f"{source_edit_column} AS source_edit_at"
            if source_edit_column in columns
            else "NULL AS source_edit_at"
        ),
    ]
    created_filter = (
        "WHERE datetime(COALESCE(created_at, ?)) >= datetime(?)"
        if "created_at" in columns
        else ""
    )
    params: list[Any] = [cutoff.isoformat(), cutoff.isoformat()] if created_filter else []
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM generated_content
            {created_filter}
            ORDER BY id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _variants_by_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    content_ids: Sequence[int],
) -> dict[int, tuple[dict[str, Any], ...]]:
    if not content_ids:
        return {}
    columns = schema["content_variants"]
    selected = [
        "id AS variant_id",
        "content_id",
        "LOWER(TRIM(platform)) AS platform",
        "variant_type",
        _column_expr(columns, "created_at"),
    ]
    placeholders = ", ".join("?" for _ in content_ids)
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM content_variants
            WHERE content_id IN ({placeholders})
            ORDER BY content_id ASC, LOWER(TRIM(platform)) ASC, variant_type ASC, id ASC""",
        list(content_ids),
    ).fetchall()
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        data = dict(row)
        if data["platform"]:
            grouped.setdefault(int(data["content_id"]), []).append(data)
    return {content_id: tuple(items) for content_id, items in grouped.items()}


def _parity_item(
    row: dict[str, Any],
    variants: Sequence[dict[str, Any]],
    expected: tuple[str, ...],
    threshold: timedelta,
) -> ContentVariantPlatformParityItem:
    source_edit_at = _parse_timestamp(row.get("source_edit_at"))
    existing = tuple(sorted({str(variant["platform"]) for variant in variants if variant.get("platform")}))
    missing = tuple(platform for platform in expected if platform not in existing)
    stale = tuple(
        _stale_variant_payload(variant)
        for variant in variants
        if str(variant.get("platform") or "") in expected
        and _is_stale_variant(variant.get("created_at"), source_edit_at, threshold)
    )
    stale_platforms = tuple(variant["platform"] for variant in stale)
    targets = tuple(dict.fromkeys((*missing, *stale_platforms)))
    return ContentVariantPlatformParityItem(
        content_id=int(row["id"]),
        content_type=row.get("content_type"),
        source_created_at=row.get("created_at"),
        source_edit_at=source_edit_at.isoformat() if source_edit_at else None,
        existing_platforms=existing,
        missing_platforms=missing,
        stale_variants=stale,
        recommended_generation_targets=targets,
    )


def _stale_variant_payload(variant: dict[str, Any]) -> dict[str, Any]:
    return {
        "created_at": variant.get("created_at"),
        "platform": variant.get("platform"),
        "variant_id": int(variant["variant_id"]),
        "variant_type": variant.get("variant_type"),
    }


def _summary(
    items: Sequence[ContentVariantPlatformParityItem],
    *,
    rows_scanned: int,
    rows_matched: int,
) -> dict[str, Any]:
    missing_counter: Counter[str] = Counter()
    stale_counter: Counter[str] = Counter()
    for item in items:
        missing_counter.update(item.missing_platforms)
        stale_counter.update(str(variant["platform"]) for variant in item.stale_variants)
    return {
        "items_with_missing": sum(1 for item in items if item.missing_platforms),
        "items_with_stale": sum(1 for item in items if item.stale_variants),
        "missing_by_platform": dict(sorted(missing_counter.items())),
        "rows_matched": rows_matched,
        "rows_scanned": rows_scanned,
        "stale_by_platform": dict(sorted(stale_counter.items())),
    }


def _item_sort_key(item: ContentVariantPlatformParityItem) -> tuple[Any, ...]:
    newest_stale = max(
        (_parse_timestamp(variant.get("created_at")) for variant in item.stale_variants),
        default=None,
    )
    stale_key = newest_stale.isoformat() if newest_stale else ""
    return (
        -len(item.missing_platforms),
        -len(item.stale_variants),
        stale_key,
        item.content_id,
    )


def _is_stale_variant(
    variant_created_at: Any,
    source_edit_at: datetime | None,
    threshold: timedelta,
) -> bool:
    variant_at = _parse_timestamp(variant_created_at)
    if variant_at is None or source_edit_at is None:
        return False
    return variant_at + threshold < source_edit_at


def _source_edit_column(columns: set[str]) -> str:
    for column in ("updated_at", "edited_at", "modified_at", "last_modified_at", "created_at"):
        if column in columns:
            return column
    return "created_at"


def _column_expr(columns: set[str], column: str) -> str:
    return f"{column} AS {column}" if column in columns else f"NULL AS {column}"


def _normalize_platforms(platforms: Sequence[str]) -> tuple[str, ...]:
    normalized = {
        str(platform).strip().lower()
        for platform in platforms
        if str(platform).strip()
    }
    return tuple(sorted(normalized))


def _join_or_dash(values: Sequence[str]) -> str:
    return ",".join(values) if values else "-"


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _ensure_aware(value)
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
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
