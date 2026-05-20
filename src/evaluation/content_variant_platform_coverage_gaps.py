"""Report generated content missing expected platform variant rows."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Mapping, Sequence


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_LIMIT = 25
DEFAULT_EXPECTED_MATRIX = (("blog", "post"), ("newsletter", "summary"), ("x", "post"))


def build_content_variant_platform_coverage_gaps_report(
    content_rows: list[dict[str, Any]],
    *,
    expected_matrix: Sequence[tuple[str, str]] | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    content_type: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Build a deterministic coverage report from generated content rows."""
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    raw_matrix = DEFAULT_EXPECTED_MATRIX if expected_matrix is None else expected_matrix
    normalized_matrix = _normalize_expected_matrix(raw_matrix)
    if not normalized_matrix:
        raise ValueError("at least one expected platform:variant_type pair is required")
    normalized_content_type = _normalize_optional(content_type)

    generated_at = _utc(now or datetime.now(timezone.utc))
    lookback_start = generated_at - timedelta(days=lookback_days)
    missing_items = []
    for row in content_rows:
        existing = _existing_pairs(row.get("variants", ()))
        missing = tuple(pair for pair in normalized_matrix if pair not in existing)
        if not missing:
            continue
        missing_items.append(
            {
                "content_id": _int(row.get("content_id") if "content_id" in row else row.get("id")),
                "content_type": _text_or_none(row.get("content_type")),
                "created_at": row.get("created_at"),
                "existing_variants": [_pair_payload(pair) for pair in existing],
                "missing_variants": [_pair_payload(pair) for pair in missing],
                "missing_variant_count": len(missing),
            }
        )

    missing_items.sort(key=_item_sort_key)
    shown = missing_items[:limit]
    return {
        "artifact_type": "content_variant_platform_coverage_gaps",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "content_type": normalized_content_type,
            "limit": limit,
            "lookback_days": lookback_days,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": lookback_start.isoformat(),
        },
        "expected_matrix": [_pair_payload(pair) for pair in normalized_matrix],
        "missing_tables": list(missing_tables),
        "total_missing_items": len(missing_items),
        "missing_variant_items": shown,
        "grouped_summaries": _grouped_summaries(missing_items),
        "summary": {
            "content_rows_scanned": len(content_rows),
            "expected_pair_count": len(normalized_matrix),
            "rows_with_missing_variants": len(missing_items),
            "shown_missing_items": len(shown),
            "total_missing_variant_pairs": sum(item["missing_variant_count"] for item in missing_items),
        },
    }


def build_content_variant_platform_coverage_gaps_report_from_db(
    db_or_conn: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = ("generated_content", "content_variants")
    missing_tables = tuple(table for table in required if table not in schema)
    if missing_tables:
        return build_content_variant_platform_coverage_gaps_report(
            [],
            missing_tables=missing_tables,
            **kwargs,
        )

    lookback_days = kwargs.get("lookback_days", DEFAULT_LOOKBACK_DAYS)
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    now = _utc(kwargs.get("now") or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=lookback_days)
    rows = _load_content_rows(
        conn,
        schema,
        cutoff=cutoff,
        window_end=now,
        content_type=kwargs.get("content_type"),
    )
    return build_content_variant_platform_coverage_gaps_report(
        rows,
        missing_tables=missing_tables,
        **{**kwargs, "now": now},
    )


def format_content_variant_platform_coverage_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_variant_platform_coverage_gaps_text(report: dict[str, Any]) -> str:
    thresholds = report["thresholds"]
    summary = report["summary"]
    expected = ", ".join(
        f"{item['platform']}:{item['variant_type']}" for item in report["expected_matrix"]
    )
    lines = [
        "Content Variant Platform Coverage Gaps",
        f"Generated: {report['generated_at']}",
        (
            f"Window: days={thresholds['lookback_days']} start={thresholds['lookback_start']} "
            f"end={thresholds['lookback_end']}"
        ),
        f"Filters: content_type={thresholds['content_type'] or '-'} limit={thresholds['limit']}",
        f"Expected: {expected}",
        (
            f"Totals: rows_scanned={summary['content_rows_scanned']} "
            f"items={report['total_missing_items']} "
            f"missing_pairs={summary['total_missing_variant_pairs']} "
            f"shown={summary['shown_missing_items']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["missing_variant_items"]:
        lines.append("No content variant platform coverage gaps found.")
        return "\n".join(lines)

    lines.extend(["", "Missing items:"])
    for item in report["missing_variant_items"]:
        missing = ", ".join(
            f"{pair['platform']}:{pair['variant_type']}" for pair in item["missing_variants"]
        )
        existing = ", ".join(
            f"{pair['platform']}:{pair['variant_type']}" for pair in item["existing_variants"]
        ) or "-"
        lines.append(
            f"- content_id={item['content_id']} type={item['content_type'] or '-'} "
            f"missing={missing} existing={existing}"
        )
    return "\n".join(lines)


def parse_expected_pair(value: str) -> tuple[str, str]:
    platform, sep, variant_type = value.partition(":")
    platform = platform.strip().lower()
    variant_type = variant_type.strip().lower()
    if not sep or not platform or not variant_type:
        raise ValueError("expected values must use platform:variant_type")
    return platform, variant_type


def _load_content_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    window_end: datetime,
    content_type: str | None,
) -> list[dict[str, Any]]:
    gc = schema["generated_content"]
    selected = [
        _column_expr(gc, "id", "gc", "content_id", default="gc.rowid"),
        _column_expr(gc, "content_type", "gc", "content_type", default="NULL"),
        _column_expr(gc, "created_at", "gc", "created_at", default="NULL"),
        _column_expr(gc, "status", "gc", "status", default="NULL"),
        _column_expr(gc, "abandoned", "gc", "abandoned", default="0"),
    ]
    filters = []
    params: list[Any] = []
    if "created_at" in gc:
        filters.append("datetime(gc.created_at) >= datetime(?) AND datetime(gc.created_at) <= datetime(?)")
        params.extend([_sqlite_ts(cutoff), _sqlite_ts(window_end)])
    normalized_content_type = _normalize_optional(content_type)
    if normalized_content_type and "content_type" in gc:
        filters.append("LOWER(TRIM(gc.content_type)) = ?")
        params.append(normalized_content_type)
    if "status" in gc:
        filters.append("LOWER(COALESCE(gc.status, '')) != 'abandoned'")
    if "abandoned" in gc:
        filters.append("COALESCE(gc.abandoned, 0) = 0")
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM generated_content gc
            {where}
            ORDER BY {_content_order_expr(gc)}""",
        params,
    ).fetchall()
    content_rows = [dict(row) for row in rows]
    variants = _variants_by_content(
        conn,
        schema,
        content_ids=[_int(row["content_id"]) for row in content_rows],
    )
    for row in content_rows:
        row["variants"] = variants.get(_int(row["content_id"]), ())
    return content_rows


def _variants_by_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    content_ids: Sequence[int],
) -> dict[int, tuple[dict[str, Any], ...]]:
    if not content_ids:
        return {}
    cv = schema["content_variants"]
    selected = [
        _column_expr(cv, "id", "cv", "variant_id", default="cv.rowid"),
        _column_expr(cv, "content_id", "cv", "content_id", default="NULL"),
        _column_expr(cv, "platform", "cv", "platform", default="NULL"),
        _column_expr(cv, "variant_type", "cv", "variant_type", default="NULL"),
    ]
    placeholders = ", ".join("?" for _ in content_ids)
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM content_variants cv
            WHERE cv.content_id IN ({placeholders})
            ORDER BY cv.content_id ASC, LOWER(TRIM(cv.platform)) ASC, LOWER(TRIM(cv.variant_type)) ASC, cv.rowid ASC""",
        list(content_ids),
    ).fetchall()
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        data = dict(row)
        content_id = _int(data.get("content_id"))
        if content_id:
            grouped.setdefault(content_id, []).append(data)
    return {content_id: tuple(items) for content_id, items in grouped.items()}


def _grouped_summaries(items: Sequence[dict[str, Any]]) -> dict[str, Any]:
    platform_counts: Counter[str] = Counter()
    variant_type_counts: Counter[str] = Counter()
    content_type_counts: Counter[str] = Counter()
    pair_counts: Counter[str] = Counter()
    for item in items:
        content_type_counts.update([item["content_type"] or "unknown"])
        for pair in item["missing_variants"]:
            platform_counts.update([pair["platform"]])
            variant_type_counts.update([pair["variant_type"]])
            pair_counts.update([f"{pair['platform']}:{pair['variant_type']}"])
    return {
        "missing_by_content_type": dict(sorted(content_type_counts.items())),
        "missing_by_expected_pair": dict(sorted(pair_counts.items())),
        "missing_by_platform": dict(sorted(platform_counts.items())),
        "missing_by_variant_type": dict(sorted(variant_type_counts.items())),
    }


def _normalize_expected_matrix(values: Sequence[tuple[str, str]]) -> tuple[tuple[str, str], ...]:
    normalized = {
        (str(platform).strip().lower(), str(variant_type).strip().lower())
        for platform, variant_type in values
        if str(platform).strip() and str(variant_type).strip()
    }
    return tuple(sorted(normalized))


def _existing_pairs(variants: Any) -> tuple[tuple[str, str], ...]:
    pairs = set()
    for variant in variants or ():
        if not isinstance(variant, Mapping):
            continue
        platform = str(variant.get("platform") or "").strip().lower()
        variant_type = str(variant.get("variant_type") or "").strip().lower()
        if platform and variant_type:
            pairs.add((platform, variant_type))
    return tuple(sorted(pairs))


def _pair_payload(pair: tuple[str, str]) -> dict[str, str]:
    return {"platform": pair[0], "variant_type": pair[1]}


def _item_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (-int(item["missing_variant_count"]), item["content_type"] or "", int(item["content_id"]), item["created_at"] or "")


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], column: str, alias: str, output: str, *, default: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"{default} AS {output}"


def _content_order_expr(columns: set[str]) -> str:
    parts = []
    if "created_at" in columns:
        parts.append("datetime(gc.created_at) DESC")
    if "id" in columns:
        parts.append("gc.id ASC")
    return ", ".join(parts) or "gc.rowid ASC"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _sqlite_ts(value: datetime) -> str:
    return _utc(value).strftime("%Y-%m-%d %H:%M:%S")


def _normalize_optional(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def _text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
