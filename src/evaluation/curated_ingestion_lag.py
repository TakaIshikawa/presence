"""Measure lag from curated publication through ingestion, embedding, and first use."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import math
import statistics
from typing import Any


DEFAULT_LAG_THRESHOLD_HOURS = 24.0
DEFAULT_LIMIT = 50


def build_curated_ingestion_lag_report(
    rows: list[dict[str, Any]],
    *,
    lag_threshold_hours: float = DEFAULT_LAG_THRESHOLD_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if lag_threshold_hours < 0:
        raise ValueError("lag_threshold_hours must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    items = [_item(row) for row in rows]
    per_source = _aggregate_by_source(items)
    late_sources = [
        source
        for source in per_source
        if _is_late_source(source, lag_threshold_hours=lag_threshold_hours)
    ]
    late_sources.sort(
        key=lambda source: (
            -max(
                source["publish_to_ingest_p95_hours"] or 0,
                source["ingest_to_embedding_p95_hours"] or 0,
                source["ingest_to_first_use_p95_hours"] or 0,
            ),
            source["source"],
        )
    )
    return {
        "artifact_type": "curated_ingestion_lag",
        "generated_at": generated_at.isoformat(),
        "filters": {"lag_threshold_hours": lag_threshold_hours, "limit": limit},
        "summary": {
            "rows_scanned": len(items),
            "missing_embedding_count": sum(1 for item in items if item["embedding_at"] is None),
            "never_used_count": sum(1 for item in items if item["first_used_at"] is None),
            "sources": len(per_source),
        },
        "items": items[:limit],
        "per_source": per_source,
        "late_sources": late_sources[:limit],
    }


def format_curated_ingestion_lag_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_curated_ingestion_lag_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Curated Ingestion Lag",
        f"Generated: {report['generated_at']}",
        f"Totals: rows={summary['rows_scanned']} sources={summary['sources']} "
        f"missing_embedding={summary['missing_embedding_count']} never_used={summary['never_used_count']}",
    ]
    if report["per_source"]:
        lines.extend(["", "Per-source lag:"])
        for source in report["per_source"]:
            lines.append(
                f"  - source={source['source']} count={source['item_count']} "
                f"publish_ingest_avg={source['publish_to_ingest_average_hours']} "
                f"ingest_embedding_avg={source['ingest_to_embedding_average_hours']} "
                f"ingest_first_use_avg={source['ingest_to_first_use_average_hours']} "
                f"missing_embedding={source['missing_embedding_count']} never_used={source['never_used_count']}"
            )
    if report["late_sources"]:
        lines.extend(["", "Late sources:"])
        for source in report["late_sources"]:
            lines.append(f"  - source={source['source']} late_reasons={','.join(source['late_reasons'])}")
    return "\n".join(lines)


def _item(row: dict[str, Any]) -> dict[str, Any]:
    published_at = _parse_dt(_first(row, "published_at", "source_published_at", "publication_time", "pub_date"))
    ingested_at = _parse_dt(_first(row, "ingested_at", "ingest_at", "curated_at", "created_at"))
    embedding_at = _parse_dt(_first(row, "embedding_at", "embedded_at", "indexed_at"))
    first_used_at = _parse_dt(_first(row, "first_used_at", "first_use_at", "used_at", "generated_at"))
    return {
        "item_id": _text(_first(row, "item_id", "knowledge_id", "source_id", "id")) or "unknown",
        "source": _text(_first(row, "source", "source_name", "domain", "source_domain")) or "unknown",
        "published_at": _iso(published_at),
        "ingested_at": _iso(ingested_at),
        "embedding_at": _iso(embedding_at),
        "first_used_at": _iso(first_used_at),
        "publish_to_ingest_hours": _hours_between(published_at, ingested_at),
        "ingest_to_embedding_hours": _hours_between(ingested_at, embedding_at),
        "ingest_to_first_use_hours": _hours_between(ingested_at, first_used_at),
    }


def _aggregate_by_source(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[item["source"]].append(item)
    sources = []
    for source, source_items in grouped.items():
        publish_to_ingest = _values(source_items, "publish_to_ingest_hours")
        ingest_to_embedding = _values(source_items, "ingest_to_embedding_hours")
        ingest_to_first_use = _values(source_items, "ingest_to_first_use_hours")
        sources.append(
            {
                "source": source,
                "item_count": len(source_items),
                "publish_to_ingest_average_hours": _avg(publish_to_ingest),
                "publish_to_ingest_p95_hours": _p95(publish_to_ingest),
                "ingest_to_embedding_average_hours": _avg(ingest_to_embedding),
                "ingest_to_embedding_p95_hours": _p95(ingest_to_embedding),
                "ingest_to_first_use_average_hours": _avg(ingest_to_first_use),
                "ingest_to_first_use_p95_hours": _p95(ingest_to_first_use),
                "missing_embedding_count": sum(1 for item in source_items if item["embedding_at"] is None),
                "never_used_count": sum(1 for item in source_items if item["first_used_at"] is None),
            }
        )
    sources.sort(key=lambda source: (-source["item_count"], source["source"]))
    return sources


def _is_late_source(source: dict[str, Any], *, lag_threshold_hours: float) -> bool:
    late_reasons = []
    for key in (
        "publish_to_ingest_p95_hours",
        "ingest_to_embedding_p95_hours",
        "ingest_to_first_use_p95_hours",
    ):
        value = source[key]
        if value is not None and value > lag_threshold_hours:
            late_reasons.append(key.removesuffix("_hours"))
    if source["missing_embedding_count"] == source["item_count"] and source["item_count"]:
        late_reasons.append("all_missing_embedding")
    if source["never_used_count"] == source["item_count"] and source["item_count"]:
        late_reasons.append("all_never_used")
    source["late_reasons"] = late_reasons
    return bool(late_reasons)


def _values(items: list[dict[str, Any]], key: str) -> list[float]:
    return [item[key] for item in items if item[key] is not None]


def _avg(values: list[float]) -> float | None:
    return round(statistics.fmean(values), 4) if values else None


def _p95(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * 0.95) - 1)
    return round(ordered[index], 4)


def _hours_between(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return round((end - start).total_seconds() / 3600, 4)


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""
