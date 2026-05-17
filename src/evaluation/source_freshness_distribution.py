"""Bucket generated content by cited source age at generation time."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import math
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
BUCKETS = (
    ("0-1d", 0, 1),
    ("2-7d", 2, 7),
    ("8-30d", 8, 30),
    ("31d+", 31, None),
)


def build_source_freshness_distribution_report(
    evidence_rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    channel: str | None = None,
    content_type: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return source-age bucket counts and percentages from content/source rows."""
    if days <= 0:
        raise ValueError("days must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "channel": channel,
        "content_type": content_type,
        "window_start": cutoff.isoformat(),
        "window_end": generated_at.isoformat(),
    }
    skipped = Counter({"missing_content_timestamp": 0, "missing_source_timestamp": 0, "outside_window": 0})
    records: list[dict[str, Any]] = []

    for row in evidence_rows:
        row_channel = _text_or_unknown(row.get("channel") or row.get("platform") or row.get("target_channel"))
        row_content_type = _text_or_unknown(row.get("content_type") or row.get("artifact_type") or row.get("type"))
        if channel is not None and row_channel != channel:
            continue
        if content_type is not None and row_content_type != content_type:
            continue

        content_at = _parse_dt(row.get("content_timestamp") or row.get("generated_at") or row.get("published_at") or row.get("created_at") or row.get("content_created_at"))
        source_at = _parse_dt(row.get("source_timestamp") or row.get("source_published_at") or row.get("source_created_at") or row.get("captured_at") or row.get("collected_at") or row.get("ingested_at"))
        if not content_at:
            skipped["missing_content_timestamp"] += 1
            continue
        if content_at < cutoff or content_at > generated_at:
            skipped["outside_window"] += 1
            continue
        if not source_at:
            skipped["missing_source_timestamp"] += 1
            continue

        source_age_days = max(0, math.floor((content_at - source_at).total_seconds() / 86400))
        records.append(
            {
                "content_id": _text(row.get("content_id") or row.get("generated_content_id") or row.get("post_id") or row.get("id")),
                "content_type": row_content_type,
                "channel": row_channel,
                "source_id": _text(row.get("source_id") or row.get("knowledge_id") or row.get("source_url") or row.get("url")),
                "source_age_days": source_age_days,
                "bucket": _bucket(source_age_days),
                "content_timestamp": content_at.isoformat(),
                "source_timestamp": source_at.isoformat(),
            }
        )

    return {
        "artifact_type": "source_freshness_distribution",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "evidence_count": len(records),
            "content_count": len({item["content_id"] for item in records if item["content_id"]}),
            **dict(skipped),
        },
        "buckets": _bucket_rows(records),
        "groups": {
            "by_channel": _group_rows(records, "channel"),
            "by_content_type": _group_rows(records, "content_type"),
            "by_channel_and_content_type": _group_rows(records, "channel", "content_type"),
        },
        "examples": _examples(records),
        "empty_state": {
            "is_empty": not records,
            "message": "No source evidence rows with usable timestamps found." if not records else None,
        },
    }


def build_source_freshness_distribution_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    report = build_source_freshness_distribution_report(_load_evidence(conn, schema), **kwargs)
    report["missing_tables"] = [] if _has_source_shape(schema) else ["source_evidence"]
    return report


def format_source_freshness_distribution_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_source_freshness_distribution_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Source Freshness Distribution",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} channel={report['filters']['channel'] or 'all'} content_type={report['filters']['content_type'] or 'all'}",
        f"Totals: evidence={totals['evidence_count']} content={totals['content_count']} missing_content_ts={totals['missing_content_timestamp']} missing_source_ts={totals['missing_source_timestamp']}",
        "",
        "Buckets:",
    ]
    for row in report["buckets"]:
        lines.append(f"- {row['bucket']}: count={row['count']} pct={row['percentage']}")
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["buckets"] or not any(row["count"] for row in report["buckets"]):
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    if report["groups"]["by_channel"]:
        lines.extend(["", "By channel:"])
        for row in report["groups"]["by_channel"]:
            lines.append(f"- {row['channel']}: evidence={row['evidence_count']} 31d+={row['buckets']['31d+']['count']}")
    return "\n".join(lines)


format_source_freshness_distribution_table = format_source_freshness_distribution_text


def _bucket_rows(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    total = len(records)
    counts = Counter(item["bucket"] for item in records)
    return [
        {"bucket": label, "count": counts[label], "percentage": round(counts[label] / total, 3) if total else 0.0}
        for label, _start, _end in BUCKETS
    ]


def _group_rows(records: list[dict[str, Any]], *fields: str) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[tuple(record[field] for field in fields)].append(record)
    rows = []
    for key, items in grouped.items():
        row = dict(zip(fields, key, strict=True))
        row["evidence_count"] = len(items)
        row["buckets"] = {item["bucket"]: {"count": item["count"], "percentage": item["percentage"]} for item in _bucket_rows(items)}
        rows.append(row)
    rows.sort(key=lambda item: tuple(item[field] for field in fields))
    return rows


def _examples(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(records, key=lambda item: (-item["source_age_days"], item["content_id"], item["source_id"]))
    return ordered[:10]


def _load_evidence(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    for table in ("source_evidence", "generated_content_sources", "content_sources", "source_artifact_links"):
        columns = schema.get(table)
        if not columns:
            continue
        selected = [
            _select(columns, ("content_id", "generated_content_id", "post_id", "artifact_id"), "content_id"),
            _select(columns, ("content_type", "artifact_type", "type"), "content_type"),
            _select(columns, ("channel", "platform", "target_channel"), "channel"),
            _select(columns, ("source_id", "knowledge_id", "source_url", "url"), "source_id"),
            _select(columns, ("generated_at", "content_timestamp", "published_at", "content_created_at", "created_at"), "content_timestamp"),
            _select(columns, ("source_timestamp", "source_published_at", "source_created_at", "captured_at", "collected_at", "ingested_at"), "source_timestamp"),
        ]
        return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]
    if {"generated_content", "content_knowledge_links", "knowledge"}.issubset(schema):
        return _load_knowledge_links(conn, schema)
    return []


def _load_knowledge_links(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    gc_cols = schema["generated_content"]
    link_cols = schema["content_knowledge_links"]
    k_cols = schema["knowledge"]
    if "id" not in gc_cols or not {"content_id", "knowledge_id"}.issubset(link_cols) or "id" not in k_cols:
        return []
    selected = [
        "gc.id AS content_id",
        _qualified_select(gc_cols, "gc", ("content_type", "type"), "content_type"),
        _qualified_select(gc_cols, "gc", ("channel", "platform", "target_channel"), "channel"),
        "k.id AS source_id",
        _qualified_select(gc_cols, "gc", ("generated_at", "published_at", "created_at"), "content_timestamp"),
        _qualified_select(k_cols, "k", ("published_at", "source_timestamp", "created_at", "ingested_at"), "source_timestamp"),
    ]
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM content_knowledge_links ckl
            JOIN generated_content gc ON gc.id = ckl.content_id
            JOIN knowledge k ON k.id = ckl.knowledge_id"""
    ).fetchall()
    return [dict(row) for row in rows]


def _has_source_shape(schema: dict[str, set[str]]) -> bool:
    return any(table in schema for table in ("source_evidence", "generated_content_sources", "content_sources", "source_artifact_links")) or {"generated_content", "content_knowledge_links", "knowledge"}.issubset(schema)


def _select(columns: set[str], names: tuple[str, ...], alias: str) -> str:
    for name in names:
        if name in columns:
            return f"{name} AS {alias}"
    return f"NULL AS {alias}"


def _qualified_select(columns: set[str], qualifier: str, names: tuple[str, ...], alias: str) -> str:
    for name in names:
        if name in columns:
            return f"{qualifier}.{name} AS {alias}"
    return f"NULL AS {alias}"


def _bucket(age_days: int) -> str:
    for label, start, end in BUCKETS:
        if age_days >= start and (end is None or age_days <= end):
            return label
    return "31d+"


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _text_or_unknown(value: Any) -> str:
    return str(value) if value not in (None, "") else "unknown"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}
