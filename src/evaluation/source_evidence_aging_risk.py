"""Report generated content backed by old source evidence."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import math
import sqlite3
from typing import Any


DEFAULT_STALE_DAYS = 30
DEFAULT_EXPIRED_DAYS = 90
DEFAULT_LIMIT = 100


def build_source_evidence_aging_risk_report(
    evidence_rows: list[dict[str, Any]],
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    expired_days: int = DEFAULT_EXPIRED_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return source-evidence age records from in-memory content/source rows."""
    if not (0 <= stale_days <= expired_days):
        raise ValueError("thresholds must satisfy 0 <= stale_days <= expired_days")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    records = []
    skipped = {"missing_content_timestamp": 0, "missing_source_timestamp": 0}
    for row in evidence_rows:
        content_at = _parse_dt(
            row.get("content_timestamp")
            or row.get("generated_at")
            or row.get("published_at")
            or row.get("created_at")
        )
        source_at = _parse_dt(
            row.get("source_timestamp")
            or row.get("source_artifact_timestamp")
            or row.get("source_created_at")
            or row.get("captured_at")
            or row.get("collected_at")
            or row.get("source_published_at")
        )
        if not content_at:
            skipped["missing_content_timestamp"] += 1
            continue
        if not source_at:
            skipped["missing_source_timestamp"] += 1
            continue

        age_days = max(0, math.floor((content_at - source_at).total_seconds() / 86400))
        bucket = _risk_bucket(age_days, stale_days, expired_days)
        records.append(
            {
                "content_id": _text(row.get("content_id") or row.get("post_id") or row.get("id")),
                "content_type": _text(row.get("content_type") or row.get("artifact_type") or "unknown"),
                "source_id": _text(row.get("source_id") or row.get("source_artifact_id") or row.get("source_url")),
                "source_type": _text(row.get("source_type") or row.get("artifact_source_type") or "unknown"),
                "content_timestamp": content_at.isoformat(),
                "source_timestamp": source_at.isoformat(),
                "source_age_days": age_days,
                "risk_bucket": bucket,
                "recommended_action": _recommendation(bucket),
            }
        )

    records.sort(key=_sort_key)
    ranked = records[:limit]
    return {
        "artifact_type": "source_evidence_aging_risk",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "stale_days": stale_days,
            "expired_days": expired_days,
            "limit": limit,
        },
        "totals": {
            "evidence_count": len(records),
            "record_count": len(ranked),
            "fresh": sum(1 for item in records if item["risk_bucket"] == "fresh"),
            "stale": sum(1 for item in records if item["risk_bucket"] == "stale"),
            "expired": sum(1 for item in records if item["risk_bucket"] == "expired"),
            **skipped,
        },
        "aggregates": {
            "by_risk_bucket_and_source_type": _aggregate(records, ("risk_bucket", "source_type")),
            "by_content_type_and_source_type": _aggregate(records, ("content_type", "source_type")),
        },
        "evidence": ranked,
        "stale_evidence": [item for item in ranked if item["risk_bucket"] in {"stale", "expired"}],
        "empty_state": {
            "is_empty": not records,
            "message": "No source evidence rows with usable timestamps found." if not records else None,
        },
    }


def build_source_evidence_aging_risk_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_source_evidence_aging_risk_report(_load_evidence(conn, schema), **kwargs)


def format_source_evidence_aging_risk_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_source_evidence_aging_risk_text(report: dict[str, Any]) -> str:
    lines = [
        "Source Evidence Aging Risk",
        f"Generated: {report['generated_at']}",
        (
            f"Thresholds: stale>={report['filters']['stale_days']}d "
            f"expired>={report['filters']['expired_days']}d"
        ),
        (
            "Totals: "
            f"evidence={report['totals']['evidence_count']} expired={report['totals']['expired']} "
            f"stale={report['totals']['stale']} fresh={report['totals']['fresh']}"
        ),
    ]
    if not report["evidence"]:
        lines.extend(["", report["empty_state"]["message"]])
        return "\n".join(lines)
    lines.extend(["", "Evidence:", "bucket   age   content              source               source_type"])
    for item in report["evidence"]:
        lines.append(
            f"{item['risk_bucket']:<8} {item['source_age_days']:<5} "
            f"{item['content_id'][:20]:<20} {item['source_id'][:20]:<20} "
            f"{item['source_type'][:20]}"
        )
    return "\n".join(lines)


def _load_evidence(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    for table in ("source_evidence", "generated_content_sources", "content_sources", "source_artifact_links"):
        columns = schema.get(table)
        if not columns:
            continue
        selected = [
            _select(columns, ("content_id", "post_id", "artifact_id", "generated_content_id"), "content_id"),
            _select(columns, ("content_type", "artifact_type"), "content_type"),
            _select(columns, ("source_id", "source_artifact_id", "source_url", "url"), "source_id"),
            _select(columns, ("source_type", "artifact_source_type", "source_kind"), "source_type"),
            _select(columns, ("generated_at", "published_at", "content_created_at", "created_at"), "content_timestamp"),
            _select(
                columns,
                ("source_timestamp", "source_artifact_timestamp", "source_created_at", "captured_at", "collected_at", "source_published_at"),
                "source_timestamp",
            ),
        ]
        return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]
    return []


def _select(columns: set[str], names: tuple[str, ...], alias: str) -> str:
    for name in names:
        if name in columns:
            return f"{name} AS {alias}"
    return f"NULL AS {alias}"


def _risk_bucket(age_days: int, stale_days: int, expired_days: int) -> str:
    if age_days >= expired_days:
        return "expired"
    if age_days >= stale_days:
        return "stale"
    return "fresh"


def _recommendation(bucket: str) -> str:
    return {
        "fresh": "keep source",
        "stale": "refresh source or add newer corroborating evidence",
        "expired": "replace source before publishing or revalidate the claim",
    }[bucket]


def _aggregate(records: list[dict[str, Any]], fields: tuple[str, ...]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, ...], int] = defaultdict(int)
    for record in records:
        groups[tuple(record[field] for field in fields)] += 1
    rows = [{**dict(zip(fields, key, strict=True)), "count": count} for key, count in groups.items()]
    rows.sort(key=lambda item: (-item["count"], tuple(item[field] for field in fields)))
    return rows


def _sort_key(item: dict[str, Any]) -> tuple[int, int, str, str]:
    rank = {"expired": 2, "stale": 1, "fresh": 0}
    return (-rank[item["risk_bucket"]], -item["source_age_days"], item["content_id"], item["source_id"])


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return _utc(datetime.fromisoformat(text))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}
