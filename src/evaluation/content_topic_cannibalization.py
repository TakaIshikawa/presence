"""Detect recent content items competing for the same topic."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
DEFAULT_MIN_OVERLAP_SCORE = 0.5
_TOKEN_RE = re.compile(r"[a-z0-9]{3,}")
_STOP = {"the", "and", "for", "with", "from", "that", "this", "into", "your", "you", "are", "content"}


def build_content_topic_cannibalization_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    min_overlap_score: float = DEFAULT_MIN_OVERLAP_SCORE,
    content_type: str | None = None,
    status: str | None = None,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if not 0 <= min_overlap_score <= 1:
        raise ValueError("min_overlap_score must be between 0 and 1")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    items = []
    for row in rows:
        created = _parse_dt(row.get("created_at") or row.get("published_at"))
        if created and created < cutoff:
            continue
        ctype = _text(row.get("content_type"))
        row_status = _text(row.get("status"))
        if content_type and ctype != content_type:
            continue
        if status and row_status != status:
            continue
        tokens = _topic_tokens(row)
        if tokens:
            items.append({"row": row, "tokens": tokens, "created_at": created})
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        for token in sorted(item["tokens"]):
            buckets[token].append(item)
    findings = []
    seen_groups = set()
    for token, bucket in buckets.items():
        if len(bucket) < 2:
            continue
        ids = tuple(sorted(_text(item["row"].get("id")) for item in bucket))
        if (token, ids) in seen_groups:
            continue
        seen_groups.add((token, ids))
        shared = set.intersection(*(item["tokens"] for item in bucket))
        union = set.union(*(item["tokens"] for item in bucket))
        score = round(len(shared) / len(union), 4) if union else 0.0
        if score < min_overlap_score and len(shared) < 2:
            continue
        dates = [item["created_at"] for item in bucket if item["created_at"]]
        findings.append(
            {
                "canonical_topic": " ".join(sorted(shared)[:4]) or token,
                "content_ids": ids,
                "content_types": sorted({_text(item["row"].get("content_type")) for item in bucket}),
                "statuses": sorted({_text(item["row"].get("status")) for item in bucket}),
                "overlap_score": score,
                "date_span": {
                    "start": min(dates).isoformat() if dates else None,
                    "end": max(dates).isoformat() if dates else None,
                },
                "reason_code": "same_channel_topic_overlap"
                if len({_text(item["row"].get("content_type")) for item in bucket}) == 1
                else "topic_overlap",
            }
        )
    findings.sort(key=lambda item: (-item["overlap_score"], item["canonical_topic"], item["content_ids"]))
    return {
        "artifact_type": "content_topic_cannibalization",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "limit": limit,
            "min_overlap_score": min_overlap_score,
            "content_type": content_type,
            "status": status,
            "lookback_start": cutoff.isoformat(),
        },
        "summary": {"content_scanned": len(items), "finding_count": len(findings)},
        "findings": findings[:limit],
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
    }


def build_content_topic_cannibalization_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    rows = _load_rows(conn, schema) if not gaps["missing_tables"] else []
    return build_content_topic_cannibalization_report(rows, schema_gaps=gaps, **kwargs)


def format_content_topic_cannibalization_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_topic_cannibalization_text(report: dict[str, Any]) -> str:
    lines = [
        "Content Topic Cannibalization",
        f"Generated: {report['generated_at']}",
        f"Totals: scanned={report['summary']['content_scanned']} findings={report['summary']['finding_count']}",
    ]
    if not report["findings"]:
        lines.extend(["", "No content topic cannibalization findings found."])
        return "\n".join(lines)
    lines.extend(["", "Findings:"])
    for item in report["findings"]:
        lines.append(
            f"  - topic={item['canonical_topic']} ids={', '.join(item['content_ids'])} score={item['overlap_score']} reason={item['reason_code']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    select = [
        _select(columns, ("id",), "id"),
        _select(columns, ("content_type", "type"), "content_type"),
        _select(columns, ("status", "publication_status"), "status"),
        _select(columns, ("title",), "title"),
        _select(columns, ("content", "body"), "body"),
        _select(columns, ("metadata",), "metadata"),
        _select(columns, ("created_at", "published_at"), "created_at"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM generated_content").fetchall()]


def _topic_tokens(row: dict[str, Any]) -> set[str]:
    metadata = _json_obj(row.get("metadata"))
    parts = [_text(row.get("title")), _text(row.get("body"))[:500]]
    for key in ("topic", "topics", "tags"):
        value = metadata.get(key)
        if isinstance(value, list):
            parts.extend(_text(item) for item in value)
        else:
            parts.append(_text(value))
    return {token for token in _TOKEN_RE.findall(" ".join(parts).lower()) if token not in _STOP}


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "generated_content" not in schema:
        return {"missing_tables": ["generated_content"], "missing_columns": {}}
    return {"missing_tables": [], "missing_columns": {}}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _select(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate if candidate == alias else f"{candidate} AS {alias}"
    return f"NULL AS {alias}"


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
