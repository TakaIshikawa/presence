"""Find knowledge sources whose citation frequency has declined."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_WINDOW_DAYS = 30
DEFAULT_MIN_DROP = 1
DEFAULT_MIN_DROP_PERCENT = 0.0
DEFAULT_LIMIT = 25


def build_source_citation_decay_report(
    rows: list[dict[str, Any]],
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    min_drop: int = DEFAULT_MIN_DROP,
    min_drop_percent: float = DEFAULT_MIN_DROP_PERCENT,
    source_type: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] = (),
) -> dict[str, Any]:
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    if min_drop < 0:
        raise ValueError("min_drop must be non-negative")
    if min_drop_percent < 0:
        raise ValueError("min_drop_percent must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    recent_start = generated_at - timedelta(days=window_days)
    baseline_start = recent_start - timedelta(days=window_days)
    source_filter = _norm(source_type, "") or None
    normalized = [_normalize(row) for row in rows]
    if source_filter:
        normalized = [row for row in normalized if row["source_type"] == source_filter]

    buckets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in normalized:
        used_at = row["used_at_dt"]
        if used_at and baseline_start <= used_at <= generated_at:
            buckets[(row["source_type"], row["author"])].append(row)

    decays = []
    for (bucket_source_type, author), bucket_rows in buckets.items():
        recent = [row for row in bucket_rows if row["used_at_dt"] and row["used_at_dt"] >= recent_start]
        baseline = [row for row in bucket_rows if row["used_at_dt"] and baseline_start <= row["used_at_dt"] < recent_start]
        drop = len(baseline) - len(recent)
        drop_percent = round(drop / len(baseline), 4) if baseline else 0.0
        if drop < min_drop or drop_percent < min_drop_percent:
            continue
        decays.append(
            {
                "source_type": bucket_source_type,
                "author": author,
                "baseline_count": len(baseline),
                "recent_count": len(recent),
                "drop_count": drop,
                "drop_percent": drop_percent,
                "source_examples": _source_examples(baseline, recent, limit),
            }
        )
    decays.sort(key=lambda item: (-item["drop_count"], -item["drop_percent"], item["source_type"], item["author"]))
    return {
        "artifact_type": "knowledge_source_citation_decay",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "window_days": window_days,
            "baseline_start": baseline_start.isoformat(),
            "recent_start": recent_start.isoformat(),
            "source_type": source_filter or "all",
            "min_drop": min_drop,
            "min_drop_percent": min_drop_percent,
            "limit": limit,
        },
        "summary": {
            "citation_rows_scanned": len(rows),
            "eligible_citation_count": len(normalized),
            "decay_bucket_count": len(decays),
        },
        "decay_buckets": decays[:limit],
        "missing_tables": list(missing_tables),
    }


def build_source_citation_decay_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing = tuple(table for table in ("knowledge", "content_knowledge_links") if table not in schema)
    rows = [] if missing else _load_rows(conn, schema)
    return build_source_citation_decay_report(rows, missing_tables=missing, **kwargs)


def format_source_citation_decay_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_source_citation_decay_text(report: dict[str, Any]) -> str:
    lines = [
        "Knowledge Source Citation Decay",
        f"Generated: {report['generated_at']}",
        f"Window days: {report['filters']['window_days']}",
        f"Thresholds: min_drop={report['filters']['min_drop']} min_drop_percent={report['filters']['min_drop_percent']}",
        f"Totals: buckets={report['summary']['decay_bucket_count']} citations={report['summary']['eligible_citation_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["decay_buckets"]:
        lines.extend(["", "source_type | author | baseline | recent | drop | drop_percent"])
        for row in report["decay_buckets"]:
            lines.append(f"{row['source_type']} | {row['author']} | {row['baseline_count']} | {row['recent_count']} | {row['drop_count']} | {row['drop_percent']}")
    elif not report["missing_tables"]:
        lines.append("No knowledge source citation decay found.")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    k = schema["knowledge"]
    l = schema["content_knowledge_links"]
    used_expr = _expr(l, "used_at", "used_at", _expr(l, "created_at", "used_at", "NULL").split(" AS ")[0])
    select = [
        "knowledge.id AS knowledge_id",
        _expr(k, "source_type", "source_type", "'unknown'"),
        _expr(k, "author", "author", "'unknown'"),
        _expr(k, "title", "title", "NULL"),
        _expr(k, "url", "url", "NULL"),
        used_expr,
    ]
    knowledge_fk = "knowledge_id" if "knowledge_id" in l else "source_id" if "source_id" in l else "knowledge_id"
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(select)}
                FROM content_knowledge_links
                JOIN knowledge ON knowledge.id = content_knowledge_links.{knowledge_fk}
                ORDER BY used_at, knowledge.id"""
        )
    ]


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    used_at = _parse_dt(row.get("used_at"))
    return {
        "knowledge_id": row.get("knowledge_id"),
        "source_type": _norm(row.get("source_type"), "unknown"),
        "author": _norm(row.get("author"), "unknown"),
        "title": row.get("title"),
        "url": row.get("url"),
        "used_at_dt": used_at,
    }


def _source_examples(baseline: list[dict[str, Any]], recent: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    recent_counts = Counter(row["knowledge_id"] for row in recent)
    baseline_counts = Counter(row["knowledge_id"] for row in baseline)
    by_id = {row["knowledge_id"]: row for row in baseline + recent}
    examples = []
    for knowledge_id, baseline_count in baseline_counts.items():
        recent_count = recent_counts[knowledge_id]
        drop = baseline_count - recent_count
        if drop <= 0:
            continue
        row = by_id[knowledge_id]
        examples.append(
            {
                "knowledge_id": knowledge_id,
                "title": row.get("title"),
                "url": row.get("url"),
                "baseline_count": baseline_count,
                "recent_count": recent_count,
                "drop_count": drop,
            }
        )
    examples.sort(key=lambda item: (-item["drop_count"], str(item["knowledge_id"])))
    return examples[:limit]


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {row[0]: {col[1] for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{column} AS {output}" if column in columns else f"{default} AS {output}"


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _norm(value: Any, default: str) -> str:
    text = "" if value is None else str(value).strip().lower()
    return text or default
