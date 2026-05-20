"""Trend reply_queue quality flags across recent reply drafts."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_WINDOW_HOURS = 168
DEFAULT_MIN_COUNT = 2
DEFAULT_MIN_RATE = 0.25
DEFAULT_LIMIT = 25


def build_reply_quality_flag_trends_report(
    rows: list[dict[str, Any]],
    *,
    window_hours: int = DEFAULT_WINDOW_HOURS,
    min_count: int = DEFAULT_MIN_COUNT,
    min_rate: float = DEFAULT_MIN_RATE,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] = (),
) -> dict[str, Any]:
    if window_hours <= 0:
        raise ValueError("window_hours must be positive")
    if min_count <= 0:
        raise ValueError("min_count must be positive")
    if not 0 <= min_rate <= 1:
        raise ValueError("min_rate must be between 0 and 1")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    window_start = generated_at - timedelta(hours=window_hours)
    parsed_rows = [_parse_row(row) for row in rows]
    malformed_rows = [row["malformed_quality_flags"] for row in parsed_rows if row["malformed_quality_flags"]]
    flag_buckets = _flag_buckets(parsed_rows, min_count=min_count, min_rate=min_rate, limit=limit)

    return {
        "artifact_type": "reply_quality_flag_trends",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "window_hours": window_hours,
            "window_start": window_start.isoformat(),
            "window_end": generated_at.isoformat(),
            "min_count": min_count,
            "min_rate": min_rate,
            "limit": limit,
        },
        "missing_tables": list(missing_tables),
        "summary": {
            "rows_scanned": len(rows),
            "rows_with_flags": sum(1 for row in parsed_rows if row["quality_flags"]),
            "malformed_quality_flag_rows": len(malformed_rows),
            "flag_bucket_count": len(flag_buckets),
            "total_flag_instances": sum(len(row["quality_flags"]) for row in parsed_rows),
        },
        "counts": {
            "by_platform": _counts(parsed_rows, "platform"),
            "by_intent": _counts(parsed_rows, "intent"),
            "by_status": _counts(parsed_rows, "status"),
            "by_flag": dict(sorted(Counter(flag for row in parsed_rows for flag in row["quality_flags"]).items())),
        },
        "flag_buckets": flag_buckets,
        "malformed_flag_rows": malformed_rows[:limit],
    }


def build_reply_quality_flag_trends_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = tuple(table for table in ("reply_queue",) if table not in schema)
    if missing_tables:
        return build_reply_quality_flag_trends_report([], missing_tables=missing_tables, **kwargs)

    window_hours = kwargs.get("window_hours", DEFAULT_WINDOW_HOURS)
    now = _utc(kwargs.get("now") or datetime.now(timezone.utc))
    cutoff = now - timedelta(hours=window_hours)
    rows = _load_rows(conn, schema["reply_queue"], cutoff=cutoff, window_end=now)
    return build_reply_quality_flag_trends_report(
        rows,
        missing_tables=missing_tables,
        **{**kwargs, "now": now},
    )


def format_reply_quality_flag_trends_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_quality_flag_trends_text(report: dict[str, Any]) -> str:
    thresholds = report["thresholds"]
    summary = report["summary"]
    lines = [
        "Reply Quality Flag Trends",
        f"Generated: {report['generated_at']}",
        (
            f"Window: hours={thresholds['window_hours']} start={thresholds['window_start']} "
            f"end={thresholds['window_end']}"
        ),
        (
            f"Thresholds: min_count={thresholds['min_count']} "
            f"min_rate={thresholds['min_rate']:.2f} limit={thresholds['limit']}"
        ),
        (
            f"Totals: rows={summary['rows_scanned']} rows_with_flags={summary['rows_with_flags']} "
            f"flag_instances={summary['total_flag_instances']} malformed={summary['malformed_quality_flag_rows']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["flag_buckets"]:
        lines.extend(["", "Flag buckets:"])
        for bucket in report["flag_buckets"]:
            lines.append(
                f"- flag={bucket['flag']} count={bucket['count']} rate={bucket['rate']:.2f} "
                f"platform={bucket['platform']} intent={bucket['intent']} status={bucket['status']}"
            )
    if report["malformed_flag_rows"]:
        lines.extend(["", "Malformed quality_flags:"])
        for item in report["malformed_flag_rows"]:
            lines.append(
                f"- reply_queue:{item['reply_queue_id']} platform={item['platform']} "
                f"intent={item['intent']} status={item['status']} classification={item['classification']}"
            )
    if not report["flag_buckets"] and not report["malformed_flag_rows"] and not report["missing_tables"]:
        lines.append("No quality flag trend buckets exceeded thresholds.")
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
    window_end: datetime,
) -> list[dict[str, Any]]:
    selected = [
        _column_expr(columns, "id", default="rowid"),
        _column_expr(columns, "platform", default="'unknown'"),
        _column_expr(columns, "intent", default="'unknown'"),
        _column_expr(columns, "status", default="'unknown'"),
        _column_expr(columns, "quality_flags", default="NULL"),
        _column_expr(columns, "detected_at", default="NULL"),
    ]
    filters = []
    params: list[Any] = []
    if "detected_at" in columns:
        filters.append("datetime(detected_at) >= datetime(?) AND datetime(detected_at) <= datetime(?)")
        params.extend([_sqlite_ts(cutoff), _sqlite_ts(window_end)])
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM reply_queue {where} ORDER BY id", params)]


def _parse_row(row: dict[str, Any]) -> dict[str, Any]:
    flags, malformed = _parse_flags(row.get("quality_flags"))
    parsed = {
        "reply_queue_id": _int(row.get("id")),
        "platform": _norm(row.get("platform")),
        "intent": _norm(row.get("intent")),
        "status": _norm(row.get("status")),
        "quality_flags": flags,
        "detected_at": row.get("detected_at"),
        "malformed_quality_flags": None,
    }
    if malformed:
        parsed["malformed_quality_flags"] = {
            "reply_queue_id": parsed["reply_queue_id"],
            "platform": parsed["platform"],
            "intent": parsed["intent"],
            "status": parsed["status"],
            "detected_at": parsed["detected_at"],
            "classification": malformed,
            "raw_quality_flags": row.get("quality_flags"),
        }
    return parsed


def _flag_buckets(
    rows: list[dict[str, Any]], *, min_count: int, min_rate: float, limit: int
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], int] = defaultdict(int)
    group_sizes: Counter[tuple[str, str, str]] = Counter()
    for row in rows:
        group_key = (row["platform"], row["intent"], row["status"])
        group_sizes[group_key] += 1
        for flag in row["quality_flags"]:
            grouped[(flag, *group_key)] += 1
    buckets = []
    for (flag, platform, intent, status), count in grouped.items():
        total = group_sizes[(platform, intent, status)]
        rate = count / total if total else 0.0
        if count >= min_count or rate >= min_rate:
            buckets.append(
                {
                    "flag": flag,
                    "platform": platform,
                    "intent": intent,
                    "status": status,
                    "count": count,
                    "group_total": total,
                    "rate": rate,
                    "exceeded_thresholds": {
                        "min_count": count >= min_count,
                        "min_rate": rate >= min_rate,
                    },
                }
            )
    buckets.sort(key=lambda item: (-item["count"], -item["rate"], item["flag"], item["platform"], item["intent"], item["status"]))
    return buckets[:limit]


def _parse_flags(raw: Any) -> tuple[list[str], str | None]:
    if raw in (None, ""):
        return [], None
    if isinstance(raw, list):
        decoded = raw
    else:
        try:
            decoded = json.loads(str(raw))
        except json.JSONDecodeError:
            return [], "invalid_json"
    if not isinstance(decoded, list):
        return [], "not_json_array"
    flags = sorted({_norm(item) for item in decoded if _norm(item) != "unknown"})
    return flags, None


def _counts(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    return dict(sorted(Counter(row[key] for row in rows).items()))


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], name: str, *, default: str) -> str:
    return name if name in columns else f"{default} AS {name}"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _sqlite_ts(value: datetime) -> str:
    return _utc(value).strftime("%Y-%m-%d %H:%M:%S")


def _norm(value: Any) -> str:
    text = "" if value is None else str(value).strip().lower()
    return text or "unknown"


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
