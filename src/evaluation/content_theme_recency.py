"""Report content themes or angles reused too recently."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 60
DEFAULT_COOLDOWN_DAYS = 14
DEFAULT_LIMIT = 100


def build_content_theme_recency_report(
    theme_rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    cooldown_days: int = DEFAULT_COOLDOWN_DAYS,
    channel: str | None = None,
    content_type: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return reuse findings for deterministic theme or angle rows."""
    if days <= 0 or cooldown_days <= 0 or limit <= 0:
        raise ValueError("days, cooldown_days, and limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "cooldown_days": cooldown_days,
        "channel": channel,
        "content_type": content_type,
        "limit": limit,
        "window_start": cutoff.isoformat(),
        "window_end": generated_at.isoformat(),
    }
    skipped = Counter({"missing_theme": 0, "missing_timestamp": 0, "outside_window": 0})
    records: list[dict[str, Any]] = []

    for row in theme_rows:
        row_channel = _text_or_unknown(row.get("channel") or row.get("platform") or row.get("target_channel"))
        row_content_type = _text_or_unknown(row.get("content_type") or row.get("artifact_type") or row.get("type"))
        if channel is not None and row_channel != channel:
            continue
        if content_type is not None and row_content_type != content_type:
            continue

        theme = _theme_text(row)
        if not theme:
            skipped["missing_theme"] += 1
            continue
        used_at = _parse_dt(row.get("used_at") or row.get("published_at") or row.get("generated_at") or row.get("created_at") or row.get("content_created_at"))
        if not used_at:
            skipped["missing_timestamp"] += 1
            continue
        if used_at < cutoff or used_at > generated_at:
            skipped["outside_window"] += 1
            continue

        records.append(
            {
                "content_id": _text(row.get("content_id") or row.get("generated_content_id") or row.get("post_id") or row.get("id")),
                "theme": theme,
                "theme_key": _normalize(theme),
                "channel": row_channel,
                "content_type": row_content_type,
                "used_at": used_at.isoformat(),
            }
        )

    theme_groups = _theme_groups(records, cooldown_days=cooldown_days)
    per_channel_groups = _per_channel_groups(records, cooldown_days=cooldown_days)
    findings = [group for group in theme_groups if group["reused_within_cooldown"]]
    findings.sort(key=lambda item: (item["days_since_last_use"] if item["days_since_last_use"] is not None else 999999, -item["use_count"], item["theme_key"]))
    return {
        "artifact_type": "content_theme_recency",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "item_count": len(records),
            "theme_count": len(theme_groups),
            "reused_theme_count": len(findings),
            **dict(skipped),
        },
        "theme_groups": theme_groups,
        "per_channel_groups": per_channel_groups,
        "findings": findings[:limit],
        "summary": {
            "cross_channel": {
                "theme_count": len(theme_groups),
                "reused_theme_count": len(findings),
            },
            "by_channel": _channel_summary(per_channel_groups),
        },
        "empty_state": {
            "is_empty": not records,
            "message": "No theme rows with usable theme text and timestamps found." if not records else None,
        },
    }


def build_content_theme_recency_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    report = build_content_theme_recency_report(_load_theme_rows(conn, schema), **kwargs)
    report["missing_tables"] = [] if "generated_content" in schema else ["generated_content"]
    return report


def format_content_theme_recency_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_theme_recency_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Content Theme Recency",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} cooldown_days={report['filters']['cooldown_days']} channel={report['filters']['channel'] or 'all'} content_type={report['filters']['content_type'] or 'all'}",
        f"Totals: items={totals['item_count']} themes={totals['theme_count']} reused={totals['reused_theme_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"] or "No themes reused within the cooldown window.")
        return "\n".join(lines)
    lines.extend(["", "Cooldown findings:"])
    for item in report["findings"]:
        lines.append(
            f"- {item['theme']}: uses={item['use_count']} days_since_last_use={item['days_since_last_use']} "
            f"latest={item['latest_used_at']} previous={item['previous_used_at']}"
        )
    return "\n".join(lines)


format_content_theme_recency_table = format_content_theme_recency_text


def _theme_groups(records: list[dict[str, Any]], *, cooldown_days: int) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[record["theme_key"]].append(record)
    rows = [_group_row(theme_key, items, cooldown_days=cooldown_days) for theme_key, items in grouped.items()]
    rows.sort(key=lambda item: (item["theme_key"], item["latest_used_at"] or ""))
    return rows


def _per_channel_groups(records: list[dict[str, Any]], *, cooldown_days: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[(record["channel"], record["theme_key"])].append(record)
    rows = []
    for (channel, theme_key), items in grouped.items():
        row = _group_row(theme_key, items, cooldown_days=cooldown_days)
        row["channel"] = channel
        rows.append(row)
    rows.sort(key=lambda item: (item["channel"], item["theme_key"]))
    return rows


def _group_row(theme_key: str, items: list[dict[str, Any]], *, cooldown_days: int) -> dict[str, Any]:
    ordered = sorted(items, key=lambda item: (item["used_at"], item["content_id"]), reverse=True)
    latest = _parse_dt(ordered[0]["used_at"]) if ordered else None
    previous = _parse_dt(ordered[1]["used_at"]) if len(ordered) > 1 else None
    days_since_last_use = round((latest - previous).total_seconds() / 86400, 2) if latest and previous else None
    return {
        "theme_key": theme_key,
        "theme": ordered[0]["theme"] if ordered else theme_key,
        "use_count": len(items),
        "latest_used_at": latest.isoformat() if latest else None,
        "previous_used_at": previous.isoformat() if previous else None,
        "days_since_last_use": days_since_last_use,
        "reused_within_cooldown": days_since_last_use is not None and days_since_last_use <= cooldown_days,
        "channels": dict(sorted(Counter(item["channel"] for item in items).items())),
        "content_types": dict(sorted(Counter(item["content_type"] for item in items).items())),
        "examples": [
            {"content_id": item["content_id"], "channel": item["channel"], "content_type": item["content_type"], "used_at": item["used_at"]}
            for item in ordered[:5]
        ],
    }


def _channel_summary(groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, dict[str, int]] = defaultdict(lambda: {"theme_count": 0, "reused_theme_count": 0})
    for group in groups:
        counts[group["channel"]]["theme_count"] += 1
        if group["reused_within_cooldown"]:
            counts[group["channel"]]["reused_theme_count"] += 1
    return [{"channel": channel, **values} for channel, values in sorted(counts.items())]


def _load_theme_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []
    rows = _generated_content_rows(conn, schema)
    if any(_theme_text(row) for row in rows):
        return rows
    joined: list[dict[str, Any]] = []
    if "planned_topics" in schema and "content_id" in schema["planned_topics"]:
        joined.extend(_planned_topic_rows(conn, schema))
    if "content_topics" in schema and "content_id" in schema["content_topics"]:
        joined.extend(_content_topic_rows(conn, schema))
    return joined or rows


def _generated_content_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cols = schema["generated_content"]
    if "id" not in cols:
        return []
    selected = [
        "id AS content_id",
        _select(cols, ("theme", "topic", "category"), "theme"),
        _select(cols, ("angle", "hook_angle"), "angle"),
        _select(cols, ("content_type", "type"), "content_type"),
        _select(cols, ("channel", "platform", "target_channel"), "channel"),
        _select(cols, ("published_at", "generated_at", "created_at"), "used_at"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM generated_content ORDER BY id ASC").fetchall()]


def _planned_topic_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    gc = schema["generated_content"]
    pt = schema["planned_topics"]
    selected = [
        "gc.id AS content_id",
        _qselect(pt, "pt", ("topic",), "theme"),
        _qselect(pt, "pt", ("angle",), "angle"),
        _qselect(gc, "gc", ("content_type", "type"), "content_type"),
        _qselect(gc, "gc", ("channel", "platform", "target_channel"), "channel"),
        _qselect(gc, "gc", ("published_at", "generated_at", "created_at"), "used_at"),
    ]
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(selected)}
                FROM planned_topics pt
                JOIN generated_content gc ON gc.id = pt.content_id
                ORDER BY gc.id ASC, pt.id ASC"""
        ).fetchall()
    ]


def _content_topic_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    gc = schema["generated_content"]
    ct = schema["content_topics"]
    selected = [
        "gc.id AS content_id",
        _qselect(ct, "ct", ("topic",), "theme"),
        _qselect(ct, "ct", ("subtopic",), "angle"),
        _qselect(gc, "gc", ("content_type", "type"), "content_type"),
        _qselect(gc, "gc", ("channel", "platform", "target_channel"), "channel"),
        _qselect(gc, "gc", ("published_at", "generated_at", "created_at"), "used_at"),
    ]
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(selected)}
                FROM content_topics ct
                JOIN generated_content gc ON gc.id = ct.content_id
                ORDER BY gc.id ASC, ct.id ASC"""
        ).fetchall()
    ]


def _theme_text(row: dict[str, Any]) -> str:
    theme = _clean(row.get("theme") or row.get("topic"))
    angle = _clean(row.get("angle") or row.get("subtopic"))
    if theme and angle:
        return f"{theme}: {angle}"
    return theme or angle


def _select(columns: set[str], names: tuple[str, ...], alias: str) -> str:
    for name in names:
        if name in columns:
            return f"{name} AS {alias}"
    return f"NULL AS {alias}"


def _qselect(columns: set[str], qualifier: str, names: tuple[str, ...], alias: str) -> str:
    for name in names:
        if name in columns:
            return f"{qualifier}.{name} AS {alias}"
    return f"NULL AS {alias}"


def _normalize(text: str) -> str:
    normalized = re.sub(r"[^a-z0-9\s]+", " ", text.casefold())
    return re.sub(r"\s+", " ", normalized).strip()


def _clean(value: Any) -> str:
    return str(value).strip() if value not in (None, "") else ""


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _text_or_unknown(value: Any) -> str:
    return str(value) if value not in (None, "") else "unknown"


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


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}
