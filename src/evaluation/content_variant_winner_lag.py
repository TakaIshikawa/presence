"""Measure lag for selected content variants before review, approval, and publication."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_STALE_HOURS = 48.0


def build_content_variant_winner_lag_report(
    rows: list[dict[str, Any]],
    *,
    stale_hours: float = DEFAULT_STALE_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a lag report from selected candidate/variant rows."""
    if stale_hours <= 0:
        raise ValueError("stale_hours must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    winners = [_winner(row, generated_at, stale_hours) for row in rows if _selected(row)]

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for winner in winners:
        grouped[(winner["channel"], winner["content_type"])].append(winner)

    summary = []
    for (channel, content_type), group in sorted(grouped.items()):
        summary.append(
            {
                "channel": channel,
                "content_type": content_type,
                "winner_count": len(group),
                "stale_winner_count": sum(1 for row in group if row["is_stale"]),
                "avg_selected_to_review_hours": _avg([row["selected_to_review_hours"] for row in group]),
                "avg_selected_to_approved_hours": _avg([row["selected_to_approved_hours"] for row in group]),
                "avg_selected_to_published_hours": _avg([row["selected_to_published_hours"] for row in group]),
                "unknown_review_count": sum(1 for row in group if row["selected_to_review_hours"] is None),
                "unknown_approved_count": sum(1 for row in group if row["selected_to_approved_hours"] is None),
                "unknown_published_count": sum(1 for row in group if row["selected_to_published_hours"] is None),
            }
        )

    stale_winners = [
        row
        for row in winners
        if row["is_stale"]
    ]
    stale_winners.sort(key=lambda row: (-row["selected_age_hours"], row["variant_id"]))

    return {
        "artifact_type": "content_variant_winner_lag",
        "generated_at": generated_at.isoformat(),
        "filters": {"stale_hours": stale_hours},
        "totals": {
            "winner_count": len(winners),
            "stale_winner_count": len(stale_winners),
            "summary_group_count": len(summary),
        },
        "winners": winners,
        "stale_winners": stale_winners,
        "summary_by_channel_content_type": summary,
    }


def build_content_variant_winner_lag_report_from_db(
    db_or_conn: Any,
    *,
    stale_hours: float = DEFAULT_STALE_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load selected content variants from SQLite and build the lag report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_db_rows(conn, schema)
    report = build_content_variant_winner_lag_report(rows, stale_hours=stale_hours, now=now)
    report["missing_tables"] = [table for table in ("content_variants", "generated_content") if table not in schema]
    return report


def format_content_variant_winner_lag_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_variant_winner_lag_text(report: dict[str, Any]) -> str:
    lines = [
        "Content Variant Winner Lag",
        f"Generated: {report['generated_at']}",
        f"Stale threshold: {report['filters']['stale_hours']}h",
        f"Winners: {report['totals']['winner_count']} stale={report['totals']['stale_winner_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["summary_by_channel_content_type"]:
        lines.append("Summary:")
        for row in report["summary_by_channel_content_type"]:
            lines.append(
                f"- {row['channel']}/{row['content_type']}: winners={row['winner_count']} "
                f"stale={row['stale_winner_count']} review={_fmt(row['avg_selected_to_review_hours'])}h "
                f"approved={_fmt(row['avg_selected_to_approved_hours'])}h "
                f"published={_fmt(row['avg_selected_to_published_hours'])}h"
            )
    if report["stale_winners"]:
        lines.append("Stale winners:")
        for row in report["stale_winners"]:
            lines.append(
                f"- {row['variant_id']}: {row['channel']}/{row['content_type']} "
                f"age={row['selected_age_hours']}h status={row['lag_status']}"
            )
    if not report["stale_winners"]:
        lines.append("No stale selected variants found.")
    return "\n".join(lines)


def _load_db_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "content_variants" not in schema:
        return []
    variant_cols = schema["content_variants"]
    content_cols = schema.get("generated_content", set())
    selected = [
        "content_variants.id AS variant_id",
        "content_variants.content_id AS content_id",
        _select_expr("content_variants", _first_column(variant_cols, "platform", "channel"), "channel", "'unknown'"),
        _select_expr("content_variants", _first_column(variant_cols, "variant_type", "content_type"), "variant_type"),
        _select_expr("content_variants", _first_column(variant_cols, "selected"), "selected", "1"),
        _select_expr("content_variants", _first_column(variant_cols, "created_at", "selected_at"), "selected_at"),
        _select_expr("content_variants", _first_column(variant_cols, "metadata"), "metadata"),
    ]
    join = ""
    if "generated_content" in schema:
        selected.extend(
            [
                _select_expr("generated_content", _first_column(content_cols, "content_type"), "content_type"),
                _select_expr("generated_content", _first_column(content_cols, "published_at"), "published_at"),
                _select_expr("generated_content", _first_column(content_cols, "published"), "published"),
                _select_expr("generated_content", _first_column(content_cols, "created_at"), "content_created_at"),
            ]
        )
        join = "LEFT JOIN generated_content ON generated_content.id = content_variants.content_id"
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM content_variants
            {join}
            WHERE COALESCE(content_variants.selected, 0) = 1
            ORDER BY content_variants.created_at ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _winner(row: dict[str, Any], now: datetime, stale_hours: float) -> dict[str, Any]:
    metadata = _json_object(_first(row, "metadata", "raw_metadata"))
    selected_at = _timestamp(row, metadata, "selected_at", "created_at", "content_created_at") or now
    review_at = _timestamp(row, metadata, "reviewed_at", "review_at", "review_started_at")
    approved_at = _timestamp(row, metadata, "approved_at", "approval_at")
    published_at = _timestamp(row, metadata, "published_at", "publish_at")
    selected_age_hours = _hours(selected_at, now) or 0.0
    lag_values = {
        "selected_to_review_hours": _hours(selected_at, review_at),
        "selected_to_approved_hours": _hours(selected_at, approved_at),
        "selected_to_published_hours": _hours(selected_at, published_at),
    }
    terminal_known = published_at is not None or approved_at is not None
    is_stale = selected_age_hours > 0 and selected_age_hours >= stale_hours and not terminal_known
    return {
        "variant_id": _text(_first(row, "variant_id", "id", "candidate_id")),
        "content_id": _text(_first(row, "content_id", "generated_content_id")),
        "channel": _text(_first(row, "channel", "platform", "target_channel") or "unknown").lower(),
        "content_type": _text(_first(row, "content_type", "variant_type") or "unknown").lower(),
        "selected_at": selected_at.isoformat(),
        "reviewed_at": review_at.isoformat() if review_at else None,
        "approved_at": approved_at.isoformat() if approved_at else None,
        "published_at": published_at.isoformat() if published_at else None,
        "selected_age_hours": round(selected_age_hours, 2),
        **lag_values,
        "lag_status": _lag_status(lag_values),
        "is_stale": is_stale,
    }


def _selected(row: dict[str, Any]) -> bool:
    return _boolish(_first(row, "selected", "is_selected", "winner", "selected_flag")) is not False


def _timestamp(row: dict[str, Any], metadata: dict[str, Any], *names: str) -> datetime | None:
    for name in names:
        parsed = _parse_datetime(row.get(name))
        if parsed:
            return parsed
        parsed = _parse_datetime(metadata.get(name))
        if parsed:
            return parsed
    return None


def _lag_status(values: dict[str, float | None]) -> str:
    if values["selected_to_published_hours"] is not None:
        return "published"
    if values["selected_to_approved_hours"] is not None:
        return "approved"
    if values["selected_to_review_hours"] is not None:
        return "reviewed"
    return "unknown"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {row["name"]: {info["name"] for info in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _first_column(columns: set[str], *names: str) -> str | None:
    return next((name for name in names if name in columns), None)


def _select_expr(table: str, column: str | None, output: str, fallback: str = "NULL") -> str:
    return f"{table}.{column} AS {output}" if column else f"{fallback} AS {output}"


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not _text(value):
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_datetime(value: Any) -> datetime | None:
    if not _text(value):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _hours(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return round((end - start).total_seconds() / 3600, 2)


def _avg(values: list[float | None]) -> float | None:
    known = [value for value in values if value is not None]
    return round(sum(known) / len(known), 2) if known else None


def _boolish(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = _text(value).lower()
    if text in {"1", "true", "yes", "y", "selected", "winner"}:
        return True
    if text in {"0", "false", "no", "n", ""}:
        return False
    return None


def _first(row: dict[str, Any], *names: str) -> Any:
    return next((row[name] for name in names if name in row and row[name] is not None), None)


def _fmt(value: float | None) -> str:
    return "unknown" if value is None else f"{value:.2f}"


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()
