"""Report duplicate proactive actions aimed at the same target."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any
from urllib.parse import urlparse, urlunparse


DEFAULT_DAYS = 7
DEFAULT_MIN_COUNT = 2
DEFAULT_LIMIT = 100
TARGET_COLUMNS = (
    "target_url",
    "inbound_url",
    "url",
    "target_tweet_id",
    "target_author_handle",
    "target_author_id",
    "target_account",
)
REQUIRED_COLUMNS = {"id", "action_type", "status", "discovery_source", "created_at"}
PENDING_REVIEW_STATUSES = {"pending"}


def build_proactive_action_duplicate_targets_report(
    action_rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    min_count: int = DEFAULT_MIN_COUNT,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic duplicate target report from proactive action rows."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_count <= 1:
        raise ValueError("min_count must be greater than 1")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    normalized_rows = [
        row
        for row in (_normalize_row(row) for row in action_rows)
        if row["target_key"]
        and (created_at := _parse_timestamp(row["created_at"])) is not None
        and created_at >= cutoff
    ]
    buckets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in normalized_rows:
        buckets[(row["target_key"], row["action_type"])].append(row)

    groups = [
        _group(rows)
        for (_target_key, _action_type), rows in buckets.items()
        if len(rows) >= min_count
    ]
    groups.sort(key=_group_sort_key)
    shown = groups[:limit]
    return {
        "artifact_type": "proactive_action_duplicate_targets",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "min_count": min_count, "limit": limit},
        "totals": {
            "rows_scanned": len(action_rows),
            "rows_with_targets": len(normalized_rows),
            "duplicate_group_count": len(groups),
            "shown_count": len(shown),
            "pending_duplicate_count": sum(group["pending_duplicate_count"] for group in groups),
            "by_action_type": dict(sorted(Counter(group["action_type"] for group in groups).items())),
        },
        "groups": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {
            table: sorted(columns)
            for table, columns in sorted((missing_columns or {}).items())
            if columns
        },
        "empty_state": {
            "is_empty": not groups,
            "message": "No duplicate proactive action targets found." if not groups else None,
        },
    }


def build_proactive_action_duplicate_targets_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "proactive_actions" not in schema:
        return build_proactive_action_duplicate_targets_report(
            [],
            missing_tables=["proactive_actions"],
            **kwargs,
        )

    columns = schema["proactive_actions"]
    missing = _missing_columns(columns)
    if missing:
        return build_proactive_action_duplicate_targets_report(
            [],
            missing_columns={"proactive_actions": missing},
            **kwargs,
        )

    days = int(kwargs.get("days", DEFAULT_DAYS))
    now = _utc(kwargs.get("now") or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days)
    return build_proactive_action_duplicate_targets_report(
        _load_action_rows(conn, columns, cutoff=cutoff),
        **kwargs,
    )


def format_proactive_action_duplicate_targets_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_proactive_action_duplicate_targets_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Proactive Action Duplicate Targets",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"days={report['filters']['days']} "
            f"min_count={report['filters']['min_count']} "
            f"limit={report['filters']['limit']}"
        ),
        (
            "Totals: "
            f"rows_scanned={totals['rows_scanned']} "
            f"rows_with_targets={totals['rows_with_targets']} "
            f"groups={totals['duplicate_group_count']} "
            f"pending_duplicates={totals['pending_duplicate_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append(
            "Missing columns: "
            + ", ".join(
                f"{table}.{column}"
                for table, columns in report["missing_columns"].items()
                for column in columns
            )
        )
    if not report["groups"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.append("Duplicate targets:")
    for group in report["groups"]:
        lines.append(
            f"  - {group['target_key']} action_type={group['action_type']} "
            f"count={group['action_count']} pending={group['pending_duplicate_count']} "
            f"actions={','.join(str(item) for item in group['action_ids'])}"
        )
    return "\n".join(lines)


def _load_action_rows(conn: sqlite3.Connection, columns: set[str], *, cutoff: datetime) -> list[dict[str, Any]]:
    selected = [
        _column_expr(columns, "id", fallback="NULL"),
        _column_expr(columns, "action_type", fallback="NULL"),
        _column_expr(columns, "status", fallback="NULL"),
        _column_expr(columns, "discovery_source", fallback="NULL"),
        _column_expr(columns, "created_at", fallback="NULL"),
        _column_expr(columns, "target_url", fallback="NULL"),
        _column_expr(columns, "inbound_url", fallback="NULL"),
        _column_expr(columns, "url", fallback="NULL"),
        _column_expr(columns, "target_tweet_id", fallback="NULL"),
        _column_expr(columns, "target_author_handle", fallback="NULL"),
        _column_expr(columns, "target_author_id", fallback="NULL"),
        _column_expr(columns, "target_account", fallback="NULL"),
    ]
    aliases = [
        "id",
        "action_type",
        "status",
        "discovery_source",
        "created_at",
        "target_url",
        "inbound_url",
        "url",
        "target_tweet_id",
        "target_author_handle",
        "target_author_id",
        "target_account",
    ]
    select = ", ".join(f"{expr} AS {alias}" for expr, alias in zip(selected, aliases))
    rows = conn.execute(
        f"""SELECT {select}
            FROM proactive_actions
            WHERE datetime(created_at) >= datetime(?)
            ORDER BY datetime(created_at) ASC, id ASC""",
        (cutoff.isoformat(),),
    ).fetchall()
    return [dict(row) for row in rows]


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    target_key, target_type = _target(row)
    return {
        "action_id": row.get("action_id", row.get("id")),
        "action_type": _normal_text(row.get("action_type")) or "unknown",
        "status": _normal_status(row.get("status")),
        "discovery_source": _normal_text(row.get("discovery_source")) or "unknown",
        "created_at": row.get("created_at"),
        "target_key": target_key,
        "target_type": target_type,
    }


def _group(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: (_parse_timestamp(row["created_at"]) or datetime.min.replace(tzinfo=timezone.utc), _int_or_text(row["action_id"])))
    status_counts = Counter(row["status"] for row in ordered)
    source_counts = Counter(row["discovery_source"] for row in ordered)
    pending_count = sum(row["status"] in PENDING_REVIEW_STATUSES for row in ordered)
    return {
        "target_key": ordered[0]["target_key"],
        "target_type": ordered[0]["target_type"],
        "action_type": ordered[0]["action_type"],
        "action_count": len(ordered),
        "action_ids": [row["action_id"] for row in ordered],
        "statuses": dict(sorted(status_counts.items())),
        "discovery_sources": dict(sorted(source_counts.items())),
        "first_seen_at": ordered[0]["created_at"],
        "last_seen_at": ordered[-1]["created_at"],
        "pending_duplicate_count": pending_count,
        "has_pending_review_duplicates": pending_count > 0,
    }


def _target(row: dict[str, Any]) -> tuple[str | None, str | None]:
    for key in ("target_url", "inbound_url", "url"):
        normalized = _normalize_url(row.get(key))
        if normalized:
            return f"url:{normalized}", "url"
    tweet_id = _normal_text(row.get("target_tweet_id"))
    if tweet_id:
        return f"tweet:{tweet_id}", "tweet"
    handle = _normalize_handle(row.get("target_author_handle") or row.get("target_account"))
    if handle:
        return f"account:{handle}", "account"
    author_id = _normal_text(row.get("target_author_id"))
    if author_id:
        return f"account_id:{author_id}", "account"
    return None, None


def _normalize_url(value: Any) -> str | None:
    text = _normal_text(value)
    if not text:
        return None
    parsed = urlparse(text if "://" in text else f"https://{text}")
    if not parsed.netloc:
        return None
    path = re.sub(r"/+$", "", parsed.path)
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), path, "", "", ""))


def _normalize_handle(value: Any) -> str | None:
    text = _normal_text(value)
    if not text:
        return None
    return text.lstrip("@")


def _missing_columns(columns: set[str]) -> list[str]:
    missing = sorted(REQUIRED_COLUMNS - columns)
    if not set(TARGET_COLUMNS) & columns:
        missing.append("target_url|target_tweet_id|target_author_handle|target_author_id")
    return missing


def _column_expr(columns: set[str], column: str, *, fallback: str) -> str:
    return column if column in columns else fallback


def _normal_status(value: Any) -> str:
    text = _normal_text(value) or "pending"
    if text in {"approved_for_review", "needs_review"}:
        return "pending"
    if text in {"published", "sent", "completed"}:
        return "posted"
    if text in {"rejected", "expired"}:
        return "dismissed"
    return text


def _normal_text(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    return text.lower() if text else None


def _parse_timestamp(value: Any) -> datetime | None:
    text = "" if value is None else str(value).strip()
    if not text:
        return None
    try:
        return _utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None


def _group_sort_key(group: dict[str, Any]) -> tuple[Any, ...]:
    return (
        -int(group["pending_duplicate_count"]),
        -int(group["action_count"]),
        group["target_key"],
        group["action_type"],
    )


def _int_or_text(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, "" if value is None else str(value))


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
