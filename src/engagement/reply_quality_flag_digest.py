"""Digest reply_queue quality flags and low-score draft issues."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MAX_SCORE = 6.0
DEFAULT_STATUSES = ("pending", "approved", "dismissed")
ACTIONABLE_FLAGS = {"generic", "sycophantic"}
REPAIR_RECOMMENDATION = "repair quality_flags to a JSON array of strings"


def build_reply_quality_flag_digest_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    statuses: tuple[str, ...] | list[str] | None = None,
    max_score: float = DEFAULT_MAX_SCORE,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only operational digest for reply draft quality issues."""

    if days < 1:
        raise ValueError("days must be at least 1")
    if not 0 <= max_score <= 10:
        raise ValueError("max_score must be between 0 and 10")

    selected_statuses = _normalize_statuses(statuses)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = tuple(table for table in ("reply_queue",) if table not in schema)
    missing_columns = _missing_columns(schema)
    filters = {
        "days": days,
        "statuses": list(selected_statuses),
        "max_score": max_score,
    }
    if missing_tables:
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    rows = _load_reply_rows(
        conn,
        schema["reply_queue"],
        cutoff=cutoff,
        now=generated_at,
        statuses=selected_statuses,
    )
    malformed = [row["malformed_quality_flags"] for row in rows if row["malformed_quality_flags"]]
    actionable = _actionable_rows(rows, max_score=max_score)
    totals = _totals(rows, actionable=actionable, max_score=max_score)

    return {
        "artifact_type": "reply_quality_flag_digest",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": totals,
        "counts": {
            "by_status": _count(rows, "status"),
            "by_intent": _count(rows, "intent"),
            "by_platform": _count(rows, "platform"),
            "by_quality_flag": _flag_counts(rows),
        },
        "malformed_quality_flags": malformed,
        "actionable_replies": actionable,
        "missing_tables": list(missing_tables),
        "missing_columns": {
            table: list(columns) for table, columns in sorted(missing_columns.items())
        },
        "has_issues": bool(malformed or actionable),
    }


def format_reply_quality_flag_digest_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON for automation."""

    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_quality_flag_digest_text(report: dict[str, Any]) -> str:
    """Render a compact deterministic operator digest."""

    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Reply Quality Flag Digest",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"days={filters['days']} "
            f"statuses={','.join(filters['statuses'])} "
            f"max_score={filters['max_score']:.1f}"
        ),
        (
            "Totals: "
            f"rows={totals['row_count']} "
            f"scored={totals['scored_count']} "
            f"low_score={totals['low_score_count']} "
            f"actionable={totals['actionable_count']} "
            f"malformed_flags={totals['malformed_quality_flags_count']}"
        ),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    missing_columns = [
        f"{table}({', '.join(columns)})"
        for table, columns in report.get("missing_columns", {}).items()
        if columns
    ]
    if missing_columns:
        lines.append("Missing optional columns: " + "; ".join(missing_columns))

    lines.extend(["", "Counts by status:"])
    lines.extend(_format_counts(report["counts"]["by_status"]))
    lines.extend(["", "Counts by intent:"])
    lines.extend(_format_counts(report["counts"]["by_intent"]))
    lines.extend(["", "Counts by platform:"])
    lines.extend(_format_counts(report["counts"]["by_platform"]))
    lines.extend(["", "Counts by quality flag:"])
    lines.extend(_format_counts(report["counts"]["by_quality_flag"]))

    malformed = report["malformed_quality_flags"]
    if malformed:
        lines.extend(["", "Malformed quality_flags:"])
        for item in malformed:
            lines.append(
                f"- reply_queue:{item['reply_queue_id']} @{item['author_handle']} "
                f"classification={item['classification']} action={item['repair_recommendation']}"
            )

    actionable = report["actionable_replies"]
    if actionable:
        lines.extend(["", "Actionable replies:"])
        for item in actionable:
            flags = ",".join(item["quality_flags"]) if item["quality_flags"] else "-"
            score = "-" if item["quality_score"] is None else f"{item['quality_score']:.1f}"
            lines.append(
                f"- reply_queue:{item['reply_queue_id']} @{item['author_handle']} "
                f"status={item['status']} intent={item['intent']} platform={item['platform']} "
                f"score={score} flags={flags} action={item['recommended_action']}"
            )
    elif not malformed and not report.get("missing_tables"):
        lines.extend(["", "No reply quality issues matched."])

    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> dict[str, Any]:
    return {
        "artifact_type": "reply_quality_flag_digest",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "row_count": 0,
            "scored_count": 0,
            "low_score_count": 0,
            "actionable_count": 0,
            "malformed_quality_flags_count": 0,
        },
        "counts": {
            "by_status": {},
            "by_intent": {},
            "by_platform": {},
            "by_quality_flag": {},
        },
        "malformed_quality_flags": [],
        "actionable_replies": [],
        "missing_tables": list(missing_tables),
        "missing_columns": {
            table: list(columns) for table, columns in sorted(missing_columns.items())
        },
        "has_issues": False,
    }


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in rows}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    expected = {
        "reply_queue": (
            "id",
            "inbound_author_handle",
            "status",
            "intent",
            "platform",
            "quality_score",
            "quality_flags",
            "detected_at",
            "reviewed_at",
            "posted_at",
        )
    }
    return {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema
    }


def _load_reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
    now: datetime,
    statuses: tuple[str, ...],
) -> list[dict[str, Any]]:
    select_columns = [
        _column_expr(columns, "id"),
        _column_expr(columns, "inbound_author_handle"),
        _column_expr(columns, "status", "'pending'"),
        _column_expr(columns, "intent", "'other'"),
        _column_expr(columns, "platform", "'x'"),
        _column_expr(columns, "quality_score"),
        _column_expr(columns, "quality_flags"),
        _column_expr(columns, "detected_at"),
        _column_expr(columns, "reviewed_at"),
        _column_expr(columns, "posted_at"),
    ]
    raw_rows = conn.execute(
        f"SELECT {', '.join(select_columns)} FROM reply_queue ORDER BY id ASC"
    ).fetchall()

    rows: list[dict[str, Any]] = []
    for raw in raw_rows:
        row = dict(raw)
        reply_id = _int_or_none(row.get("id"))
        if reply_id is None:
            continue
        status = _clean_label(row.get("status")) or "pending"
        if status not in statuses:
            continue
        timestamp = (
            _parse_timestamp(row.get("detected_at"))
            or _parse_timestamp(row.get("reviewed_at"))
            or _parse_timestamp(row.get("posted_at"))
            or now
        )
        if not cutoff <= timestamp <= now:
            continue
        author_handle = _normalize_handle(row.get("inbound_author_handle"))
        parsed_flags = _parse_quality_flags(row.get("quality_flags"), reply_id=reply_id)
        if parsed_flags["malformed"]:
            parsed_flags["malformed"]["author_handle"] = author_handle
            parsed_flags["malformed"]["status"] = status
            parsed_flags["malformed"]["intent"] = _clean_label(row.get("intent")) or "other"
        rows.append(
            {
                "reply_queue_id": reply_id,
                "author_handle": author_handle,
                "status": status,
                "intent": _clean_label(row.get("intent")) or "other",
                "platform": _clean_label(row.get("platform")) or "x",
                "quality_score": _float_or_none(row.get("quality_score")),
                "quality_flags": parsed_flags["flags"],
                "malformed_quality_flags": parsed_flags["malformed"],
                "detected_at": timestamp.isoformat(),
            }
        )
    return rows


def _parse_quality_flags(raw: Any, *, reply_id: int) -> dict[str, Any]:
    if raw is None or str(raw).strip() == "":
        return {"flags": [], "malformed": None}
    classification = None
    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        parsed = None
        classification = "invalid_json"
    if classification is None and not isinstance(parsed, list):
        classification = "not_json_array"
    if classification is None:
        flags = []
        for item in parsed:
            if isinstance(item, str) and item.strip():
                flags.append(item.strip().lower())
            else:
                classification = "array_contains_non_scalar"
                break
    else:
        flags = []
    if classification is None:
        return {"flags": sorted(set(flags)), "malformed": None}
    return {
        "flags": [],
        "malformed": {
            "reply_queue_id": reply_id,
            "classification": classification,
            "raw_value": str(raw),
            "repair_recommendation": REPAIR_RECOMMENDATION,
        },
    }


def _totals(
    rows: list[dict[str, Any]],
    *,
    actionable: list[dict[str, Any]],
    max_score: float,
) -> dict[str, int]:
    return {
        "row_count": len(rows),
        "scored_count": sum(1 for row in rows if row["quality_score"] is not None),
        "low_score_count": sum(
            1
            for row in rows
            if row["quality_score"] is not None and row["quality_score"] < max_score
        ),
        "actionable_count": len(actionable),
        "malformed_quality_flags_count": sum(1 for row in rows if row["malformed_quality_flags"]),
    }


def _actionable_rows(rows: list[dict[str, Any]], *, max_score: float) -> list[dict[str, Any]]:
    actionable: list[dict[str, Any]] = []
    for row in rows:
        score = row["quality_score"]
        flags = set(row["quality_flags"])
        reasons: list[str] = []
        if score is not None and score < max_score:
            reasons.append("low_quality_score")
        if flags & ACTIONABLE_FLAGS:
            reasons.extend(f"flag:{flag}" for flag in sorted(flags & ACTIONABLE_FLAGS))
        if not reasons:
            continue
        actionable.append(
            {
                "reply_queue_id": row["reply_queue_id"],
                "author_handle": row["author_handle"],
                "status": row["status"],
                "intent": row["intent"],
                "platform": row["platform"],
                "quality_score": score,
                "quality_flags": row["quality_flags"],
                "issue_reasons": reasons,
                "recommended_action": _recommended_action(score, flags, max_score),
            }
        )
    actionable.sort(
        key=lambda item: (
            item["quality_score"] is None,
            item["quality_score"] if item["quality_score"] is not None else 99.0,
            item["reply_queue_id"],
        )
    )
    return actionable


def _recommended_action(score: float | None, flags: set[str], max_score: float) -> str:
    if "sycophantic" in flags:
        return "rewrite to remove praise and answer substantively"
    if "generic" in flags:
        return "rewrite with specific context from the thread"
    if score is not None and score < max_score:
        return "review or rewrite before approving"
    return "review quality flags"


def _count(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counter = Counter(str(row[key]) for row in rows)
    return dict(sorted(counter.items()))


def _flag_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        counter.update(row["quality_flags"])
    return dict(sorted(counter.items()))


def _format_counts(counts: dict[str, int]) -> list[str]:
    if not counts:
        return ["- none"]
    return [f"- {key}: {value}" for key, value in sorted(counts.items())]


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    if column in columns:
        return column
    return f"{default} AS {column}"


def _normalize_statuses(statuses: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    values = statuses or DEFAULT_STATUSES
    normalized = tuple(sorted({_clean_label(status) or "" for status in values if str(status).strip()}))
    if not normalized:
        raise ValueError("at least one status is required")
    return normalized


def _parse_timestamp(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return _as_utc(raw)
    text = str(raw).strip()
    if not text:
        return None
    try:
        return _as_utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return _as_utc(datetime.strptime(text, fmt))
        except ValueError:
            continue
    return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clean_label(raw: Any) -> str | None:
    if raw is None:
        return None
    value = str(raw).strip().lower()
    return value or None


def _normalize_handle(raw: Any) -> str:
    value = str(raw or "").strip().lstrip("@").lower()
    return value or "(unknown)"


def _float_or_none(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _int_or_none(raw: Any) -> int | None:
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None
