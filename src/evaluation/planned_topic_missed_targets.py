"""Report planned topics whose target dates have passed without publication."""

from __future__ import annotations

from datetime import date, datetime, timezone
import json
import sqlite3
from typing import Any, Iterable


DEFAULT_MIN_DAYS_OVERDUE = 1
DEFAULT_LIMIT = 100


def build_planned_topic_missed_targets_report(
    rows: list[dict[str, Any]],
    *,
    as_of: date | datetime | str | None = None,
    min_days_overdue: int = DEFAULT_MIN_DAYS_OVERDUE,
    campaign_status: str | Iterable[str] = "all",
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
) -> dict[str, Any]:
    if min_days_overdue < 0:
        raise ValueError("min_days_overdue must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    as_of_date = _parse_date(as_of) or generated_at.date()
    campaign_statuses = _normalize_filter(campaign_status)
    findings: list[dict[str, Any]] = []
    scanned = 0
    for row in rows:
        if campaign_statuses and "all" not in campaign_statuses:
            if _clean(row.get("campaign_status"), "unknown").lower() not in campaign_statuses:
                continue
        target_date = _parse_date(row.get("target_date"))
        if target_date is None:
            continue
        days_overdue = (as_of_date - target_date).days
        if days_overdue < min_days_overdue:
            continue
        scanned += 1
        published = _published(row.get("published"))
        status = _clean(row.get("status"), "planned").lower()
        content_id = row.get("content_id")
        if published:
            continue
        if status not in {"planned", "skipped"} and content_id in (None, ""):
            continue
        findings.append(
            {
                "planned_topic_id": _int_or_none(row.get("planned_topic_id") or row.get("id")),
                "campaign_id": _int_or_none(row.get("campaign_id")),
                "campaign_name": _clean(row.get("campaign_name")) or None,
                "campaign_status": _clean(row.get("campaign_status"), "unknown").lower(),
                "topic": _clean(row.get("topic")) or None,
                "angle": _clean(row.get("angle")) or None,
                "target_date": target_date.isoformat(),
                "status": status,
                "content_id": _int_or_none(content_id),
                "published": False,
                "days_overdue": days_overdue,
            }
        )
    findings.sort(key=lambda item: (-item["days_overdue"], item["campaign_name"] or "", item["planned_topic_id"] or 0))
    shown = findings[:limit]
    return {
        "artifact_type": "planned_topic_missed_targets",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "as_of": as_of_date.isoformat(),
            "min_days_overdue": min_days_overdue,
            "campaign_status": list(campaign_statuses),
            "limit": limit,
        },
        "summary": {"rows_scanned": scanned, "missed_target_count": len(findings), "shown_count": len(shown)},
        "missed_targets": shown,
        "missing_tables": sorted(missing_tables or []),
    }


def build_planned_topic_missed_targets_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = {"planned_topics"}
    missing = sorted(required - set(schema))
    rows = _load_rows(conn, schema) if not missing else []
    return build_planned_topic_missed_targets_report(rows, missing_tables=missing, **kwargs)


def format_planned_topic_missed_targets_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_planned_topic_missed_targets_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Planned Topic Missed Targets",
        f"Generated: {report['generated_at']}",
        f"As of: {report['filters']['as_of']}",
        f"Minimum days overdue: {report['filters']['min_days_overdue']}",
        f"Totals: scanned={summary['rows_scanned']} missed={summary['missed_target_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["missed_targets"]:
        lines.append("No planned topic missed targets found.")
        return "\n".join(lines)
    lines.extend(["", "planned_topic_id | campaign_name | topic | target_date | status | content_id | published | days_overdue"])
    for item in report["missed_targets"]:
        lines.append(
            f"{item['planned_topic_id'] or '-'} | {item['campaign_name'] or '-'} | {item['topic'] or '-'} | "
            f"{item['target_date']} | {item['status']} | {item['content_id'] or '-'} | {item['published']} | {item['days_overdue']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    pt = schema["planned_topics"]
    cc = schema.get("content_campaigns", set())
    gc = schema.get("generated_content", set())
    campaign_expr = _column_expr(pt, "campaign_id", "content_campaign_id", fallback="NULL", alias="pt")
    content_expr = _column_expr(pt, "content_id", "generated_content_id", fallback="NULL", alias="pt")
    campaign_join = "LEFT JOIN content_campaigns cc ON cc.id = " + campaign_expr if "content_campaigns" in schema and "id" in cc else ""
    content_join = "LEFT JOIN generated_content gc ON gc.id = " + content_expr if "generated_content" in schema and "id" in gc else ""
    select = [
        _column_expr(pt, "id", fallback="pt.rowid", alias="pt") + " AS planned_topic_id",
        f"{campaign_expr} AS campaign_id",
        _column_expr(cc, "name", fallback="NULL", alias="cc") + " AS campaign_name",
        _column_expr(cc, "status", fallback="'unknown'", alias="cc") + " AS campaign_status",
        _column_expr(pt, "topic", "title", fallback="NULL", alias="pt") + " AS topic",
        _column_expr(pt, "angle", fallback="NULL", alias="pt") + " AS angle",
        _column_expr(pt, "target_date", fallback="NULL", alias="pt") + " AS target_date",
        _column_expr(pt, "status", fallback="'planned'", alias="pt") + " AS status",
        f"{content_expr} AS content_id",
        _column_expr(gc, "published", fallback="NULL", alias="gc") + " AS published",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM planned_topics pt {campaign_join} {content_join} ORDER BY pt.rowid ASC")]


def _column_expr(columns: set[str], *names: str, fallback: str, alias: str) -> str:
    for name in names:
        if name in columns:
            return f"{alias}.{name}"
    return fallback


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _normalize_filter(value: str | Iterable[str]) -> tuple[str, ...]:
    values = value.split(",") if isinstance(value, str) else list(value)
    normalized = tuple(item for item in (_clean(part).lower() for part in values) if item)
    return normalized or ("all",)


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return _utc(value).date()
    if isinstance(value, date):
        return value
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def _published(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = _clean(value).lower()
    return text in {"1", "true", "yes", "published"}


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
