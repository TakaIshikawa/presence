"""Report curated source failure recovery status."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Iterable


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_LIMIT = 100


def build_curated_source_failure_recovery_report(
    rows: list[dict[str, Any]],
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    source_type: str | Iterable[str] = "all",
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
) -> dict[str, Any]:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    source_types = _normalize_filter(source_type)
    items: list[dict[str, Any]] = []
    for row in rows:
        row_type = _clean(row.get("source_type"), "unknown").lower()
        if source_types and "all" not in source_types and row_type not in source_types:
            continue
        status = _classify(row, cutoff)
        if not status:
            continue
        last_success = _parse_dt(row.get("last_success_at"))
        last_failure = _parse_dt(row.get("last_failure_at"))
        basis = last_success if status in {"recovered", "recovering"} else last_failure
        items.append(
            {
                "source_id": _int_or_none(row.get("source_id") or row.get("id")),
                "source_type": row_type,
                "identifier": _identifier(row),
                "status": status,
                "source_status": _clean(row.get("status"), "unknown").lower(),
                "active": _bool(row.get("active")),
                "last_success_at": _iso(last_success),
                "last_failure_at": _iso(last_failure),
                "last_fetch_at": _iso(_parse_dt(row.get("last_fetch_at") or row.get("fetched_at"))),
                "consecutive_failures": _int_or_none(row.get("consecutive_failures")) or 0,
                "recovery_age_days": _age_days(basis, generated_at),
            }
        )
    items.sort(key=lambda item: (item["status"], -(item["recovery_age_days"] or -1), item["identifier"]))
    shown = items[:limit]
    return {
        "artifact_type": "curated_source_failure_recovery",
        "generated_at": generated_at.isoformat(),
        "filters": {"lookback_days": lookback_days, "source_type": list(source_types), "limit": limit},
        "summary": {
            "rows_scanned": len(rows),
            "finding_count": len(items),
            "shown_count": len(shown),
            "by_status": dict(sorted(Counter(item["status"] for item in items).items())),
        },
        "sources": shown,
        "missing_tables": sorted(missing_tables or []),
    }


def build_curated_source_failure_recovery_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing = [] if "curated_sources" in schema else ["curated_sources"]
    rows = _load_rows(conn, schema["curated_sources"]) if not missing else []
    return build_curated_source_failure_recovery_report(rows, missing_tables=missing, **kwargs)


def format_curated_source_failure_recovery_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_curated_source_failure_recovery_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Curated Source Failure Recovery",
        f"Generated: {report['generated_at']}",
        f"Lookback days: {report['filters']['lookback_days']}",
        f"Totals: scanned={summary['rows_scanned']} findings={summary['finding_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["sources"]:
        lines.append("No curated source failure recovery signals found.")
        return "\n".join(lines)
    lines.extend(["", "source_type | identifier | status | active | last_success_at | last_failure_at | consecutive_failures | recovery_age_days"])
    for item in report["sources"]:
        lines.append(
            f"{item['source_type']} | {item['identifier']} | {item['status']} | {item['active']} | "
            f"{item['last_success_at'] or '-'} | {item['last_failure_at'] or '-'} | "
            f"{item['consecutive_failures']} | {item['recovery_age_days'] if item['recovery_age_days'] is not None else '-'}"
        )
    return "\n".join(lines)


def _classify(row: dict[str, Any], cutoff: datetime) -> str | None:
    active = _bool(row.get("active"))
    status = _clean(row.get("status")).lower()
    last_success = _parse_dt(row.get("last_success_at"))
    last_failure = _parse_dt(row.get("last_failure_at"))
    failures = _int_or_none(row.get("consecutive_failures")) or 0
    if not active:
        return None
    if last_failure and last_success and last_success > last_failure:
        return "recovered" if failures == 0 else "recovering"
    if last_failure and failures == 0 and status not in {"failed", "error", "disabled"}:
        return "recovering"
    if last_failure and last_failure < cutoff and (not last_success or last_success <= last_failure):
        return "still_failing"
    if last_failure and failures > 0:
        return "still_failing"
    return None


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    wanted = [
        "id", "source_id", "source_type", "identifier", "source", "name", "url", "feed_url", "handle",
        "status", "active", "last_fetch_at", "fetched_at", "last_success_at", "last_failure_at", "consecutive_failures",
    ]
    select = [_expr(columns, col, col, "NULL") for col in wanted]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM curated_sources ORDER BY rowid ASC")]


def _identifier(row: dict[str, Any]) -> str:
    for key in ("identifier", "source", "name", "url", "feed_url", "handle"):
        value = _clean(row.get(key))
        if value:
            return value
    return "unknown"


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{column} AS {output}" if column in columns else f"{default} AS {output}"


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


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    for candidate in (text, text.replace(" ", "T", 1)):
        try:
            return _utc(datetime.fromisoformat(candidate))
        except ValueError:
            continue
    return None


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _age_days(value: datetime | None, now: datetime) -> int | None:
    return None if value is None else int((now - value).total_seconds() // 86400)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _clean(value).lower() not in {"", "0", "false", "no", "inactive"}


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
