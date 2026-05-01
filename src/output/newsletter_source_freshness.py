"""Check newsletter sends for stale or repeatedly reused source content."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MAX_SOURCE_AGE_DAYS = 14
DEFAULT_MAX_REUSE_COUNT = 2


def build_newsletter_source_freshness(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    max_source_age_days: int = DEFAULT_MAX_SOURCE_AGE_DAYS,
    max_reuse_count: int = DEFAULT_MAX_REUSE_COUNT,
    issue_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return a read-only freshness report for recent newsletter source material."""
    if days <= 0:
        raise ValueError("days must be positive")
    if max_source_age_days < 0:
        raise ValueError("max_source_age_days must be non-negative")
    if max_reuse_count < 1:
        raise ValueError("max_reuse_count must be positive")

    conn = getattr(db_or_conn, "conn", db_or_conn)
    schema = _schema(conn)
    now = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days)
    filters = {
        "days": days,
        "max_source_age_days": max_source_age_days,
        "max_reuse_count": max_reuse_count,
        "issue_id": issue_id,
        "cutoff": cutoff.isoformat(),
    }
    if "newsletter_sends" not in schema:
        return _empty_report(now, filters, ["newsletter_sends"])

    sends = _load_sends(conn, schema, cutoff=cutoff, issue_id=issue_id)
    parsed_by_send: dict[int, tuple[list[int], list[str]]] = {}
    reuse_counter: Counter[int] = Counter()
    for send in sends:
        source_ids, parse_warnings = parse_source_content_ids(
            send.get("source_content_ids")
        )
        parsed_by_send[int(send["newsletter_send_id"])] = (
            source_ids,
            parse_warnings,
        )
        reuse_counter.update(source_ids)

    content = _load_generated_content(conn, schema, reuse_counter.keys())
    publication_statuses = _load_publication_statuses(conn, schema, reuse_counter.keys())

    report_sends = []
    warning_counts: Counter[str] = Counter()
    source_count = 0
    missing_source_count = 0
    stale_source_count = 0
    repeated_source_count = 0
    unpublished_source_count = 0

    for send in sends:
        send_id = int(send["newsletter_send_id"])
        source_ids, parse_warnings = parsed_by_send[send_id]
        send_warnings = set(parse_warnings)
        if not source_ids and "malformed_source_content_ids" not in send_warnings:
            send_warnings.add("missing_source_content_ids")

        sources = []
        sent_at = _parse_datetime(send.get("sent_at"))
        for content_id in source_ids:
            source_count += 1
            content_row = content.get(content_id)
            reuse_count = reuse_counter[content_id]
            item_warnings: list[str] = []
            if content_row is None:
                item_warnings.append("missing_source_row")
                missing_source_count += 1
                sources.append(
                    {
                        "content_id": content_id,
                        "content_type": None,
                        "created_at": None,
                        "source_age_days": None,
                        "reuse_count": reuse_count,
                        "publication_status": "missing",
                        "warnings": item_warnings,
                    }
                )
                send_warnings.update(item_warnings)
                continue

            created_at = _parse_datetime(content_row.get("created_at"))
            source_age_days = _age_days(created_at, sent_at)
            publication_status = _publication_status(
                content_row, publication_statuses.get(content_id, [])
            )
            if (
                source_age_days is not None
                and source_age_days > max_source_age_days
            ):
                item_warnings.append("stale_source")
                stale_source_count += 1
            if reuse_count > max_reuse_count:
                item_warnings.append("repeated_source")
                repeated_source_count += 1
            if publication_status != "published":
                item_warnings.append("unpublished_source")
                unpublished_source_count += 1

            sources.append(
                {
                    "content_id": content_id,
                    "content_type": content_row.get("content_type"),
                    "created_at": content_row.get("created_at"),
                    "source_age_days": source_age_days,
                    "reuse_count": reuse_count,
                    "publication_status": publication_status,
                    "warnings": item_warnings,
                }
            )
            send_warnings.update(item_warnings)

        for warning in send_warnings:
            warning_counts[warning] += 1

        report_sends.append(
            {
                "newsletter_send_id": send_id,
                "issue_id": send.get("issue_id") or "",
                "subject": send.get("subject") or "",
                "sent_at": send.get("sent_at") or "",
                "status": send.get("status") or "",
                "source_content_ids": source_ids,
                "sources": sources,
                "warnings": sorted(send_warnings),
            }
        )

    return {
        "generated_at": now.isoformat(),
        "filters": filters,
        "summary": {
            "send_count": len(report_sends),
            "source_count": source_count,
            "missing_source_count": missing_source_count,
            "stale_source_count": stale_source_count,
            "repeated_source_count": repeated_source_count,
            "unpublished_source_count": unpublished_source_count,
            "warning_counts": dict(sorted(warning_counts.items())),
        },
        "sends": report_sends,
    }


def format_newsletter_source_freshness_json(report: dict[str, Any]) -> str:
    """Render the freshness report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_source_freshness_text(report: dict[str, Any]) -> str:
    """Render a compact operator-facing source freshness report."""
    filters = report["filters"]
    summary = report["summary"]
    lines = [
        "Newsletter source freshness report",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days={filters['days']} "
            f"max_source_age_days={filters['max_source_age_days']} "
            f"max_reuse_count={filters['max_reuse_count']} "
            f"issue_id={filters['issue_id'] or 'all'}"
        ),
        (
            "Totals: "
            f"sends={summary['send_count']} "
            f"sources={summary['source_count']} "
            f"stale={summary['stale_source_count']} "
            f"repeated={summary['repeated_source_count']} "
            f"unpublished={summary['unpublished_source_count']} "
            f"missing={summary['missing_source_count']}"
        ),
        "",
    ]
    if not report["sends"]:
        lines.append("No newsletter sends found.")
        return "\n".join(lines)

    for send in report["sends"]:
        warnings = ", ".join(send["warnings"]) if send["warnings"] else "-"
        lines.append(
            f"Send {send['newsletter_send_id']} issue={send['issue_id'] or '-'} "
            f"status={send['status'] or '-'} sent_at={send['sent_at'] or '-'} "
            f"warnings={warnings}"
        )
        if not send["sources"]:
            lines.append("  - no source content IDs")
            continue
        lines.append("  ID      Age  Reuse  Publication  Warnings")
        for source in send["sources"]:
            age = (
                "-"
                if source["source_age_days"] is None
                else f"{source['source_age_days']:.1f}d"
            )
            source_warnings = (
                ", ".join(source["warnings"]) if source["warnings"] else "-"
            )
            lines.append(
                f"  {source['content_id']:<7} "
                f"{age:<5} "
                f"{source['reuse_count']:<6} "
                f"{source['publication_status']:<12} "
                f"{source_warnings}"
            )
    return "\n".join(lines)


def parse_source_content_ids(raw_value: Any) -> tuple[list[int], list[str]]:
    """Parse newsletter_sends.source_content_ids without raising on bad data."""
    if raw_value in (None, ""):
        return [], ["missing_source_content_ids"]
    try:
        parsed = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    except (TypeError, json.JSONDecodeError):
        return [], ["malformed_source_content_ids"]

    if not isinstance(parsed, list):
        return [], ["malformed_source_content_ids"]

    source_ids: list[int] = []
    malformed = False
    for item in parsed:
        try:
            content_id = int(item)
        except (TypeError, ValueError):
            malformed = True
            continue
        if content_id <= 0:
            malformed = True
            continue
        source_ids.append(content_id)

    warnings = ["malformed_source_content_ids"] if malformed else []
    if not source_ids and not warnings:
        warnings.append("missing_source_content_ids")
    return source_ids, warnings


def _load_sends(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    issue_id: str | None,
) -> list[dict[str, Any]]:
    columns = schema["newsletter_sends"]
    required = {"id", "source_content_ids"}
    if not required.issubset(columns):
        return []
    select = {
        "newsletter_send_id": "ns.id",
        "issue_id": _column_expr(columns, "issue_id", alias="ns"),
        "subject": _column_expr(columns, "subject", "''", alias="ns"),
        "source_content_ids": "ns.source_content_ids",
        "status": _column_expr(columns, "status", "''", alias="ns"),
        "sent_at": _column_expr(columns, "sent_at", "NULL", alias="ns"),
    }
    filters = []
    params: list[Any] = []
    if "sent_at" in columns:
        filters.append("ns.sent_at >= ?")
        params.append(cutoff.isoformat())
    if issue_id is not None and "issue_id" in columns:
        filters.append("ns.issue_id = ?")
        params.append(issue_id)
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT
                   {select['newsletter_send_id']} AS newsletter_send_id,
                   {select['issue_id']} AS issue_id,
                   {select['subject']} AS subject,
                   {select['source_content_ids']} AS source_content_ids,
                   {select['status']} AS status,
                   {select['sent_at']} AS sent_at
               FROM newsletter_sends ns
               {where_clause}
               ORDER BY {select['sent_at']} DESC, ns.id DESC""",
            params,
        ).fetchall()
    ]


def _load_generated_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: Any,
) -> dict[int, dict[str, Any]]:
    ids = sorted({int(content_id) for content_id in content_ids})
    columns = schema.get("generated_content")
    if not ids or not columns or "id" not in columns:
        return {}
    placeholders = ",".join("?" for _ in ids)
    select = {
        "id": "gc.id",
        "content_type": _column_expr(columns, "content_type", alias="gc"),
        "created_at": _column_expr(columns, "created_at", alias="gc"),
        "published": _column_expr(columns, "published", "0", alias="gc"),
        "published_at": _column_expr(columns, "published_at", alias="gc"),
        "published_url": _column_expr(columns, "published_url", alias="gc"),
        "tweet_id": _column_expr(columns, "tweet_id", alias="gc"),
    }
    rows = conn.execute(
        f"""SELECT
               {select['id']} AS id,
               {select['content_type']} AS content_type,
               {select['created_at']} AS created_at,
               {select['published']} AS published,
               {select['published_at']} AS published_at,
               {select['published_url']} AS published_url,
               {select['tweet_id']} AS tweet_id
           FROM generated_content gc
           WHERE gc.id IN ({placeholders})""",
        ids,
    ).fetchall()
    return {int(row["id"]): dict(row) for row in rows}


def _load_publication_statuses(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: Any,
) -> dict[int, list[str]]:
    ids = sorted({int(content_id) for content_id in content_ids})
    columns = schema.get("content_publications")
    if not ids or not columns or not {"content_id", "status"}.issubset(columns):
        return {}
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"""SELECT content_id, status
            FROM content_publications
            WHERE content_id IN ({placeholders})
              AND status IS NOT NULL
            ORDER BY content_id ASC, status ASC""",
        ids,
    ).fetchall()
    statuses: dict[int, list[str]] = {}
    for row in rows:
        statuses.setdefault(int(row["content_id"]), []).append(str(row["status"]))
    return statuses


def _publication_status(row: dict[str, Any], statuses: list[str]) -> str:
    status_set = {status for status in statuses if status}
    if (
        _truthy(row.get("published"))
        or row.get("published_at")
        or row.get("published_url")
        or row.get("tweet_id")
        or "published" in status_set
    ):
        return "published"
    for status in ("failed", "queued", "held", "cancelled"):
        if status in status_set:
            return status
    return "unpublished"


def _age_days(created_at: datetime | None, sent_at: datetime | None) -> float | None:
    if created_at is None or sent_at is None:
        return None
    return round((sent_at - created_at).total_seconds() / 86400, 2)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _ensure_utc(parsed)


def _empty_report(
    now: datetime, filters: dict[str, Any], missing_required_tables: list[str]
) -> dict[str, Any]:
    return {
        "generated_at": now.isoformat(),
        "filters": filters,
        "summary": {
            "send_count": 0,
            "source_count": 0,
            "missing_source_count": 0,
            "stale_source_count": 0,
            "repeated_source_count": 0,
            "unpublished_source_count": 0,
            "warning_counts": {},
        },
        "sends": [],
        "missing_required_tables": missing_required_tables,
    }


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str = "NULL",
    *,
    alias: str,
) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
