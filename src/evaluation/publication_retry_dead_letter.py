"""Report publication targets that may need manual retry intervention."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any

from output.publish_errors import classify_publish_error, normalize_error_category


DEFAULT_LIMIT = 100
DEFAULT_MIN_FAILURES = 3
DEFAULT_OLDER_THAN_HOURS = 24.0

_TARGET_STATUSES = {"failed", "held"}
_MANUAL_REVIEW_CATEGORIES = {"auth", "duplicate", "media", "validation"}


@dataclass(frozen=True)
class PublicationRetryDeadLetterRow:
    """One content/platform publication target with repeated failures."""

    content_id: int
    platform: str
    failure_count: int
    latest_attempt_at: str | None
    latest_error: str | None
    latest_error_category: str
    next_retry_at: str | None
    queue_status: str | None
    publication_status: str | None
    content_excerpt: str | None
    dead_letter_candidate: bool
    recommended_action: str
    sources: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["sources"] = list(self.sources)
        return payload


@dataclass(frozen=True)
class PublicationRetryDeadLetterReport:
    """Dead-letter candidate report with filters and schema metadata."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    rows: tuple[PublicationRetryDeadLetterRow, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "publication_retry_dead_letter",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


def build_publication_retry_dead_letter_report(
    db_or_conn: Any,
    *,
    min_failures: int = DEFAULT_MIN_FAILURES,
    older_than_hours: float = DEFAULT_OLDER_THAN_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> PublicationRetryDeadLetterReport:
    """Return publication targets that are failed, held, or repeatedly failing."""
    if min_failures <= 0:
        raise ValueError("min_failures must be positive")
    if older_than_hours <= 0:
        raise ValueError("older_than_hours must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _aware(now or datetime.now(timezone.utc))
    filters = {
        "min_failures": min_failures,
        "older_than_hours": older_than_hours,
        "limit": limit,
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_columns: dict[str, tuple[str, ...]] = {}
    missing_tables = tuple(
        table
        for table in (
            "content_publications",
            "publication_attempts",
            "publish_queue",
            "generated_content",
        )
        if table not in schema
    )

    groups: dict[tuple[int, str], dict[str, Any]] = {}
    _merge_attempts(conn, schema, groups, missing_columns)
    _merge_publications(conn, schema, groups, missing_columns)
    _merge_queue(conn, schema, groups, missing_columns)
    _merge_generated_content(conn, schema, groups, missing_columns)

    rows = [
        _build_row(
            group,
            now=generated_at,
            min_failures=min_failures,
            older_than_hours=older_than_hours,
        )
        for group in groups.values()
        if _include_group(group)
    ]
    rows.sort(key=_sort_key)
    limited = tuple(rows[:limit])
    return PublicationRetryDeadLetterReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "candidate_count": sum(row.dead_letter_candidate for row in limited),
            "held_count": sum(row.queue_status == "held" for row in limited),
            "row_count": len(limited),
            "total_groups": len(rows),
        },
        rows=limited,
        missing_tables=missing_tables,
        missing_columns={
            table: columns for table, columns in missing_columns.items() if columns
        },
    )


def format_publication_retry_dead_letter_json(
    report: PublicationRetryDeadLetterReport,
) -> str:
    """Serialize the publication retry dead-letter report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publication_retry_dead_letter_text(
    report: PublicationRetryDeadLetterReport,
) -> str:
    """Render the dead-letter candidate report for operators."""
    lines = [
        "Publication Retry Dead-Letter Report",
        f"Generated: {report.generated_at}",
        (
            f"Filters: min_failures={report.filters['min_failures']} "
            f"older_than_hours={report.filters['older_than_hours']} "
            f"limit={report.filters['limit']}"
        ),
        (
            "Totals: "
            f"rows={report.totals['row_count']} "
            f"candidates={report.totals['candidate_count']} "
            f"held={report.totals['held_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        ]
        lines.append(f"Missing columns: {'; '.join(missing)}")
    lines.append("")
    if not report.rows:
        lines.append("No failed or held publication retry targets found.")
        return "\n".join(lines)

    columns = [
        ("content_id", "CONTENT", 8),
        ("platform", "PLATFORM", 10),
        ("failure_count", "FAILURES", 8),
        ("latest_attempt_at", "LATEST_ATTEMPT", 25),
        ("latest_error_category", "CATEGORY", 11),
        ("next_retry_at", "NEXT_RETRY", 25),
        ("dead_letter_candidate", "DEAD", 5),
        ("recommended_action", "ACTION", 25),
    ]
    lines.append("  ".join(label.ljust(width) for _, label, width in columns))
    lines.append("  ".join("-" * width for _, _, width in columns))
    for row in report.rows:
        values = row.to_dict()
        values["latest_attempt_at"] = row.latest_attempt_at or "-"
        values["next_retry_at"] = row.next_retry_at or "-"
        values["dead_letter_candidate"] = "yes" if row.dead_letter_candidate else "no"
        lines.append(
            "  ".join(
                _format_cell(values.get(key), width).ljust(width)
                for key, _, width in columns
            )
        )
    return "\n".join(lines)


def _merge_attempts(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    groups: dict[tuple[int, str], dict[str, Any]],
    missing_columns: dict[str, tuple[str, ...]],
) -> None:
    columns = schema.get("publication_attempts")
    if columns is None:
        return
    required = ("content_id", "platform", "success")
    missing = [column for column in required if column not in columns]
    optional = [column for column in ("attempted_at", "error", "error_category") if column not in columns]
    if missing:
        missing_columns["publication_attempts"] = tuple(sorted([*missing, *optional]))
        return
    if optional:
        missing_columns["publication_attempts"] = tuple(sorted(optional))
    attempted_expr = _column_expr(columns, "attempted_at", "NULL")
    error_expr = _column_expr(columns, "error", "NULL")
    category_expr = _column_expr(columns, "error_category", "NULL")
    id_expr = _column_expr(columns, "id", "rowid")
    rows = _fetch_dicts(
        conn,
        f"""SELECT content_id, platform, {attempted_expr} AS attempted_at,
                  {error_expr} AS error, {category_expr} AS error_category,
                  {id_expr} AS id
           FROM publication_attempts
           WHERE success = 0
           ORDER BY content_id ASC, platform ASC,
                    datetime({attempted_expr}) ASC, {id_expr} ASC""",
    )
    for row in rows:
        group = _group(groups, row["content_id"], row["platform"])
        group["attempt_failures"] += 1
        group["sources"].add("publication_attempts")
        _maybe_latest(
            group,
            timestamp=row.get("attempted_at"),
            error=row.get("error"),
            category=row.get("error_category"),
            tie_breaker=_int(row.get("id"), default=0),
        )


def _merge_publications(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    groups: dict[tuple[int, str], dict[str, Any]],
    missing_columns: dict[str, tuple[str, ...]],
) -> None:
    columns = schema.get("content_publications")
    if columns is None:
        return
    required = ("content_id", "platform")
    missing = [column for column in required if column not in columns]
    optional = [
        column
        for column in (
            "status",
            "attempt_count",
            "last_error_at",
            "updated_at",
            "error",
            "error_category",
            "next_retry_at",
        )
        if column not in columns
    ]
    if missing:
        missing_columns["content_publications"] = tuple(sorted([*missing, *optional]))
        return
    if optional:
        missing_columns["content_publications"] = tuple(sorted(optional))
    status_expr = _column_expr(columns, "status", "'failed'")
    attempt_expr = _column_expr(columns, "attempt_count", "0")
    last_error_expr = _column_expr(columns, "last_error_at", "NULL")
    updated_expr = _column_expr(columns, "updated_at", "NULL")
    error_expr = _column_expr(columns, "error", "NULL")
    category_expr = _column_expr(columns, "error_category", "NULL")
    retry_expr = _column_expr(columns, "next_retry_at", "NULL")
    id_expr = _column_expr(columns, "id", "rowid")
    rows = _fetch_dicts(
        conn,
        f"""SELECT content_id, platform, {status_expr} AS status,
                  {attempt_expr} AS attempt_count,
                  COALESCE({last_error_expr}, {updated_expr}) AS failure_at,
                  {error_expr} AS error, {category_expr} AS error_category,
                  {retry_expr} AS next_retry_at, {id_expr} AS id
           FROM content_publications
           WHERE LOWER({status_expr}) IN ('failed', 'held')
           ORDER BY content_id ASC, platform ASC, {id_expr} ASC""",
    )
    for row in rows:
        group = _group(groups, row["content_id"], row["platform"])
        group["publication_status"] = _clean(row.get("status"))
        group["publication_attempt_count"] = max(
            group["publication_attempt_count"],
            _int(row.get("attempt_count"), default=0),
        )
        group["sources"].add("content_publications")
        group["next_retry_at"] = _min_futureish(
            group.get("next_retry_at"),
            row.get("next_retry_at"),
        )
        _maybe_latest(
            group,
            timestamp=row.get("failure_at"),
            error=row.get("error"),
            category=row.get("error_category"),
            tie_breaker=_int(row.get("id"), default=0),
        )


def _merge_queue(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    groups: dict[tuple[int, str], dict[str, Any]],
    missing_columns: dict[str, tuple[str, ...]],
) -> None:
    columns = schema.get("publish_queue")
    if columns is None:
        return
    required = ("content_id",)
    missing = [column for column in required if column not in columns]
    optional = [
        column
        for column in ("platform", "status", "scheduled_at", "created_at", "error", "error_category", "hold_reason")
        if column not in columns
    ]
    if missing:
        missing_columns["publish_queue"] = tuple(sorted([*missing, *optional]))
        return
    if optional:
        missing_columns["publish_queue"] = tuple(sorted(optional))
    platform_expr = _column_expr(columns, "platform", "'all'")
    status_expr = _column_expr(columns, "status", "'failed'")
    scheduled_expr = _column_expr(columns, "scheduled_at", "NULL")
    created_expr = _column_expr(columns, "created_at", "NULL")
    error_expr = _column_expr(columns, "error", "NULL")
    category_expr = _column_expr(columns, "error_category", "NULL")
    hold_expr = _column_expr(columns, "hold_reason", "NULL")
    id_expr = _column_expr(columns, "id", "rowid")
    rows = _fetch_dicts(
        conn,
        f"""SELECT content_id, {platform_expr} AS platform,
                  {status_expr} AS status,
                  COALESCE({scheduled_expr}, {created_expr}) AS failure_at,
                  {error_expr} AS error, {category_expr} AS error_category,
                  {hold_expr} AS hold_reason, {id_expr} AS id
           FROM publish_queue
           WHERE LOWER({status_expr}) IN ('failed', 'held')
           ORDER BY content_id ASC, platform ASC, {id_expr} ASC""",
    )
    for row in rows:
        platforms = ("x", "bluesky") if _platform(row.get("platform")) == "all" else (_platform(row.get("platform")),)
        for platform in platforms:
            group = _group(groups, row["content_id"], platform)
            group["queue_status"] = _clean(row.get("status"))
            group["queue_failures"] += 1
            group["sources"].add("publish_queue")
            error = row.get("hold_reason") if _clean(row.get("status")) == "held" else row.get("error")
            _maybe_latest(
                group,
                timestamp=row.get("failure_at"),
                error=error,
                category=row.get("error_category"),
                tie_breaker=_int(row.get("id"), default=0),
            )


def _merge_generated_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    groups: dict[tuple[int, str], dict[str, Any]],
    missing_columns: dict[str, tuple[str, ...]],
) -> None:
    if not groups:
        return
    columns = schema.get("generated_content")
    if columns is None:
        return
    if "id" not in columns:
        missing_columns["generated_content"] = ("id",)
        return
    content_expr = _column_expr(columns, "content", "NULL")
    ids = sorted({key[0] for key in groups})
    placeholders = ",".join("?" for _ in ids)
    rows = _fetch_dicts(
        conn,
        f"""SELECT id, {content_expr} AS content
            FROM generated_content
            WHERE id IN ({placeholders})""",
        ids,
    )
    for row in rows:
        excerpt = _excerpt(row.get("content"))
        for key, group in groups.items():
            if key[0] == row["id"]:
                group["content_excerpt"] = excerpt


def _build_row(
    group: dict[str, Any],
    *,
    now: datetime,
    min_failures: int,
    older_than_hours: float,
) -> PublicationRetryDeadLetterRow:
    failure_count = max(
        group["attempt_failures"],
        group["publication_attempt_count"],
        group["queue_failures"],
    )
    latest_at = _parse_timestamp(group.get("latest_attempt_at"))
    age_hours = (
        (now - latest_at).total_seconds() / 3600 if latest_at is not None else None
    )
    is_old = age_hours is None or age_hours >= older_than_hours
    candidate = failure_count >= min_failures and is_old
    category = _category(group.get("latest_error_category"), group.get("latest_error"))
    return PublicationRetryDeadLetterRow(
        content_id=group["content_id"],
        platform=group["platform"],
        failure_count=failure_count,
        latest_attempt_at=latest_at.isoformat() if latest_at is not None else None,
        latest_error=_clean(group.get("latest_error")),
        latest_error_category=category,
        next_retry_at=_timestamp_iso(group.get("next_retry_at")),
        queue_status=group.get("queue_status"),
        publication_status=group.get("publication_status"),
        content_excerpt=group.get("content_excerpt"),
        dead_letter_candidate=candidate,
        recommended_action=_recommended_action(
            candidate=candidate,
            category=category,
            queue_status=group.get("queue_status"),
            next_retry_at=_parse_timestamp(group.get("next_retry_at")),
            now=now,
        ),
        sources=tuple(sorted(group["sources"])),
    )


def _include_group(group: dict[str, Any]) -> bool:
    return (
        group["attempt_failures"] > 0
        or group["publication_status"] in _TARGET_STATUSES
        or group["queue_status"] in _TARGET_STATUSES
    )


def _recommended_action(
    *,
    candidate: bool,
    category: str,
    queue_status: str | None,
    next_retry_at: datetime | None,
    now: datetime,
) -> str:
    if queue_status == "held":
        return "review_hold"
    if candidate and category in _MANUAL_REVIEW_CATEGORIES:
        return "manual_fix_before_retry"
    if candidate:
        return "manual_replay_or_cancel"
    if next_retry_at is not None and next_retry_at > now:
        return "wait_for_retry"
    return "continue_retry"


def _maybe_latest(
    group: dict[str, Any],
    *,
    timestamp: Any,
    error: Any,
    category: Any,
    tie_breaker: int,
) -> None:
    current_time = _parse_timestamp(group.get("latest_attempt_at"))
    next_time = _parse_timestamp(timestamp)
    current_key = (current_time or datetime.min.replace(tzinfo=timezone.utc), group["latest_tie"])
    next_key = (next_time or datetime.min.replace(tzinfo=timezone.utc), tie_breaker)
    if next_key >= current_key:
        group["latest_attempt_at"] = next_time.isoformat() if next_time else _clean(timestamp)
        group["latest_error"] = _clean(error)
        group["latest_error_category"] = _clean(category)
        group["latest_tie"] = tie_breaker


def _group(
    groups: dict[tuple[int, str], dict[str, Any]],
    content_id: Any,
    platform: Any,
) -> dict[str, Any]:
    key = (_int(content_id, default=0), _platform(platform))
    if key not in groups:
        groups[key] = {
            "content_id": key[0],
            "platform": key[1],
            "attempt_failures": 0,
            "publication_attempt_count": 0,
            "queue_failures": 0,
            "latest_attempt_at": None,
            "latest_error": None,
            "latest_error_category": None,
            "latest_tie": 0,
            "next_retry_at": None,
            "queue_status": None,
            "publication_status": None,
            "content_excerpt": None,
            "sources": set(),
        }
    return groups[key]


def _sort_key(row: PublicationRetryDeadLetterRow) -> tuple[Any, ...]:
    latest = _parse_timestamp(row.latest_attempt_at)
    return (
        not row.dead_letter_candidate,
        row.platform,
        -(row.failure_count),
        latest or datetime.min.replace(tzinfo=timezone.utc),
        row.content_id,
    )


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


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _fetch_dicts(
    conn: sqlite3.Connection,
    sql: str,
    params: list[Any] | tuple[Any, ...] = (),
) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _column_expr(columns: set[str], name: str, fallback: str) -> str:
    return name if name in columns else fallback


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _aware(parsed)


def _timestamp_iso(value: Any) -> str | None:
    parsed = _parse_timestamp(value)
    return parsed.isoformat() if parsed is not None else _clean(value)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _category(category: Any, error: Any) -> str:
    normalized = normalize_error_category(category)
    if normalized != "unknown":
        return normalized
    return classify_publish_error(error)


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _platform(value: Any) -> str:
    return (_clean(value) or "unknown").lower()


def _excerpt(value: Any, limit: int = 120) -> str | None:
    text = " ".join(str(value or "").split())
    if not text:
        return None
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


def _min_futureish(left: Any, right: Any) -> str | None:
    left_time = _parse_timestamp(left)
    right_time = _parse_timestamp(right)
    if left_time is None:
        return right_time.isoformat() if right_time else _clean(right)
    if right_time is None:
        return left_time.isoformat()
    return min(left_time, right_time).isoformat()


def _int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _format_cell(value: Any, width: int) -> str:
    text = "" if value is None else str(value)
    return text if len(text) <= width else text[: max(0, width - 3)] + "..."
