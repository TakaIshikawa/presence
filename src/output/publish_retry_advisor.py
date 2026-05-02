"""Advise safe retries for failed or unpublished platform posts."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any

from .publish_error_signatures import normalize_publish_error_signature
from .publish_errors import classify_publish_error, normalize_error_category


DEFAULT_DAYS = 30
DEFAULT_MAX_RETRIES = 3
SUPPORTED_PLATFORMS = ("all", "x", "bluesky")
TRANSIENT_CATEGORIES = {"network", "rate_limit", "unknown"}
TRANSIENT_MARKERS = (
    "429",
    "too many requests",
    "rate limit",
    "ratelimit",
    "throttl",
    "timeout",
    "timed out",
    "connection",
    "network",
    "temporarily unavailable",
    "service unavailable",
    "bad gateway",
    "gateway timeout",
    "502",
    "503",
    "504",
)


@dataclass(frozen=True)
class PublishRetryRecord:
    """One failed or unpublished platform post with retry advice."""

    source: str
    source_id: int | None
    content_id: int
    platform: str
    status: str
    error_category: str
    error_signature: str
    error: str | None
    retry_count: int
    last_attempt_at: str | None
    last_attempt_age_hours: float | None
    next_retry_at: str | None
    recommended_action: str
    reason: str
    queue_id: int | None = None
    publication_id: int | None = None
    attempt_id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PublishRetryGroup:
    """Grouped retry advice for repeated failure signatures."""

    platform: str
    error_signature: str
    error_category: str
    recommended_action: str
    count: int
    retry_count_min: int
    retry_count_max: int
    oldest_last_attempt_at: str | None
    newest_last_attempt_at: str | None
    source_counts: dict[str, int]
    content_ids: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["content_ids"] = list(self.content_ids)
        payload["source_counts"] = dict(sorted(self.source_counts.items()))
        return payload


@dataclass(frozen=True)
class PublishRetryAdviceReport:
    """Read-only retry advice report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    groups: tuple[PublishRetryGroup, ...]
    records: tuple[PublishRetryRecord, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "publish_retry_advice",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "group_count": len(self.groups),
            "groups": [group.to_dict() for group in self.groups],
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "record_count": len(self.records),
            "records": [record.to_dict() for record in self.records],
            "totals": dict(sorted(self.totals.items())),
        }


def build_publish_retry_advice_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str = "all",
    max_retries: int = DEFAULT_MAX_RETRIES,
    now: datetime | None = None,
) -> PublishRetryAdviceReport:
    """Build a read-only report of retry recommendations before publish retry jobs."""
    if days <= 0:
        raise ValueError("days must be positive")
    if max_retries <= 0:
        raise ValueError("max_retries must be positive")
    if platform not in SUPPORTED_PLATFORMS:
        raise ValueError(f"platform must be one of: {', '.join(SUPPORTED_PLATFORMS)}")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    missing_tables, missing_columns = _schema_gaps(schema)
    filters = {"days": days, "max_retries": max_retries, "platform": platform}
    if missing_tables:
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    raw_rows = _fetch_advice_rows(conn, schema, cutoff=cutoff, platform=platform)
    records = tuple(
        sorted(
            (
                _record_from_row(row, now=generated_at, max_retries=max_retries)
                for row in raw_rows
            ),
            key=_record_sort_key,
        )
    )
    groups = tuple(_build_groups(records))
    by_action: dict[str, int] = {}
    by_platform: dict[str, int] = {}
    by_category: dict[str, int] = {}
    for record in records:
        by_action[record.recommended_action] = by_action.get(record.recommended_action, 0) + 1
        by_platform[record.platform] = by_platform.get(record.platform, 0) + 1
        by_category[record.error_category] = by_category.get(record.error_category, 0) + 1

    return PublishRetryAdviceReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "records": len(records),
            "groups": len(groups),
            "by_action": dict(sorted(by_action.items())),
            "by_category": dict(sorted(by_category.items())),
            "by_platform": dict(sorted(by_platform.items())),
        },
        groups=groups,
        records=records,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_publish_retry_advice_json(report: PublishRetryAdviceReport) -> str:
    """Serialize publish retry advice as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publish_retry_advice_text(report: PublishRetryAdviceReport) -> str:
    """Render publish retry advice for operators."""
    lines = [
        "Publish Retry Advice",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['days']} days; "
            f"platform={report.filters['platform']}; "
            f"max_retries={report.filters['max_retries']}"
        ),
        f"Totals: {report.totals['records']} records, {report.totals['groups']} groups",
    ]
    if report.missing_tables:
        lines.append(f"Missing required schema: {', '.join(report.missing_tables)}")
    missing = [
        f"{table}({', '.join(columns)})"
        for table, columns in report.missing_columns.items()
        if columns
    ]
    if missing:
        lines.append(f"Missing optional columns: {'; '.join(missing)}")
    lines.append("")

    if not report.records:
        lines.append("No failed or unpublished platform posts need retry advice.")
        return "\n".join(lines)

    lines.append("Groups:")
    for group in report.groups:
        lines.append(
            "  - {platform} / {category} / {action}: count={count} "
            "retries={retry_min}-{retry_max} newest={newest}".format(
                platform=group.platform,
                category=group.error_category,
                action=group.recommended_action,
                count=group.count,
                retry_min=group.retry_count_min,
                retry_max=group.retry_count_max,
                newest=group.newest_last_attempt_at or "-",
            )
        )
        lines.append(f"    signature: {group.error_signature}")
        lines.append(
            "    content_ids: "
            + (", ".join(str(content_id) for content_id in group.content_ids) or "-")
        )

    lines.append("")
    lines.append("Records:")
    for record in report.records:
        source_ref = record.source
        if record.source_id is not None:
            source_ref += f"#{record.source_id}"
        lines.append(
            "  - content_id={content_id} platform={platform} source={source} "
            "status={status} retries={retries} action={action}".format(
                content_id=record.content_id,
                platform=record.platform,
                source=source_ref,
                status=record.status,
                retries=record.retry_count,
                action=record.recommended_action,
            )
        )
        lines.append(f"    reason: {record.reason}")
    return "\n".join(lines)


def _fetch_advice_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    platform: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rows.extend(_publication_rows(conn, schema, cutoff=cutoff, platform=platform))
    rows.extend(_queue_rows(conn, schema, cutoff=cutoff, platform=platform))
    rows.extend(_generated_content_rows(conn, schema, cutoff=cutoff, platform=platform))

    by_key: dict[tuple[str, int | None, int, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            str(row.get("source")),
            _optional_int(row.get("source_id")),
            int(row["content_id"]),
            str(row["platform"]),
        )
        current = by_key.get(key)
        if current is None or _timestamp_sort(row.get("last_attempt_at")) > _timestamp_sort(
            current.get("last_attempt_at")
        ):
            by_key[key] = row
    return list(by_key.values())


def _publication_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    platform: str,
) -> list[dict[str, Any]]:
    columns = schema.get("content_publications")
    gc_columns = schema.get("generated_content", set())
    if not columns or not {"id", "content_id", "platform", "status"}.issubset(columns):
        return []
    gc_join = (
        "LEFT JOIN generated_content gc ON gc.id = cp.content_id"
        if "generated_content" in schema
        else ""
    )
    timestamp_expr = _coalesce_expr(
        "cp",
        columns,
        ("last_error_at", "updated_at", "next_retry_at", "published_at"),
    )
    where = ["LOWER(COALESCE(cp.status, '')) != 'published'"]
    params: list[Any] = []
    if "error" in columns:
        where.append("(LOWER(COALESCE(cp.status, '')) = 'failed' OR cp.error IS NOT NULL)")
    else:
        where.append("LOWER(COALESCE(cp.status, '')) = 'failed'")
    if platform != "all":
        where.append("LOWER(cp.platform) = ?")
        params.append(platform)
    if timestamp_expr != "NULL":
        where.append(f"{timestamp_expr} >= ?")
        params.append(cutoff.isoformat())

    published_filter = _published_filter("gc", gc_columns, "cp.platform")
    if published_filter:
        where.append(published_filter)

    rows = conn.execute(
        f"""SELECT
                  'content_publications' AS source,
                  cp.id AS source_id,
                  NULL AS queue_id,
                  cp.id AS publication_id,
                  NULL AS attempt_id,
                  cp.content_id AS content_id,
                  cp.platform AS platform,
                  cp.status AS status,
                  {_column_expr("cp", columns, "error")} AS error,
                  {_column_expr("cp", columns, "error_category")} AS error_category,
                  {_column_expr("cp", columns, "attempt_count", "0")} AS retry_count,
                  {_column_expr("cp", columns, "next_retry_at")} AS next_retry_at,
                  {timestamp_expr} AS last_attempt_at,
                  {_column_expr("gc", gc_columns, "retry_count", "0")} AS content_retry_count
           FROM content_publications cp
           {gc_join}
           WHERE {' AND '.join(where)}
           ORDER BY cp.platform ASC, last_attempt_at ASC, cp.id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _queue_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    platform: str,
) -> list[dict[str, Any]]:
    columns = schema.get("publish_queue")
    gc_columns = schema.get("generated_content", set())
    if not columns or not {"id", "content_id", "status"}.issubset(columns):
        return []
    gc_join = (
        "LEFT JOIN generated_content gc ON gc.id = pq.content_id"
        if "generated_content" in schema
        else ""
    )
    timestamp_expr = _coalesce_expr("pq", columns, ("created_at", "scheduled_at", "published_at"))
    where = ["LOWER(COALESCE(pq.status, '')) IN ('failed', 'held')"]
    params: list[Any] = []
    if platform != "all" and "platform" in columns:
        where.append("(LOWER(pq.platform) = ? OR LOWER(pq.platform) = 'all')")
        params.append(platform)
    if timestamp_expr != "NULL":
        where.append(f"{timestamp_expr} >= ?")
        params.append(cutoff.isoformat())
    platform_expr = _platform_expr("pq", columns)
    published_filter = _published_filter("gc", gc_columns, platform_expr)
    if published_filter:
        where.append(published_filter)

    rows = conn.execute(
        f"""SELECT
                  'publish_queue' AS source,
                  pq.id AS source_id,
                  pq.id AS queue_id,
                  NULL AS publication_id,
                  NULL AS attempt_id,
                  pq.content_id AS content_id,
                  {platform_expr} AS platform,
                  pq.status AS status,
                  {_column_expr("pq", columns, "error")} AS error,
                  {_column_expr("pq", columns, "error_category")} AS error_category,
                  {_column_expr("gc", gc_columns, "retry_count", "0")} AS retry_count,
                  NULL AS next_retry_at,
                  {timestamp_expr} AS last_attempt_at,
                  {_column_expr("gc", gc_columns, "retry_count", "0")} AS content_retry_count
           FROM publish_queue pq
           {gc_join}
           WHERE {' AND '.join(where)}
           ORDER BY platform ASC, last_attempt_at ASC, pq.id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _generated_content_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    platform: str,
) -> list[dict[str, Any]]:
    columns = schema.get("generated_content")
    if not columns or not {"id", "published"}.issubset(columns):
        return []
    if "retry_count" not in columns and "last_retry_at" not in columns:
        return []
    timestamp_expr = _coalesce_expr("gc", columns, ("last_retry_at", "created_at"))
    where = [
        "COALESCE(gc.published, 0) = 0",
        "COALESCE(gc.retry_count, 0) > 0" if "retry_count" in columns else "1 = 0",
    ]
    params: list[Any] = []
    platform_expr = _content_platform_expr("gc", columns)
    if platform != "all":
        where.append(f"{platform_expr} = ?")
        params.append(platform)
    if timestamp_expr != "NULL":
        where.append(f"{timestamp_expr} >= ?")
        params.append(cutoff.isoformat())

    rows = conn.execute(
        f"""SELECT
                  'generated_content' AS source,
                  gc.id AS source_id,
                  NULL AS queue_id,
                  NULL AS publication_id,
                  NULL AS attempt_id,
                  gc.id AS content_id,
                  {platform_expr} AS platform,
                  'unpublished' AS status,
                  NULL AS error,
                  NULL AS error_category,
                  {_column_expr("gc", columns, "retry_count", "0")} AS retry_count,
                  NULL AS next_retry_at,
                  {timestamp_expr} AS last_attempt_at,
                  {_column_expr("gc", columns, "retry_count", "0")} AS content_retry_count
           FROM generated_content gc
           WHERE {' AND '.join(where)}
           ORDER BY platform ASC, last_attempt_at ASC, gc.id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _record_from_row(
    row: dict[str, Any],
    *,
    now: datetime,
    max_retries: int,
) -> PublishRetryRecord:
    platform = _normalize_platform(row.get("platform"))
    error = _clean(row.get("error"))
    category = normalize_error_category(row.get("error_category"))
    if category == "unknown" and error:
        category = classify_publish_error(error, platform=platform)
    signature = normalize_publish_error_signature(error or category or row.get("status"))
    retry_count = max(_int(row.get("retry_count")), _int(row.get("content_retry_count")))
    last_attempt_at = _clean(row.get("last_attempt_at"))
    last_attempt_dt = _parse_timestamp(last_attempt_at)
    next_retry_at = _clean(row.get("next_retry_at"))
    age_hours = _age_hours(last_attempt_dt, now)
    action, reason = recommend_publish_retry_action(
        error_category=category,
        error=error,
        retry_count=retry_count,
        last_attempt_at=last_attempt_dt,
        next_retry_at=_parse_timestamp(next_retry_at),
        now=now,
        max_retries=max_retries,
    )
    return PublishRetryRecord(
        source=str(row["source"]),
        source_id=_optional_int(row.get("source_id")),
        content_id=int(row["content_id"]),
        platform=platform,
        status=str(row.get("status") or "unknown"),
        error_category=category,
        error_signature=signature,
        error=error,
        retry_count=retry_count,
        last_attempt_at=last_attempt_at,
        last_attempt_age_hours=age_hours,
        next_retry_at=next_retry_at,
        recommended_action=action,
        reason=reason,
        queue_id=_optional_int(row.get("queue_id")),
        publication_id=_optional_int(row.get("publication_id")),
        attempt_id=_optional_int(row.get("attempt_id")),
    )


def recommend_publish_retry_action(
    *,
    error_category: str,
    error: str | None,
    retry_count: int,
    last_attempt_at: datetime | None,
    next_retry_at: datetime | None,
    now: datetime | None = None,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> tuple[str, str]:
    """Return an advisory action and reason for one retry candidate."""
    now = _as_utc(now or datetime.now(timezone.utc))
    category = normalize_error_category(error_category)
    if category == "auth":
        return "review_credentials", "Credential or authorization failure; refresh platform credentials before retrying."
    if retry_count >= max_retries:
        return "manual_review", f"Retry count {retry_count} has reached the configured limit {max_retries}."
    if category in {"duplicate", "media", "validation"}:
        return "manual_review", f"{category} failures usually need content or asset changes before retrying."
    if next_retry_at is not None and next_retry_at > now:
        return "wait", f"Backoff is active until {next_retry_at.isoformat()}."

    transient = category in TRANSIENT_CATEGORIES or _has_transient_marker(error)
    age_minutes = _age_minutes(last_attempt_at, now)
    wait_minutes = _recommended_wait_minutes(category, retry_count)
    if transient and age_minutes is not None and age_minutes < wait_minutes:
        return "wait", f"Last attempt was {round(age_minutes, 1)} minutes ago; wait at least {wait_minutes} minutes."
    if transient:
        return "retry_now", "Known transient failure pattern and retry backoff has elapsed."
    if retry_count == 0:
        return "retry_now", "No recorded retry attempts; safe to try once before manual triage."
    return "manual_review", "Unknown repeated failure; inspect publisher logs before another retry."


def _build_groups(records: tuple[PublishRetryRecord, ...]) -> list[PublishRetryGroup]:
    grouped: dict[tuple[str, str, str, str], list[PublishRetryRecord]] = {}
    for record in records:
        key = (
            record.platform,
            record.error_signature,
            record.error_category,
            record.recommended_action,
        )
        grouped.setdefault(key, []).append(record)

    groups: list[PublishRetryGroup] = []
    for (platform, signature, category, action), items in grouped.items():
        timestamps = sorted(item.last_attempt_at for item in items if item.last_attempt_at)
        source_counts: dict[str, int] = {}
        for item in items:
            source_counts[item.source] = source_counts.get(item.source, 0) + 1
        groups.append(
            PublishRetryGroup(
                platform=platform,
                error_signature=signature,
                error_category=category,
                recommended_action=action,
                count=len(items),
                retry_count_min=min(item.retry_count for item in items),
                retry_count_max=max(item.retry_count for item in items),
                oldest_last_attempt_at=timestamps[0] if timestamps else None,
                newest_last_attempt_at=timestamps[-1] if timestamps else None,
                source_counts=source_counts,
                content_ids=tuple(sorted({item.content_id for item in items})),
            )
        )
    groups.sort(
        key=lambda group: (
            _action_rank(group.recommended_action),
            -group.count,
            group.platform,
            group.error_category,
            group.error_signature,
        )
    )
    return groups


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
    ).fetchall()
    return {
        row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    expected = {
        "generated_content": ("id", "published", "retry_count"),
        "publish_queue": ("id", "content_id", "status", "platform"),
        "content_publications": ("id", "content_id", "platform", "status"),
    }
    missing_columns = {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema
    }
    if not any(table in schema for table in expected):
        return ("generated_content", "publish_queue", "content_publications"), missing_columns
    blocking: tuple[str, ...] = ()
    return blocking, missing_columns


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> PublishRetryAdviceReport:
    return PublishRetryAdviceReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "records": 0,
            "groups": 0,
            "by_action": {},
            "by_category": {},
            "by_platform": {},
        },
        groups=(),
        records=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _platform_expr(alias: str, columns: set[str]) -> str:
    if "platform" not in columns:
        return "'unknown'"
    return (
        f"CASE LOWER(COALESCE({alias}.platform, '')) "
        "WHEN 'twitter' THEN 'x' "
        "WHEN 'bsky' THEN 'bluesky' "
        "WHEN 'all' THEN 'all' "
        f"ELSE LOWER(COALESCE({alias}.platform, 'unknown')) END"
    )


def _content_platform_expr(alias: str, columns: set[str]) -> str:
    if "content_type" not in columns:
        return "'x'"
    return (
        f"CASE LOWER(COALESCE({alias}.content_type, '')) "
        "WHEN 'bluesky_post' THEN 'bluesky' "
        "WHEN 'bsky_post' THEN 'bluesky' "
        "ELSE 'x' END"
    )


def _published_filter(alias: str, columns: set[str], platform_expr: str) -> str | None:
    clauses: list[str] = []
    if "published" in columns:
        clauses.append(f"NOT ({platform_expr} = 'x' AND COALESCE({alias}.published, 0) = 1)")
    if "bluesky_uri" in columns:
        clauses.append(f"NOT ({platform_expr} = 'bluesky' AND {alias}.bluesky_uri IS NOT NULL)")
    return " AND ".join(clauses) if clauses else None


def _column_expr(
    alias: str,
    columns: set[str],
    column: str,
    fallback: str = "NULL",
) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _coalesce_expr(alias: str, columns: set[str], candidates: tuple[str, ...]) -> str:
    parts = [_column_expr(alias, columns, column) for column in candidates if column in columns]
    if not parts:
        return "NULL"
    if len(parts) == 1:
        return parts[0]
    return f"COALESCE({', '.join(parts)})"


def _normalize_platform(value: Any) -> str:
    text = str(value or "unknown").strip().lower()
    if text in {"twitter", "x_post"}:
        return "x"
    if text in {"bsky", "bluesky_post"}:
        return "bluesky"
    return text or "unknown"


def _recommended_wait_minutes(category: str, retry_count: int) -> int:
    category = normalize_error_category(category)
    if category == "rate_limit":
        base = 60
    elif category == "network":
        base = 5
    else:
        base = 15
    return min(480, base * (2 ** max(0, retry_count - 1)))


def _has_transient_marker(error: str | None) -> bool:
    text = str(error or "").lower()
    return any(marker in text for marker in TRANSIENT_MARKERS)


def _parse_timestamp(value: Any) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _age_minutes(value: datetime | None, now: datetime) -> float | None:
    if value is None:
        return None
    return max(0.0, (_as_utc(now) - _as_utc(value)).total_seconds() / 60)


def _age_hours(value: datetime | None, now: datetime) -> float | None:
    minutes = _age_minutes(value, now)
    return round(minutes / 60, 2) if minutes is not None else None


def _timestamp_sort(value: Any) -> str:
    return _clean(value) or ""


def _record_sort_key(record: PublishRetryRecord) -> tuple[int, str, str, str, int]:
    return (
        _action_rank(record.recommended_action),
        record.platform,
        record.error_category,
        record.last_attempt_at or "",
        record.content_id,
    )


def _action_rank(action: str) -> int:
    ranks = {
        "review_credentials": 0,
        "retry_now": 1,
        "wait": 2,
        "manual_review": 3,
    }
    return ranks.get(action, 99)


def _clean(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
