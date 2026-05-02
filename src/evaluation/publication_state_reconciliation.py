"""Reconcile durable publication state across publication tables."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Iterable, Mapping, Sequence


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_DAYS = DEFAULT_LOOKBACK_DAYS
DEFAULT_LIMIT = 50
ISSUE_CODES = (
    "legacy_published_without_platform_record",
    "platform_published_without_legacy_timestamp",
    "queued_after_successful_attempt",
    "failed_publication_with_success_attempt",
    "duplicate_success_attempts_for_platform",
)
ACTIVE_QUEUE_STATUSES = {"queued", "failed", "held"}
SUCCESS_STATUS = "published"
FAILED_STATUS = "failed"


@dataclass(frozen=True)
class PublicationStateReconciliationIssue:
    """One publication state drift finding."""

    issue_code: str
    content_id: int
    platform: str
    message: str
    generated_content_id: int | None = None
    content_publication_id: int | None = None
    publish_queue_id: int | None = None
    publication_attempt_ids: tuple[int, ...] = ()
    details: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["publication_attempt_ids"] = list(self.publication_attempt_ids)
        result["details"] = dict(self.details or {})
        return result


@dataclass(frozen=True)
class PublicationStateReconciliationReport:
    """Read-only reconciliation result for platform publication state."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    issues: tuple[PublicationStateReconciliationIssue, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    @property
    def has_issues(self) -> bool:
        return bool(self.issues)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "publication_state_reconciliation",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "has_issues": self.has_issues,
            "issue_count": len(self.issues),
            "issues": [issue.to_dict() for issue in self.issues],
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(self.totals),
        }


def build_publication_state_reconciliation_report(
    db_or_conn: Any,
    *,
    generated_at: datetime | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    limit: int = DEFAULT_LIMIT,
    days: int | None = None,
    now: datetime | None = None,
    platforms: Sequence[str] | None = None,
) -> PublicationStateReconciliationReport:
    """Compare generated_content, content_publications, publish_queue, and attempts.

    ``days``, ``now``, and ``platforms`` are accepted for compatibility with the
    first version of this report; new callers should use ``lookback_days`` and
    ``generated_at``.
    """
    if days is not None:
        lookback_days = days
    if now is not None and generated_at is None:
        generated_at = now
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    conn = _connection(db_or_conn)
    generated_at = _ensure_utc(generated_at or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    selected_platforms = _normalise(platforms or ())
    filters = {
        "lookback_days": lookback_days,
        "lookback_start": cutoff.isoformat(),
        "lookback_end": generated_at.isoformat(),
        "limit": limit,
        "platform": list(selected_platforms),
    }

    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    generated_rows = _load_generated_content(conn, schema)
    publication_rows = _load_content_publications(
        conn,
        schema,
        cutoff=cutoff,
        platforms=selected_platforms,
    )
    queue_rows = _load_publish_queue(
        conn,
        schema,
        cutoff=cutoff,
        platforms=selected_platforms,
    )
    attempt_rows = _load_publication_attempts(
        conn,
        schema,
        cutoff=cutoff,
        platforms=selected_platforms,
    )
    all_issues = _find_issues(
        generated_rows,
        publication_rows,
        queue_rows,
        attempt_rows,
        platforms=selected_platforms,
    )
    issues = all_issues[:limit]

    return PublicationStateReconciliationReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(generated_rows, publication_rows, queue_rows, attempt_rows, issues, len(all_issues)),
        issues=tuple(issues),
        missing_tables=(),
        missing_columns={},
    )


def format_publication_state_reconciliation_json(
    report: PublicationStateReconciliationReport,
) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publication_state_reconciliation_text(
    report: PublicationStateReconciliationReport,
) -> str:
    """Render a deterministic terminal report."""
    totals = report.totals
    lines = [
        "Publication State Reconciliation",
        f"Generated: {report.generated_at}",
        (
            f"Lookback: {report.filters['lookback_days']} days "
            f"({report.filters['lookback_start']} to {report.filters['lookback_end']})"
        ),
        f"Limit: {report.filters['limit']}",
        (
            "Totals: "
            f"generated={totals['generated_content_count']} "
            f"publications={totals['content_publication_count']} "
            f"queue={totals['publish_queue_count']} "
            f"attempts={totals['publication_attempt_count']} "
            f"issues={totals['issue_count']}"
        ),
    ]
    if report.filters.get("platform"):
        lines.append("Platforms: " + ", ".join(report.filters["platform"]))
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        ]
        lines.append(f"Missing columns: {'; '.join(missing)}")
    lines.append("")

    if not report.issues:
        lines.append("No publication state drift found.")
        return "\n".join(lines)

    lines.append("Issues:")
    for issue in report.issues:
        identifiers = [f"content={issue.content_id}", f"platform={issue.platform}"]
        if issue.content_publication_id is not None:
            identifiers.append(f"publication={issue.content_publication_id}")
        if issue.publish_queue_id is not None:
            identifiers.append(f"queue={issue.publish_queue_id}")
        if issue.publication_attempt_ids:
            identifiers.append(
                "attempts=" + ",".join(str(item) for item in issue.publication_attempt_ids)
            )
        lines.append(
            "  - "
            + " ".join(identifiers)
            + f" code={issue.issue_code} message={issue.message}"
        )
    return "\n".join(lines)


def _find_issues(
    generated_rows: list[dict[str, Any]],
    publication_rows: list[dict[str, Any]],
    queue_rows: list[dict[str, Any]],
    attempt_rows: list[dict[str, Any]],
    *,
    platforms: tuple[str, ...],
) -> list[PublicationStateReconciliationIssue]:
    generated_by_id = {int(row["id"]): row for row in generated_rows}
    publications_by_key: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    attempts_by_key: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    queues_by_key: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)

    for row in publication_rows:
        publications_by_key[(int(row["content_id"]), _platform(row["platform"]))].append(row)
    for row in attempt_rows:
        attempts_by_key[(int(row["content_id"]), _platform(row["platform"]))].append(row)
    for row in queue_rows:
        content_id = int(row["content_id"])
        queue_platform = _platform(row.get("platform"))
        if queue_platform == "all":
            related_platforms = {
                key_platform
                for key_content_id, key_platform in set(publications_by_key) | set(attempts_by_key)
                if key_content_id == content_id
            } or {"x", "bluesky"}
        else:
            related_platforms = {queue_platform}
        for platform in related_platforms:
            queues_by_key[(content_id, platform)].append(row)

    keys = set(publications_by_key) | set(attempts_by_key) | set(queues_by_key)
    keys.update((int(row["id"]), "x") for row in generated_rows if _truthy(row.get("published")))
    if platforms:
        keys = {key for key in keys if key[1] in platforms}

    issues: list[PublicationStateReconciliationIssue] = []
    for content_id, platform in sorted(keys):
        gc = generated_by_id.get(content_id)
        publications = sorted(
            publications_by_key.get((content_id, platform), []),
            key=lambda row: (str(row.get("updated_at") or ""), int(row.get("id") or 0)),
        )
        attempts = sorted(
            attempts_by_key.get((content_id, platform), []),
            key=lambda row: (str(row.get("attempted_at") or ""), int(row.get("id") or 0)),
        )
        queues = sorted(
            queues_by_key.get((content_id, platform), []),
            key=lambda row: (str(row.get("scheduled_at") or ""), int(row.get("id") or 0)),
        )
        successful_attempts = [row for row in attempts if _truthy(row.get("success"))]
        published_publications = [
            row for row in publications if _platform(row.get("status")) == SUCCESS_STATUS
        ]

        if platform == "x" and gc and _truthy(gc.get("published")):
            if not published_publications and not successful_attempts:
                issues.append(
                    _issue(
                        "legacy_published_without_platform_record",
                        content_id,
                        platform,
                        "generated_content is marked published but no platform publication or successful attempt exists",
                        generated_content_id=content_id,
                    )
                )

        for publication in publications:
            status = _platform(publication.get("status"))
            publication_id = _int(publication.get("id"))
            if (
                platform == "x"
                and status == SUCCESS_STATUS
                and gc
                and not _text(gc.get("published_at"))
            ):
                issues.append(
                    _issue(
                        "platform_published_without_legacy_timestamp",
                        content_id,
                        platform,
                        "content_publications is published but generated_content.published_at is missing",
                        generated_content_id=content_id,
                        content_publication_id=publication_id,
                    )
                )
            if status == FAILED_STATUS and successful_attempts:
                issues.append(
                    _issue(
                        "failed_publication_with_success_attempt",
                        content_id,
                        platform,
                        "content_publications is failed even though a successful publication attempt exists",
                        generated_content_id=content_id if gc else None,
                        content_publication_id=publication_id,
                        publication_attempt_ids=_ids(successful_attempts),
                    )
                )

        if len(successful_attempts) > 1:
            issues.append(
                _issue(
                    "duplicate_success_attempts_for_platform",
                    content_id,
                    platform,
                    "multiple successful publication attempts exist for the same content and platform",
                    generated_content_id=content_id if gc else None,
                    publication_attempt_ids=_ids(successful_attempts),
                    details={
                        "success_count": len(successful_attempts),
                        "platform_post_ids": sorted(
                            {
                                _text(row.get("platform_post_id"))
                                for row in successful_attempts
                                if _text(row.get("platform_post_id"))
                            }
                        ),
                    },
                )
            )

        latest_success_at = _latest_timestamp(successful_attempts, "attempted_at")
        if latest_success_at is not None:
            for queue in queues:
                status = _platform(queue.get("status"))
                queue_time = _parse_datetime(
                    queue.get("scheduled_at") or queue.get("created_at") or queue.get("published_at")
                )
                if status in ACTIVE_QUEUE_STATUSES and (
                    queue_time is None or queue_time >= latest_success_at
                ):
                    issues.append(
                        _issue(
                            "queued_after_successful_attempt",
                            content_id,
                            platform,
                            "publish_queue still has active work after a successful publication attempt",
                            generated_content_id=content_id if gc else None,
                            publish_queue_id=_int(queue.get("id")),
                            publication_attempt_ids=_ids(
                                [
                                    row
                                    for row in successful_attempts
                                    if _parse_datetime(row.get("attempted_at")) == latest_success_at
                                ]
                            ),
                            details={
                                "queue_status": status,
                                "queue_platform": _platform(queue.get("platform")),
                                "latest_success_attempted_at": latest_success_at.isoformat(),
                            },
                        )
                    )

    issues.sort(
        key=lambda issue: (
            issue.content_id,
            issue.platform,
            issue.issue_code,
            issue.content_publication_id or 0,
            issue.publish_queue_id or 0,
            issue.publication_attempt_ids,
        )
    )
    return issues


def _load_generated_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    rows = conn.execute(
        f"""SELECT
               gc.id AS id,
               {_column_expr(columns, "published", "0", alias="gc")} AS published,
               {_column_expr(columns, "published_at", "NULL", alias="gc")} AS published_at,
               {_column_expr(columns, "created_at", "NULL", alias="gc")} AS created_at
           FROM generated_content gc
           ORDER BY gc.id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _load_content_publications(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    platforms: tuple[str, ...],
) -> list[dict[str, Any]]:
    columns = schema["content_publications"]
    where, params = _platform_where("cp", platforms)
    timestamp_fields = [
        column
        for column in ("published_at", "updated_at", "last_error_at")
        if column in columns
    ]
    if timestamp_fields:
        where.append(_window_filter("cp", timestamp_fields))
        params.append(cutoff.isoformat())
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""SELECT
               cp.id AS id,
               cp.content_id AS content_id,
               cp.platform AS platform,
               cp.status AS status,
               {_column_expr(columns, "published_at", "NULL", alias="cp")} AS published_at,
               {_column_expr(columns, "updated_at", "NULL", alias="cp")} AS updated_at,
               {_column_expr(columns, "last_error_at", "NULL", alias="cp")} AS last_error_at
           FROM content_publications cp
           {where_clause}
           ORDER BY cp.content_id ASC, cp.platform ASC, cp.id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _load_publish_queue(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    platforms: tuple[str, ...],
) -> list[dict[str, Any]]:
    columns = schema["publish_queue"]
    where, params = (
        _platform_where("pq", platforms, include_all=True)
        if "platform" in columns
        else ([], [])
    )
    timestamp_fields = [
        column for column in ("scheduled_at", "published_at", "created_at") if column in columns
    ]
    if timestamp_fields:
        where.append(_window_filter("pq", timestamp_fields))
        params.append(cutoff.isoformat())
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""SELECT
               pq.id AS id,
               pq.content_id AS content_id,
               {_column_expr(columns, "platform", "'all'", alias="pq")} AS platform,
               pq.status AS status,
               {_column_expr(columns, "scheduled_at", "NULL", alias="pq")} AS scheduled_at,
               {_column_expr(columns, "published_at", "NULL", alias="pq")} AS published_at,
               {_column_expr(columns, "created_at", "NULL", alias="pq")} AS created_at
           FROM publish_queue pq
           {where_clause}
           ORDER BY pq.content_id ASC, platform ASC, pq.id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _load_publication_attempts(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    platforms: tuple[str, ...],
) -> list[dict[str, Any]]:
    columns = schema["publication_attempts"]
    where, params = _platform_where("pa", platforms)
    if "attempted_at" in columns:
        where.append(_window_filter("pa", ["attempted_at"]))
        params.append(cutoff.isoformat())
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""SELECT
               pa.id AS id,
               pa.content_id AS content_id,
               pa.platform AS platform,
               pa.attempted_at AS attempted_at,
               pa.success AS success,
               {_column_expr(columns, "platform_post_id", "NULL", alias="pa")} AS platform_post_id,
               {_column_expr(columns, "platform_url", "NULL", alias="pa")} AS platform_url
           FROM publication_attempts pa
           {where_clause}
           ORDER BY pa.content_id ASC, pa.platform ASC, pa.attempted_at ASC, pa.id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _totals(
    generated_rows: list[dict[str, Any]],
    publication_rows: list[dict[str, Any]],
    queue_rows: list[dict[str, Any]],
    attempt_rows: list[dict[str, Any]],
    issues: list[PublicationStateReconciliationIssue],
    total_issue_count: int,
) -> dict[str, Any]:
    by_code = Counter(issue.issue_code for issue in issues)
    return {
        "generated_content_count": len(generated_rows),
        "content_publication_count": len(publication_rows),
        "publish_queue_count": len(queue_rows),
        "publication_attempt_count": len(attempt_rows),
        "issue_count": len(issues),
        "total_issue_count": total_issue_count,
        "limited": total_issue_count > len(issues),
        "affected_content_count": len({issue.content_id for issue in issues}),
        "by_issue_code": {code: by_code.get(code, 0) for code in ISSUE_CODES},
        "by_platform": dict(sorted(Counter(issue.platform for issue in issues).items())),
    }


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "generated_content": {"id", "published", "published_at"},
        "content_publications": {"id", "content_id", "platform", "status"},
        "publish_queue": {"id", "content_id", "status"},
        "publication_attempts": {"id", "content_id", "platform", "attempted_at", "success"},
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and columns - schema[table]
    }
    return missing_tables, missing_columns


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> PublicationStateReconciliationReport:
    return PublicationStateReconciliationReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "generated_content_count": 0,
            "content_publication_count": 0,
            "publish_queue_count": 0,
            "publication_attempt_count": 0,
            "issue_count": 0,
            "total_issue_count": 0,
            "limited": False,
            "affected_content_count": 0,
            "by_issue_code": {code: 0 for code in ISSUE_CODES},
            "by_platform": {},
        },
        issues=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _issue(
    issue_code: str,
    content_id: int,
    platform: str,
    message: str,
    *,
    generated_content_id: int | None = None,
    content_publication_id: int | None = None,
    publish_queue_id: int | None = None,
    publication_attempt_ids: tuple[int, ...] = (),
    details: dict[str, Any] | None = None,
) -> PublicationStateReconciliationIssue:
    return PublicationStateReconciliationIssue(
        issue_code=issue_code,
        content_id=content_id,
        platform=platform,
        message=message,
        generated_content_id=generated_content_id,
        content_publication_id=content_publication_id,
        publish_queue_id=publish_queue_id,
        publication_attempt_ids=publication_attempt_ids,
        details=details or {},
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")}
        for table in tables
        if table
    }


def _platform_where(
    alias: str,
    platforms: tuple[str, ...],
    *,
    include_all: bool = False,
) -> tuple[list[str], list[Any]]:
    if not platforms:
        return [], []
    selected = list(platforms)
    if include_all:
        selected.append("all")
    placeholders = ",".join("?" for _ in selected)
    return [f"LOWER(COALESCE({alias}.platform, '')) IN ({placeholders})"], selected


def _window_filter(alias: str, fields: Sequence[str]) -> str:
    if len(fields) == 1:
        field = fields[0]
        return f"(datetime({alias}.{field}) >= datetime(?) OR {alias}.{field} IS NULL)"
    expressions = ", ".join(f"{alias}.{field}" for field in fields)
    null_checks = " AND ".join(f"{alias}.{field} IS NULL" for field in fields)
    return f"(datetime(COALESCE({expressions})) >= datetime(?) OR ({null_checks}))"


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str = "NULL",
    *,
    alias: str,
) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _latest_timestamp(
    rows: Sequence[Mapping[str, Any]],
    field: str,
) -> datetime | None:
    values = [
        parsed
        for row in rows
        if (parsed := _parse_datetime(row.get(field))) is not None
    ]
    return max(values) if values else None


def _ids(rows: Sequence[Mapping[str, Any]]) -> tuple[int, ...]:
    return tuple(
        parsed
        for row in rows
        if (parsed := _int(row.get("id"))) is not None
    )


def _normalise(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({_platform(value) for value in values if _platform(value)}))


def _platform(value: Any) -> str:
    return str(value or "").strip().casefold()


def _text(value: Any) -> str:
    return str(value or "").strip()


def _int(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _truthy(value: Any) -> bool:
    return _int(value) == 1


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
