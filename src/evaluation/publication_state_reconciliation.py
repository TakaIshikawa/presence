"""Reconcile durable publication state across publication tables."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Iterable, Mapping, Sequence


DEFAULT_DAYS = 30
ISSUE_CODES = (
    "generated_published_without_platform_success",
    "publication_published_generated_unpublished",
    "publication_failed_after_success",
    "published_row_missing_identifier",
    "x_tweet_id_mismatch",
    "x_published_url_mismatch",
    "duplicate_successful_attempt_post_ids",
)


@dataclass(frozen=True)
class PublicationStateReconciliationIssue:
    """One publication state drift finding."""

    issue_code: str
    content_id: int
    platform: str
    message: str
    generated_content_id: int | None = None
    content_publication_id: int | None = None
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
    days: int = DEFAULT_DAYS,
    platforms: Sequence[str] | None = None,
    now: datetime | None = None,
) -> PublicationStateReconciliationReport:
    """Compare generated_content, content_publications, and publication_attempts."""
    if days <= 0:
        raise ValueError("days must be positive")

    conn = _connection(db_or_conn)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    selected_platforms = _normalise(platforms or ())
    filters = {
        "days": days,
        "cutoff": cutoff.isoformat(),
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

    generated_rows = _load_generated_content(conn, schema, cutoff=cutoff)
    publication_rows = _load_content_publications(
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
    issues = _find_issues(
        generated_rows,
        publication_rows,
        attempt_rows,
        platforms=selected_platforms,
    )

    return PublicationStateReconciliationReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(generated_rows, publication_rows, attempt_rows, issues),
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
            f"Window: {report.filters['days']} days "
            f"platforms={','.join(report.filters['platform']) if report.filters['platform'] else 'all'}"
        ),
        (
            "Totals: "
            f"generated={totals['generated_content_count']} "
            f"publications={totals['content_publication_count']} "
            f"attempts={totals['publication_attempt_count']} "
            f"issues={totals['issue_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in report.missing_columns.items()
        ]
        lines.append(f"Missing columns: {'; '.join(missing)}")
    lines.append("")

    if not report.issues:
        lines.append("No publication state drift found.")
        return "\n".join(lines)

    lines.append("Issues:")
    for issue in report.issues:
        lines.append(
            f"  - content={issue.content_id} platform={issue.platform} "
            f"code={issue.issue_code} message={issue.message}"
        )
    return "\n".join(lines)


def _find_issues(
    generated_rows: list[dict[str, Any]],
    publication_rows: list[dict[str, Any]],
    attempt_rows: list[dict[str, Any]],
    *,
    platforms: tuple[str, ...],
) -> list[PublicationStateReconciliationIssue]:
    generated_by_id = {int(row["id"]): row for row in generated_rows}
    publications_by_key: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    attempts_by_key: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in publication_rows:
        publications_by_key[(int(row["content_id"]), _platform(row["platform"]))].append(row)
    for row in attempt_rows:
        attempts_by_key[(int(row["content_id"]), _platform(row["platform"]))].append(row)

    keys = set(publications_by_key) | set(attempts_by_key)
    keys.update((int(row["id"]), "x") for row in generated_rows if _int(row.get("published")) == 1)
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
        successful_attempts = [row for row in attempts if _truthy(row.get("success"))]
        published_publications = [
            row for row in publications if _platform(row.get("status")) == "published"
        ]

        if platform == "x" and gc and _int(gc.get("published")) == 1:
            if not published_publications and not successful_attempts:
                issues.append(
                    _issue(
                        "generated_published_without_platform_success",
                        content_id,
                        platform,
                        "generated_content is marked published but X has no published publication row or successful attempt",
                        generated_content_id=content_id,
                    )
                )

        for publication in publications:
            publication_id = _int(publication.get("id"))
            status = _platform(publication.get("status"))
            if status == "published":
                published_state = _int(gc.get("published")) if gc else None
                if published_state != 1:
                    state_label = "missing" if gc is None else _generated_state_label(published_state)
                    issues.append(
                        _issue(
                            "publication_published_generated_unpublished",
                            content_id,
                            platform,
                            f"content_publications is published but generated_content is {state_label}",
                            generated_content_id=content_id if gc else None,
                            content_publication_id=publication_id,
                        )
                    )
                missing = [
                    field
                    for field in ("platform_post_id", "platform_url")
                    if not _text(publication.get(field))
                ]
                if missing:
                    issues.append(
                        _issue(
                            "published_row_missing_identifier",
                            content_id,
                            platform,
                            "published content_publications row is missing "
                            + " and ".join(missing),
                            generated_content_id=content_id if gc else None,
                            content_publication_id=publication_id,
                            details={"missing_fields": missing},
                        )
                    )

            if status == "failed":
                failure_time = _parse_datetime(
                    publication.get("last_error_at") or publication.get("updated_at")
                )
                later_successes = [
                    row
                    for row in successful_attempts
                    if failure_time is None
                    or (
                        _parse_datetime(row.get("attempted_at")) is not None
                        and _parse_datetime(row.get("attempted_at")) > failure_time
                    )
                ]
                if later_successes:
                    issues.append(
                        _issue(
                            "publication_failed_after_success",
                            content_id,
                            platform,
                            "content_publications is failed even though a later successful attempt exists",
                            generated_content_id=content_id if gc else None,
                            content_publication_id=publication_id,
                            publication_attempt_ids=tuple(
                                _int(row.get("id")) for row in later_successes if _int(row.get("id")) is not None
                            ),
                        )
                    )

        if platform == "x" and gc and published_publications:
            x_publication = _latest_row(published_publications, "published_at", "updated_at")
            gc_tweet_id = _text(gc.get("tweet_id"))
            cp_post_id = _text(x_publication.get("platform_post_id"))
            if gc_tweet_id and cp_post_id and gc_tweet_id != cp_post_id:
                issues.append(
                    _issue(
                        "x_tweet_id_mismatch",
                        content_id,
                        platform,
                        "generated_content.tweet_id disagrees with the X publication post id",
                        generated_content_id=content_id,
                        content_publication_id=_int(x_publication.get("id")),
                        details={
                            "generated_content_tweet_id": gc_tweet_id,
                            "content_publications_platform_post_id": cp_post_id,
                        },
                    )
                )
            gc_url = _text(gc.get("published_url"))
            cp_url = _text(x_publication.get("platform_url"))
            if gc_url and cp_url and gc_url != cp_url:
                issues.append(
                    _issue(
                        "x_published_url_mismatch",
                        content_id,
                        platform,
                        "generated_content.published_url disagrees with the X publication URL",
                        generated_content_id=content_id,
                        content_publication_id=_int(x_publication.get("id")),
                        details={
                            "generated_content_published_url": gc_url,
                            "content_publications_platform_url": cp_url,
                        },
                    )
                )

        successful_post_ids = {
            _text(row.get("platform_post_id"))
            for row in successful_attempts
            if _text(row.get("platform_post_id"))
        }
        if len(successful_post_ids) > 1:
            issues.append(
                _issue(
                    "duplicate_successful_attempt_post_ids",
                    content_id,
                    platform,
                    "multiple successful publication attempts have different platform post IDs",
                    generated_content_id=content_id if gc else None,
                    publication_attempt_ids=tuple(
                        _int(row.get("id")) for row in successful_attempts if _int(row.get("id")) is not None
                    ),
                    details={"platform_post_ids": sorted(successful_post_ids)},
                )
            )

    issues.sort(key=lambda issue: (issue.content_id, issue.platform, issue.issue_code, issue.message))
    return issues


def _load_generated_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    filters = []
    params: list[Any] = []
    timestamp_fields = [column for column in ("published_at", "created_at") if column in columns]
    if timestamp_fields:
        filters.append(_window_filter("gc", timestamp_fields))
        params.append(cutoff.isoformat())
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = conn.execute(
        f"""SELECT
               gc.id AS id,
               gc.published AS published,
               {_column_expr(columns, "published_url", "NULL", alias="gc")} AS published_url,
               {_column_expr(columns, "tweet_id", "NULL", alias="gc")} AS tweet_id,
               {_column_expr(columns, "created_at", "NULL", alias="gc")} AS created_at,
               {_column_expr(columns, "published_at", "NULL", alias="gc")} AS published_at
           FROM generated_content gc
           {where_clause}
           ORDER BY gc.id ASC""",
        params,
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
               {_column_expr(columns, "platform_post_id", "NULL", alias="cp")} AS platform_post_id,
               {_column_expr(columns, "platform_url", "NULL", alias="cp")} AS platform_url,
               {_column_expr(columns, "published_at", "NULL", alias="cp")} AS published_at,
               {_column_expr(columns, "updated_at", "NULL", alias="cp")} AS updated_at,
               {_column_expr(columns, "last_error_at", "NULL", alias="cp")} AS last_error_at
           FROM content_publications cp
           {where_clause}
           ORDER BY cp.content_id ASC, cp.platform ASC, cp.id ASC""",
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
    attempt_rows: list[dict[str, Any]],
    issues: list[PublicationStateReconciliationIssue],
) -> dict[str, Any]:
    by_code = Counter(issue.issue_code for issue in issues)
    return {
        "generated_content_count": len(generated_rows),
        "content_publication_count": len(publication_rows),
        "publication_attempt_count": len(attempt_rows),
        "issue_count": len(issues),
        "affected_content_count": len({issue.content_id for issue in issues}),
        "by_issue_code": {code: by_code.get(code, 0) for code in ISSUE_CODES},
        "by_platform": dict(sorted(Counter(issue.platform for issue in issues).items())),
    }


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "generated_content": {"id", "published"},
        "content_publications": {"id", "content_id", "platform", "status"},
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
            "publication_attempt_count": 0,
            "issue_count": 0,
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


def _platform_where(alias: str, platforms: tuple[str, ...]) -> tuple[list[str], list[Any]]:
    if not platforms:
        return [], []
    placeholders = ",".join("?" for _ in platforms)
    return [f"LOWER(COALESCE({alias}.platform, '')) IN ({placeholders})"], list(platforms)


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


def _latest_row(
    rows: Sequence[Mapping[str, Any]],
    *timestamp_fields: str,
) -> Mapping[str, Any]:
    return max(
        rows,
        key=lambda row: (
            *(_parse_datetime(row.get(field)) or datetime.min.replace(tzinfo=timezone.utc) for field in timestamp_fields),
            _int(row.get("id")) or 0,
        ),
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


def _generated_state_label(value: int | None) -> str:
    if value == -1:
        return "abandoned"
    if value == 0:
        return "unpublished"
    return f"state {value}"


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
