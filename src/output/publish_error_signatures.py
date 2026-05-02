"""Group repeated publish failures by normalized error signatures."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any

from .publish_errors import classify_publish_error, normalize_error_category


DEFAULT_DAYS = 30
DEFAULT_MIN_COUNT = 2
SUPPORTED_PLATFORMS = ("all", "x", "bluesky")
MAX_EXAMPLES = 3

_URL_RE = re.compile(r"\b(?:[a-z][a-z0-9+.-]*://|www\.)\S+", re.IGNORECASE)
_EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w.-]+\.[a-z]{2,}\b", re.IGNORECASE)
_ISO_TIMESTAMP_RE = re.compile(
    r"\b\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}"
    r"(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b",
    re.IGNORECASE,
)
_DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
_TIME_RE = re.compile(r"\b\d{1,2}:\d{2}(?::\d{2})?(?:\.\d+)?\b")
_UUID_RE = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)
_LONG_HEX_RE = re.compile(r"\b[0-9a-f]{12,}\b", re.IGNORECASE)
_LONG_TOKEN_RE = re.compile(r"\b[A-Za-z0-9_-]{20,}\b")
_KEYED_ID_RE = re.compile(
    r"\b("
    r"id|ids|tweet|content|queue|publication|attempt|request|trace|"
    r"uri|cid|did|record"
    r")([ #:=/-]+)[A-Za-z0-9_.:-]{3,}\b",
    re.IGNORECASE,
)
_NUMBER_RE = re.compile(r"\b\d+\b")
_WHITESPACE_RE = re.compile(r"\s+")
_STATUS_CODES = (
    "400",
    "401",
    "402",
    "403",
    "404",
    "409",
    "422",
    "429",
    "500",
    "502",
    "503",
    "504",
)


@dataclass(frozen=True)
class PublishErrorSignature:
    """One grouped publish failure signature."""

    platform: str
    error_category: str
    signature: str
    count: int
    source_counts: dict[str, int]
    platforms: tuple[str, ...]
    categories: tuple[str, ...]
    first_seen: str | None
    last_seen: str | None
    suggested_action: str
    queue_ids: tuple[int, ...]
    publication_ids: tuple[int, ...]
    attempt_ids: tuple[int, ...]
    content_ids: tuple[int, ...]
    example_errors: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["platforms"] = list(self.platforms)
        payload["categories"] = list(self.categories)
        payload["queue_ids"] = list(self.queue_ids)
        payload["publication_ids"] = list(self.publication_ids)
        payload["attempt_ids"] = list(self.attempt_ids)
        payload["content_ids"] = list(self.content_ids)
        payload["example_errors"] = list(self.example_errors)
        payload["source_counts"] = dict(sorted(self.source_counts.items()))
        return payload


@dataclass(frozen=True)
class PublishErrorSignatureReport:
    """Read-only report of repeated publish failure signatures."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    signatures: tuple[PublishErrorSignature, ...]
    availability: dict[str, bool]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "publish_error_signatures",
            "availability": dict(sorted(self.availability.items())),
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "signature_count": len(self.signatures),
            "signatures": [signature.to_dict() for signature in self.signatures],
            "totals": dict(sorted(self.totals.items())),
        }


@dataclass(frozen=True)
class _FailureRow:
    source: str
    source_id: int | None
    queue_id: int | None
    publication_id: int | None
    attempt_id: int | None
    content_id: int | None
    platform: str
    error_category: str
    error: str
    seen_at: str | None


def build_publish_error_signature_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_count: int = DEFAULT_MIN_COUNT,
    platform: str = "all",
    now: datetime | None = None,
) -> PublishErrorSignatureReport:
    """Aggregate failed publication rows by normalized error signature."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_count <= 0:
        raise ValueError("min_count must be positive")
    if platform not in SUPPORTED_PLATFORMS:
        raise ValueError(f"platform must be one of: {', '.join(SUPPORTED_PLATFORMS)}")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    availability = {
        "publish_queue": _publish_queue_available(schema),
        "content_publications": _content_publications_available(schema),
        "publication_attempts": _publication_attempts_available(schema),
    }
    missing_tables = tuple(
        table
        for table in ("publish_queue",)
        if table not in schema or not _publish_queue_available(schema)
    )
    missing_columns = _missing_columns(schema)
    filters = {"days": days, "min_count": min_count, "platform": platform}

    if missing_tables:
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            availability=availability,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows: list[_FailureRow] = []
    rows.extend(_load_publish_queue_rows(conn, schema, cutoff=cutoff, platform=platform))
    rows.extend(_load_content_publication_rows(conn, schema, cutoff=cutoff, platform=platform))
    rows.extend(_load_publication_attempt_rows(conn, schema, cutoff=cutoff, platform=platform))
    signatures = tuple(_build_signatures(rows, min_count=min_count))
    by_source: dict[str, int] = {}
    by_platform: dict[str, int] = {}
    for row in rows:
        by_source[row.source] = by_source.get(row.source, 0) + 1
        by_platform[row.platform] = by_platform.get(row.platform, 0) + 1

    return PublishErrorSignatureReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "failure_count": len(rows),
            "reported_signature_count": len(signatures),
            "by_source": dict(sorted(by_source.items())),
            "by_platform": dict(sorted(by_platform.items())),
        },
        signatures=signatures,
        availability=availability,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_publish_error_signature_json(report: PublishErrorSignatureReport) -> str:
    """Serialize a publish error signature report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publish_error_signature_text(report: PublishErrorSignatureReport) -> str:
    """Render an operator-readable publish error signature report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Publish Error Signatures",
        f"Generated: {report.generated_at}",
        (
            f"Window: {filters['days']} days; "
            f"min_count={filters['min_count']}; platform={filters['platform']}"
        ),
        (
            f"Totals: {totals['failure_count']} failures, "
            f"{totals['reported_signature_count']} signatures"
        ),
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
    unavailable = [
        table
        for table in ("content_publications", "publication_attempts")
        if not report.availability.get(table, False)
    ]
    if unavailable:
        lines.append(f"Unavailable enrichment tables: {', '.join(unavailable)}")
    lines.append("")

    if not report.signatures:
        lines.append("No repeated publish error signatures found.")
        return "\n".join(lines)

    lines.append("Repeated signatures:")
    for signature in report.signatures:
        queue_ids = _join_ids(signature.queue_ids)
        content_ids = _join_ids(signature.content_ids)
        example = signature.example_errors[0] if signature.example_errors else "-"
        lines.append(
            "  - {platform} / {category}: count={count} "
            "first={first_seen} last={last_seen} action={action}".format(
                platform=signature.platform,
                category=signature.error_category,
                count=signature.count,
                first_seen=signature.first_seen or "-",
                last_seen=signature.last_seen or "-",
                action=signature.suggested_action,
            )
        )
        lines.append(f"    signature: {signature.signature}")
        lines.append(
            f"    queue_ids={queue_ids}; content_ids={content_ids}; example={example}"
        )
    return "\n".join(lines)


def normalize_publish_error_signature(error: Any) -> str:
    """Collapse volatile ids, URLs, and timestamps out of publish error text."""
    text = str(error or "").strip().lower()
    if not text:
        return "(empty error)"

    text = _URL_RE.sub("<url>", text)
    text = _EMAIL_RE.sub("<email>", text)
    text = _ISO_TIMESTAMP_RE.sub("<timestamp>", text)
    text = _DATE_RE.sub("<date>", text)
    text = _TIME_RE.sub("<time>", text)
    text = _UUID_RE.sub("<id>", text)
    text = _KEYED_ID_RE.sub(
        lambda match: f"{match.group(1).lower()}{match.group(2)}<id>",
        text,
    )
    text = _LONG_HEX_RE.sub("<id>", text)
    text = _LONG_TOKEN_RE.sub("<id>", text)

    placeholders: dict[str, str] = {}
    for index, code in enumerate(_STATUS_CODES):
        token = f"__status_{index}__"
        placeholders[token] = code
        text = re.sub(rf"\b{re.escape(code)}\b", token, text)
    text = _NUMBER_RE.sub("<id>", text)
    for token, code in placeholders.items():
        text = text.replace(token, code)

    text = _WHITESPACE_RE.sub(" ", text)
    text = text.replace("( ", "(").replace(" )", ")").strip(" .")
    return text or "(empty error)"


def _build_signatures(
    rows: list[_FailureRow],
    *,
    min_count: int,
) -> list[PublishErrorSignature]:
    grouped: dict[tuple[str, str, str], list[_FailureRow]] = {}
    for row in rows:
        signature = normalize_publish_error_signature(row.error)
        grouped.setdefault((row.platform, row.error_category, signature), []).append(row)

    signatures: list[PublishErrorSignature] = []
    for (platform, category, signature), group_rows in grouped.items():
        if len(group_rows) < min_count:
            continue
        seen = sorted(
            timestamp for timestamp in (row.seen_at for row in group_rows) if timestamp
        )
        source_counts: dict[str, int] = {}
        examples: list[str] = []
        for row in sorted(group_rows, key=_row_sort_key):
            source_counts[row.source] = source_counts.get(row.source, 0) + 1
            if row.error and row.error not in examples:
                examples.append(row.error)
        signatures.append(
            PublishErrorSignature(
                platform=platform,
                error_category=category,
                signature=signature,
                count=len(group_rows),
                source_counts=source_counts,
                platforms=tuple(sorted({row.platform for row in group_rows})),
                categories=tuple(sorted({row.error_category for row in group_rows})),
                first_seen=seen[0] if seen else None,
                last_seen=seen[-1] if seen else None,
                suggested_action=_suggested_action(category),
                queue_ids=_ids(row.queue_id for row in group_rows),
                publication_ids=_ids(row.publication_id for row in group_rows),
                attempt_ids=_ids(row.attempt_id for row in group_rows),
                content_ids=_ids(row.content_id for row in group_rows),
                example_errors=tuple(examples[:MAX_EXAMPLES]),
            )
        )
    signatures.sort(
        key=lambda item: (
            -item.count,
            item.platform,
            item.error_category,
            item.signature,
        )
    )
    return signatures


def _load_publish_queue_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    platform: str,
) -> list[_FailureRow]:
    if not _publish_queue_available(schema):
        return []
    columns = schema["publish_queue"]
    timestamp_expr = _coalesce_expr(
        "pq",
        columns,
        ("created_at", "published_at", "scheduled_at"),
    )
    where = ["LOWER(pq.status) = 'failed'"]
    params: list[Any] = []
    if platform != "all" and "platform" in columns:
        where.append("pq.platform = ?")
        params.append(platform)
    if timestamp_expr != "NULL":
        where.append(f"{timestamp_expr} >= ?")
        params.append(cutoff.isoformat())

    rows = conn.execute(
        f"""SELECT {_column_expr("pq", columns, "id")} AS queue_id,
                  {_column_expr("pq", columns, "content_id")} AS content_id,
                  {_column_expr("pq", columns, "platform", "'unknown'")} AS platform,
                  {_column_expr("pq", columns, "error")} AS error,
                  {_column_expr("pq", columns, "error_category")} AS error_category,
                  {timestamp_expr} AS seen_at
           FROM publish_queue pq
           WHERE {' AND '.join(where)}
           ORDER BY seen_at ASC, queue_id ASC""",
        params,
    ).fetchall()
    return [
        _failure_row(dict(row), source="publish_queue", id_key="queue_id")
        for row in rows
        if _clean(dict(row).get("error"))
    ]


def _load_content_publication_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    platform: str,
) -> list[_FailureRow]:
    if not _content_publications_available(schema):
        return []
    columns = schema["content_publications"]
    timestamp_expr = _coalesce_expr(
        "cp",
        columns,
        ("last_error_at", "updated_at", "published_at", "next_retry_at"),
    )
    where = ["LOWER(cp.status) = 'failed'"]
    params: list[Any] = []
    if platform != "all":
        where.append("cp.platform = ?")
        params.append(platform)
    if timestamp_expr != "NULL":
        where.append(f"{timestamp_expr} >= ?")
        params.append(cutoff.isoformat())
    rows = conn.execute(
        f"""SELECT {_column_expr("cp", columns, "id")} AS publication_id,
                  {_column_expr("cp", columns, "content_id")} AS content_id,
                  {_column_expr("cp", columns, "platform", "'unknown'")} AS platform,
                  {_column_expr("cp", columns, "error")} AS error,
                  {_column_expr("cp", columns, "error_category")} AS error_category,
                  {timestamp_expr} AS seen_at
           FROM content_publications cp
           WHERE {' AND '.join(where)}
           ORDER BY seen_at ASC, publication_id ASC""",
        params,
    ).fetchall()
    return [
        _failure_row(dict(row), source="content_publications", id_key="publication_id")
        for row in rows
        if _clean(dict(row).get("error"))
    ]


def _load_publication_attempt_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    platform: str,
) -> list[_FailureRow]:
    if not _publication_attempts_available(schema):
        return []
    columns = schema["publication_attempts"]
    timestamp_expr = _column_expr("pa", columns, "attempted_at")
    where = ["COALESCE(pa.success, 0) = 0"]
    params: list[Any] = []
    if platform != "all":
        where.append("pa.platform = ?")
        params.append(platform)
    if timestamp_expr != "NULL":
        where.append(f"{timestamp_expr} >= ?")
        params.append(cutoff.isoformat())
    rows = conn.execute(
        f"""SELECT {_column_expr("pa", columns, "id")} AS attempt_id,
                  {_column_expr("pa", columns, "queue_id")} AS queue_id,
                  {_column_expr("pa", columns, "content_id")} AS content_id,
                  {_column_expr("pa", columns, "platform", "'unknown'")} AS platform,
                  {_column_expr("pa", columns, "error")} AS error,
                  {_column_expr("pa", columns, "error_category")} AS error_category,
                  {timestamp_expr} AS seen_at
           FROM publication_attempts pa
           WHERE {' AND '.join(where)}
           ORDER BY seen_at ASC, attempt_id ASC""",
        params,
    ).fetchall()
    return [
        _failure_row(dict(row), source="publication_attempts", id_key="attempt_id")
        for row in rows
        if _clean(dict(row).get("error"))
    ]


def _failure_row(data: dict[str, Any], *, source: str, id_key: str) -> _FailureRow:
    error = _clean(data.get("error")) or ""
    category = normalize_error_category(data.get("error_category"))
    if category == "unknown":
        category = classify_publish_error(error, platform=_clean(data.get("platform")))
    return _FailureRow(
        source=source,
        source_id=_optional_int(data.get(id_key)),
        queue_id=_optional_int(data.get("queue_id")),
        publication_id=_optional_int(data.get("publication_id")),
        attempt_id=_optional_int(data.get("attempt_id")),
        content_id=_optional_int(data.get("content_id")),
        platform=_clean(data.get("platform")) or "unknown",
        error_category=category,
        error=error,
        seen_at=_clean(data.get("seen_at")),
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
    ).fetchall()
    names = [row[0] for row in rows]
    return {
        name: {column[1] for column in conn.execute(f"PRAGMA table_info({name})")}
        for name in names
    }


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    expected = {
        "publish_queue": ("id", "content_id", "platform", "status", "error"),
        "content_publications": ("id", "content_id", "platform", "status", "error"),
        "publication_attempts": (
            "id",
            "content_id",
            "platform",
            "success",
            "error",
            "attempted_at",
        ),
    }
    return {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema
    }


def _publish_queue_available(schema: dict[str, set[str]]) -> bool:
    return "publish_queue" in schema and {"id", "content_id", "status", "error"}.issubset(
        schema["publish_queue"]
    )


def _content_publications_available(schema: dict[str, set[str]]) -> bool:
    return "content_publications" in schema and {
        "id",
        "content_id",
        "platform",
        "status",
        "error",
    }.issubset(schema["content_publications"])


def _publication_attempts_available(schema: dict[str, set[str]]) -> bool:
    return "publication_attempts" in schema and {
        "id",
        "content_id",
        "platform",
        "success",
        "error",
        "attempted_at",
    }.issubset(schema["publication_attempts"])


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


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    availability: dict[str, bool],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> PublishErrorSignatureReport:
    return PublishErrorSignatureReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "failure_count": 0,
            "reported_signature_count": 0,
            "by_source": {},
            "by_platform": {},
        },
        signatures=(),
        availability=availability,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _suggested_action(category: str) -> str:
    if category in {"network", "rate_limit"}:
        return "retry_later"
    if category == "auth":
        return "fix_credentials"
    if category == "media":
        return "fix_media"
    if category == "duplicate":
        return "cancel_duplicate"
    if category == "validation":
        return "fix_content"
    return "inspect_error"


def _row_sort_key(row: _FailureRow) -> tuple[str, str, int]:
    return (row.seen_at or "", row.source, row.source_id or 0)


def _ids(values: Any) -> tuple[int, ...]:
    return tuple(sorted({int(value) for value in values if value is not None}))


def _join_ids(values: tuple[int, ...]) -> str:
    return ", ".join(str(value) for value in values) if values else "-"


def _clean(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
