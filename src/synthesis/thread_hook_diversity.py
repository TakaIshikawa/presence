"""Detect repetitive opening hook structures in X thread candidates."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any

from output.x_client import parse_thread_content
from synthesis.thread_validator import THREAD_MARKER_RE, parse_thread_posts


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 50
DEFAULT_MAX_SHARE = 0.5
DEFAULT_STATUSES = ("queued", "held")
HOOK_CATEGORIES = (
    "question",
    "contrast",
    "lesson",
    "confession",
    "build-log",
    "announcement",
    "plain",
    "empty",
)

_NUMBER_RE = re.compile(r"^\s*(?:\d+[.)/:-]\s*)+")
_WHITESPACE_RE = re.compile(r"\s+")
_CONTRAST_RE = re.compile(
    r"(?i)\b(but|actually|instead|however|yet|while)\b|"
    r"\b(most|many)\s+(teams|people|developers|founders)\b.*\b(miss|think|assume|expect)\b|"
    r"\b(not\s+.+\s+but|from\s+.+\s+to)\b"
)
_LESSON_RE = re.compile(
    r"(?i)\b(lesson|learned|takeaway|what\s+i\s+learned|here'?s\s+what)\b|"
    r"\b\d+\s+(things|lessons|rules|patterns|mistakes)\b"
)
_CONFESSION_RE = re.compile(
    r"(?i)\b(confession|i\s+was\s+wrong|we\s+were\s+wrong|i\s+used\s+to|"
    r"i\s+thought|i\s+assumed|i\s+missed|i\s+ignored)\b"
)
_BUILD_LOG_RE = re.compile(
    r"(?i)\b(i|we)\s+(built|shipped|launched|implemented|added|fixed|debugged|"
    r"rewrote|migrated|created|made)\b|"
    r"\b(building|shipping|launching|debugging|rewriting|build log)\b"
)
_ANNOUNCEMENT_RE = re.compile(
    r"(?i)\b(announcing|launching|introducing|new:|update:|release:|shipped:|"
    r"just shipped|now available)\b"
)


@dataclass(frozen=True)
class ThreadHookRecord:
    """One thread candidate included in the diversity audit."""

    thread_id: int
    source: str
    source_id: int | None
    status: str
    scheduled_at: str | None
    created_at: str | None
    opening: str
    hook_category: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ThreadHookDiversityFinding:
    """One thread whose hook category is overrepresented."""

    thread_id: int
    hook_category: str
    duplicate_count: int
    category_count: int
    category_share: float
    recommendation: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ThreadHookDiversityReport:
    """Read-only hook diversity report for X thread candidates."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    findings: tuple[ThreadHookDiversityFinding, ...]
    records: tuple[ThreadHookRecord, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "thread_hook_diversity",
            "filters": dict(self.filters),
            "findings": [finding.to_dict() for finding in self.findings],
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "records": [record.to_dict() for record in self.records],
            "totals": dict(sorted(self.totals.items())),
        }


def build_thread_hook_diversity_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    max_share: float = DEFAULT_MAX_SHARE,
    status: tuple[str, ...] = DEFAULT_STATUSES,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ThreadHookDiversityReport:
    """Build a report that flags overused first-tweet hook categories."""
    if days <= 0:
        raise ValueError("days must be positive")
    if not 0 < max_share <= 1:
        raise ValueError("max_share must be greater than 0 and at most 1")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive when provided")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    filters = {
        "days": days,
        "limit": limit,
        "max_share": max_share,
        "status": list(status),
    }
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns.get("generated_content"):
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    cutoff = generated_at - timedelta(days=days)
    rows = _fetch_candidate_rows(
        conn,
        schema,
        cutoff=cutoff,
        status=tuple(s.lower() for s in status),
        limit=limit,
    )
    records = tuple(_record_from_row(row) for row in rows)
    findings = tuple(_build_findings(records, max_share=max_share))
    by_category: dict[str, int] = {}
    for record in records:
        by_category[record.hook_category] = by_category.get(record.hook_category, 0) + 1

    return ThreadHookDiversityReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "record_count": len(records),
            "finding_count": len(findings),
            "by_category": dict(sorted(by_category.items())),
        },
        findings=findings,
        records=records,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def extract_first_tweet_hook(content: str) -> str:
    """Extract the first tweet opening from stored thread content."""
    text = str(content or "").strip()
    if not text:
        return ""

    decoded = _decode_json(text)
    if decoded is not None:
        opening = _opening_from_json(decoded)
        if opening:
            return _normalize_opening(opening)

    if any(THREAD_MARKER_RE.match(line) for line in text.splitlines()):
        posts, _ = parse_thread_posts(text)
        return _normalize_opening(posts[0].text) if posts else ""

    parts = parse_thread_content(text)
    if parts:
        return _normalize_opening(parts[0])
    return _normalize_opening(text)


def classify_thread_hook(opening: str) -> str:
    """Classify a first-tweet hook into a deterministic category."""
    normalized = _normalize_opening(opening)
    if not normalized:
        return "empty"
    if len(normalized) < 8:
        return "plain"
    if "?" in normalized:
        return "question"
    if _CONFESSION_RE.search(normalized):
        return "confession"
    if _BUILD_LOG_RE.search(normalized):
        return "build-log"
    if _LESSON_RE.search(normalized):
        return "lesson"
    if _ANNOUNCEMENT_RE.search(normalized):
        return "announcement"
    if _CONTRAST_RE.search(normalized):
        return "contrast"
    return "plain"


def format_thread_hook_diversity_json(report: ThreadHookDiversityReport) -> str:
    """Serialize the hook diversity report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _fetch_candidate_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    status: tuple[str, ...],
    limit: int | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rows.extend(_queued_rows(conn, schema, cutoff=cutoff, status=status))
    rows.extend(_generated_rows(conn, schema, cutoff=cutoff))

    by_thread_id: dict[int, dict[str, Any]] = {}
    for row in rows:
        thread_id = int(row["thread_id"])
        current = by_thread_id.get(thread_id)
        if current is None or _row_rank(row) < _row_rank(current):
            by_thread_id[thread_id] = row

    sorted_rows = sorted(by_thread_id.values(), key=_candidate_sort_key)
    if limit is not None:
        return sorted_rows[:limit]
    return sorted_rows


def _queued_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    status: tuple[str, ...],
) -> list[dict[str, Any]]:
    if "publish_queue" not in schema:
        return []
    pq = schema["publish_queue"]
    gc = schema["generated_content"]
    if not {"id", "content_id", "status"}.issubset(pq):
        return []

    timestamp_expr = _coalesce_expr("pq", pq, ("scheduled_at", "created_at", "published_at"))
    placeholders = ", ".join("?" for _ in status) or "''"
    platform_filter = (
        "AND LOWER(COALESCE(pq.platform, 'all')) IN ('x', 'all')"
        if "platform" in pq
        else ""
    )
    params: list[Any] = list(status)
    if timestamp_expr != "NULL":
        params.append(cutoff.isoformat())
        cutoff_filter = f"AND {timestamp_expr} >= ?"
    else:
        cutoff_filter = ""

    rows = conn.execute(
        f"""SELECT
                  gc.id AS thread_id,
                  gc.content AS content,
                  {_column_expr("gc", gc, "created_at")} AS created_at,
                  'publish_queue' AS source,
                  pq.id AS source_id,
                  pq.status AS status,
                  {_column_expr("pq", pq, "scheduled_at")} AS scheduled_at
           FROM publish_queue pq
           INNER JOIN generated_content gc ON gc.id = pq.content_id
           WHERE gc.content_type = 'x_thread'
             AND LOWER(COALESCE(pq.status, '')) IN ({placeholders})
             {platform_filter}
             {cutoff_filter}
           ORDER BY {timestamp_expr} DESC, pq.id DESC""",
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows]


def _generated_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    gc = schema["generated_content"]
    timestamp_expr = _coalesce_expr("gc", gc, ("created_at", "published_at"))
    if timestamp_expr == "NULL":
        return []
    published_filter = "AND COALESCE(gc.published, 0) = 0" if "published" in gc else ""
    queue_exclusion = (
        "AND NOT EXISTS (SELECT 1 FROM publish_queue pq WHERE pq.content_id = gc.id)"
        if "publish_queue" in schema and "content_id" in schema["publish_queue"]
        else ""
    )
    rows = conn.execute(
        f"""SELECT
                  gc.id AS thread_id,
                  gc.content AS content,
                  {_column_expr("gc", gc, "created_at")} AS created_at,
                  'generated_content' AS source,
                  gc.id AS source_id,
                  'generated' AS status,
                  NULL AS scheduled_at
           FROM generated_content gc
           WHERE gc.content_type = 'x_thread'
             AND {timestamp_expr} >= ?
             {published_filter}
             {queue_exclusion}
           ORDER BY {timestamp_expr} DESC, gc.id DESC""",
        (cutoff.isoformat(),),
    ).fetchall()
    return [dict(row) for row in rows]


def _record_from_row(row: dict[str, Any]) -> ThreadHookRecord:
    opening = extract_first_tweet_hook(str(row.get("content") or ""))
    return ThreadHookRecord(
        thread_id=int(row["thread_id"]),
        source=str(row["source"]),
        source_id=_optional_int(row.get("source_id")),
        status=str(row.get("status") or "unknown"),
        scheduled_at=_clean(row.get("scheduled_at")),
        created_at=_clean(row.get("created_at")),
        opening=opening,
        hook_category=classify_thread_hook(opening),
    )


def _build_findings(
    records: tuple[ThreadHookRecord, ...],
    *,
    max_share: float,
) -> list[ThreadHookDiversityFinding]:
    total = len(records)
    if not total:
        return []
    category_counts: dict[str, int] = {}
    for record in records:
        category_counts[record.hook_category] = category_counts.get(record.hook_category, 0) + 1

    findings: list[ThreadHookDiversityFinding] = []
    for record in records:
        count = category_counts[record.hook_category]
        share = round(count / total, 4)
        if share <= max_share:
            continue
        findings.append(
            ThreadHookDiversityFinding(
                thread_id=record.thread_id,
                hook_category=record.hook_category,
                duplicate_count=max(0, count - 1),
                category_count=count,
                category_share=share,
                recommendation=_recommendation(record.hook_category, share),
            )
        )
    findings.sort(key=lambda item: (-item.category_share, item.hook_category, item.thread_id))
    return findings


def _recommendation(category: str, share: float) -> str:
    return (
        f"{category} hooks make up {share:.0%} of candidate X threads; "
        "rewrite some openings into a different hook structure before publishing."
    )


def _opening_from_json(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        for item in value:
            opening = _opening_from_json(item)
            if opening:
                return opening
        return ""
    if isinstance(value, dict):
        for key in ("first_post", "opening", "hook", "text", "content", "body"):
            if key in value:
                opening = _opening_from_json(value[key])
                if opening:
                    return opening
        for key in ("thread", "tweets", "posts", "items", "parts"):
            if key in value:
                opening = _opening_from_json(value[key])
                if opening:
                    return opening
    return ""


def _decode_json(text: str) -> Any | None:
    stripped = text.strip()
    if not stripped or stripped[0] not in "[{":
        return None
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return None


def _normalize_opening(value: str) -> str:
    lines = [line.strip() for line in str(value or "").splitlines() if line.strip()]
    if not lines:
        return ""
    first = re.sub(r"(?i)^(?:tweet|post|thread)\s*\d*\s*[:.)-]\s*", "", lines[0])
    first = _NUMBER_RE.sub("", first)
    return _WHITESPACE_RE.sub(" ", first).strip()


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    missing_tables = []
    missing_columns: dict[str, tuple[str, ...]] = {}
    if "generated_content" not in schema:
        missing_tables.append("generated_content")
    else:
        required = ("id", "content_type", "content")
        missing = tuple(column for column in required if column not in schema["generated_content"])
        if missing:
            missing_columns["generated_content"] = missing
    return tuple(missing_tables), missing_columns


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> ThreadHookDiversityReport:
    return ThreadHookDiversityReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={"record_count": 0, "finding_count": 0, "by_category": {}},
        findings=(),
        records=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {
        row["name"] if isinstance(row, sqlite3.Row) else row[0]: {
            column[1]
            for column in conn.execute(
                f"PRAGMA table_info({row['name'] if isinstance(row, sqlite3.Row) else row[0]})"
            )
        }
        for row in tables
    }


def _coalesce_expr(alias: str, columns: set[str], candidates: tuple[str, ...]) -> str:
    existing = [f"{alias}.{column}" for column in candidates if column in columns]
    if not existing:
        return "NULL"
    if len(existing) == 1:
        return existing[0]
    return f"COALESCE({', '.join(existing)})"


def _column_expr(alias: str, columns: set[str], column: str) -> str:
    return f"{alias}.{column}" if column in columns else "NULL"


def _candidate_sort_key(row: dict[str, Any]) -> tuple[int, str, int]:
    source_rank = 0 if row.get("source") == "publish_queue" else 1
    timestamp = _clean(row.get("scheduled_at")) or _clean(row.get("created_at")) or ""
    return (source_rank, f"{-_timestamp_sort(timestamp):020.6f}", int(row["thread_id"]))


def _row_rank(row: dict[str, Any]) -> tuple[int, float]:
    source_rank = 0 if row.get("source") == "publish_queue" else 1
    timestamp = _clean(row.get("scheduled_at")) or _clean(row.get("created_at")) or ""
    return (source_rank, -_timestamp_sort(timestamp))


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _timestamp_sort(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0
