"""Summarize curated source ingestion failures."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse


DEFAULT_DAYS = 7
DEFAULT_MIN_FAILURES = 1
TABLE = "curated_sources"

REQUIRED_COLUMNS = ("source_type", "identifier")
OPTIONAL_COLUMNS = (
    "id",
    "name",
    "feed_url",
    "homepage_url",
    "status",
    "last_fetch_status",
    "consecutive_failures",
    "last_success_at",
    "last_failure_at",
    "last_error",
)

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def build_curated_ingestion_failure_digest(
    failure_rows: Iterable[Mapping[str, Any]],
    *,
    min_failures: int = DEFAULT_MIN_FAILURES,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Group failure rows by source and normalized error category."""
    if min_failures <= 0:
        raise ValueError("min_failures must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    buckets: dict[tuple[str, str, str], dict[str, Any]] = {}
    raw_rows = [_mapping(row) for row in failure_rows]
    considered = 0

    for row in raw_rows:
        failure_count = _int(row.get("consecutive_failures"), default=1)
        if failure_count < min_failures:
            continue
        error_text = _clean(row.get("last_error")) or ""
        category = normalize_curated_ingestion_error_category(error_text)
        source_key = _source_key(row)
        key = (source_key, category, retryability_for_error_category(category, error_text))
        considered += 1
        bucket = buckets.setdefault(
            key,
            {
                "error_category": category,
                "failure_count": 0,
                "first_failure_at": None,
                "last_failure_at": None,
                "last_success_at": None,
                "representative_error": error_text or None,
                "retryability": key[2],
                "source": source_key,
                "source_type": _clean(row.get("source_type")) or "unknown",
                "sources": [],
                "stale_success_age_days": None,
            },
        )
        bucket["failure_count"] += failure_count
        bucket["first_failure_at"] = _min_timestamp(
            bucket["first_failure_at"],
            _clean(row.get("last_failure_at")),
        )
        bucket["last_failure_at"] = _max_timestamp(
            bucket["last_failure_at"],
            _clean(row.get("last_failure_at")),
        )
        bucket["last_success_at"] = _oldest_success_timestamp(
            bucket["last_success_at"],
            _clean(row.get("last_success_at")),
        )
        success_age = _age_days(generated_at, _parse_datetime(row.get("last_success_at")))
        existing_age = bucket.get("stale_success_age_days")
        bucket["stale_success_age_days"] = (
            success_age
            if existing_age is None
            else success_age
            if success_age is not None and success_age > existing_age
            else existing_age
        )
        bucket["sources"].append(_source_detail(row, generated_at))

    digest = list(buckets.values())
    for bucket in digest:
        bucket["sources"].sort(key=_source_detail_sort_key)
        bucket["source_count"] = len(bucket["sources"])
        if not bucket["representative_error"]:
            bucket["representative_error"] = next(
                (source["last_error"] for source in bucket["sources"] if source.get("last_error")),
                None,
            )
    digest.sort(key=_digest_sort_key)

    by_category = Counter(item["error_category"] for item in digest)
    by_retryability = Counter(item["retryability"] for item in digest)
    return {
        "artifact_type": "curated_ingestion_failure_digest",
        "generated_at": generated_at.isoformat(),
        "groups": digest,
        "totals": {
            "failure_groups": len(digest),
            "failure_rows_considered": considered,
            "failure_rows_scanned": len(raw_rows),
            "failures": sum(int(item["failure_count"]) for item in digest),
            "by_error_category": dict(sorted(by_category.items())),
            "by_retryability": dict(sorted(by_retryability.items())),
        },
    }


def build_curated_ingestion_failure_digest_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_failures: int = DEFAULT_MIN_FAILURES,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load recent curated source failures and return a JSON-ready digest."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_failures <= 0:
        raise ValueError("min_failures must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": cutoff.isoformat(),
        "min_failures": min_failures,
    }
    schema_gaps: dict[str, Any] = {"missing_columns": {}, "missing_tables": []}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if TABLE not in schema:
        digest = build_curated_ingestion_failure_digest(
            [],
            min_failures=min_failures,
            now=generated_at,
        )
        schema_gaps["missing_tables"] = [TABLE]
    else:
        missing_required = [column for column in REQUIRED_COLUMNS if column not in schema[TABLE]]
        missing_optional = [column for column in OPTIONAL_COLUMNS if column not in schema[TABLE]]
        if missing_required or missing_optional:
            schema_gaps["missing_columns"] = {
                TABLE: [*missing_required, *missing_optional],
            }
        rows = [] if missing_required else _load_failure_rows(conn, schema[TABLE], cutoff=cutoff)
        digest = build_curated_ingestion_failure_digest(
            rows,
            min_failures=min_failures,
            now=generated_at,
        )

    return {
        **digest,
        "filters": filters,
        "schema_gaps": schema_gaps,
        "source_table": TABLE if TABLE in schema else None,
    }


def format_curated_ingestion_failure_digest_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report, indent=2, sort_keys=True)


def normalize_curated_ingestion_error_category(error: object) -> str:
    """Classify a curated ingestion error into a stable category."""
    text = str(error or "").lower()
    compact = _NON_ALNUM_RE.sub(" ", text)
    if not compact.strip():
        return "unknown"
    if _contains_any(compact, ("rate limit", "ratelimit", "too many requests", "429", "throttl")):
        return "rate_limit"
    if _contains_any(
        compact,
        ("unauthorized", "forbidden", "authentication", "invalid token", "401", "403"),
    ):
        return "auth"
    if _contains_any(compact, ("not found", "404", "gone", "410", "no such user")):
        return "not_found"
    if _contains_any(compact, ("timeout", "timed out", "connection", "network", "dns", "ssl")):
        return "network"
    if _contains_any(compact, ("bad gateway", "service unavailable", "gateway timeout", "502", "503", "504")):
        return "unavailable"
    if _contains_any(compact, ("xml", "rss", "atom", "feed", "parse", "malformed")):
        return "parse"
    if _contains_any(compact, ("extractor", "anthropic", "api error", "model")):
        return "extractor"
    if _contains_any(compact, ("invalid", "validation", "unsupported", "too short")):
        return "validation"
    return "unknown"


def retryability_for_error_category(category: str, error: object = None) -> str:
    """Return retryability when error text supports a clear distinction."""
    if category in {"rate_limit", "network", "unavailable", "extractor"}:
        return "retryable"
    if category in {"auth", "not_found", "validation"}:
        return "non_retryable"
    if category == "parse":
        text = str(error or "").lower()
        if _contains_any(text, ("temporarily", "truncated", "timeout")):
            return "retryable"
        return "non_retryable"
    return "unknown"


def _load_failure_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    select = [_column_expr(columns, column) for column in OPTIONAL_COLUMNS]
    for column in REQUIRED_COLUMNS:
        if column not in OPTIONAL_COLUMNS:
            select.append(_column_expr(columns, column))
    where = []
    params: list[Any] = []
    if "last_fetch_status" in columns:
        where.append("LOWER(COALESCE(last_fetch_status, 'failure')) IN ('failure', 'quarantined')")
    if "last_failure_at" in columns:
        where.append("(last_failure_at IS NULL OR last_failure_at >= ?)")
        params.append(cutoff.isoformat())
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    order_sql = (
        "last_failure_at DESC, source_type ASC, identifier ASC"
        if "last_failure_at" in columns
        else "source_type ASC, identifier ASC"
    )
    rows = conn.execute(
        f"""SELECT {', '.join(select)}
            FROM {TABLE}
            {where_sql}
            ORDER BY {order_sql}""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _source_detail(row: Mapping[str, Any], generated_at: datetime) -> dict[str, Any]:
    last_success_at = _clean(row.get("last_success_at"))
    last_failure_at = _clean(row.get("last_failure_at"))
    return {
        "consecutive_failures": _int(row.get("consecutive_failures"), default=1),
        "id": row.get("id"),
        "identifier": _clean(row.get("identifier")) or "",
        "last_error": _clean(row.get("last_error")),
        "last_failure_at": last_failure_at,
        "last_fetch_status": _clean(row.get("last_fetch_status")),
        "last_success_at": last_success_at,
        "name": _clean(row.get("name")),
        "source": _source_key(row),
        "source_type": _clean(row.get("source_type")) or "unknown",
        "status": _clean(row.get("status")),
        "success_age_days": _age_days(generated_at, _parse_datetime(last_success_at)),
    }


def _source_key(row: Mapping[str, Any]) -> str:
    source_type = _clean(row.get("source_type")) or "unknown"
    identifier = _clean(row.get("identifier")) or _clean(row.get("feed_url")) or ""
    if source_type == "x_account":
        return "@" + identifier.lstrip("@").lower() if identifier else "x_account:unknown"
    for value in (identifier, _clean(row.get("feed_url")), _clean(row.get("homepage_url"))):
        host = _host(value)
        if host:
            return host
    return identifier.lower() if identifier else f"{source_type}:unknown"


def _host(value: Any) -> str:
    text = _clean(value)
    if not text:
        return ""
    parsed = urlparse(text if "://" in text else f"https://{text}")
    host = (parsed.netloc or parsed.path.split("/", 1)[0]).lower()
    return host[4:] if host.startswith("www.") else host


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in rows}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _column_expr(columns: set[str], column: str) -> str:
    return column if column in columns else f"NULL AS {column}"


def _digest_sort_key(item: Mapping[str, Any]) -> tuple[int, int, int, str, str]:
    age = item.get("stale_success_age_days")
    return (
        -int(item["failure_count"]),
        -int(age if age is not None else 10_000),
        -int(item["source_count"]),
        str(item.get("last_failure_at") or ""),
        str(item["source"]),
    )


def _source_detail_sort_key(item: Mapping[str, Any]) -> tuple[int, str, str]:
    return (
        -int(item.get("consecutive_failures") or 0),
        str(item.get("last_failure_at") or ""),
        str(item.get("identifier") or ""),
    )


def _mapping(row: Mapping[str, Any]) -> dict[str, Any]:
    return dict(row)


def _contains_any(text: str, markers: tuple[str, ...]) -> bool:
    return any(marker in text for marker in markers)


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    try:
        return _ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _age_days(now: datetime, timestamp: datetime | None) -> int | None:
    if timestamp is None:
        return None
    return max(0, (now - timestamp).days)


def _max_timestamp(left: str | None, right: str | None) -> str | None:
    if not left:
        return right
    if not right:
        return left
    return max(left, right)


def _min_timestamp(left: str | None, right: str | None) -> str | None:
    if not left:
        return right
    if not right:
        return left
    return min(left, right)


def _oldest_success_timestamp(left: str | None, right: str | None) -> str | None:
    if not left:
        return right
    if not right:
        return left
    return min(left, right)
