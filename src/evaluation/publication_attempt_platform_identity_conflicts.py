"""Report platform identity conflicts in successful publication attempts."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_LOOKBACK_DAYS = 30
ARTIFACT_TYPE = "publication_attempt_platform_identity_conflicts"
SUCCESS_STATUSES = {"ok", "published", "sent", "success", "succeeded"}
SUCCESS_COLUMNS = {"success", "status", "outcome", "result"}
REQUIRED_COLUMNS = {"content_id", "platform"}
OPTIONAL_COLUMNS = ("id", "attempted_at", "created_at", "published_at", "platform_post_id", "post_id", "platform_url", "url", "response_metadata")
REASONS = ("duplicate_platform_post_id", "conflicting_success_identity", "metadata_identity_mismatch")
POST_ID_KEYS = ("platform_post_id", "post_id", "tweet_id", "tweetId", "id", "uri", "at_uri")
URL_KEYS = ("platform_url", "post_url", "url", "permalink", "link")


def build_publication_attempt_platform_identity_conflicts_report(
    attempt_rows: list[dict[str, Any]],
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | str | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic platform identity conflict report from attempt rows."""
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _coerce_ts(now) if now is not None else datetime.now(timezone.utc)
    cutoff = generated_at - timedelta(days=lookback_days)
    successful = []
    scanned = 0
    for raw in attempt_rows:
        row = _normalize(raw)
        attempted_at = _parse_ts(row["attempted_at"])
        if attempted_at is not None and attempted_at < cutoff:
            continue
        scanned += 1
        if _is_success(row):
            successful.append(row)

    findings = _duplicate_post_id_findings(successful)
    findings.extend(_conflicting_success_identity_findings(successful))
    findings.extend(_metadata_mismatch_findings(successful))
    findings.sort(key=_sort_key)
    shown = findings[:limit]
    counts = Counter(item["reason"] for item in findings)
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"lookback_days": lookback_days, "limit": limit},
        "totals": {
            "attempt_count": scanned,
            "successful_attempt_count": len(successful),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_reason": {reason: counts[reason] for reason in REASONS},
        },
        "findings": _group_findings(shown),
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items()) if columns},
        "empty_state": {
            "is_empty": not findings,
            "message": "No publication attempt platform identity conflicts found." if not findings else None,
        },
    }


def build_publication_attempt_platform_identity_conflicts_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load publication attempts from SQLite and build the identity conflict report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    columns = schema.get("publication_attempts")
    if columns is None:
        return build_publication_attempt_platform_identity_conflicts_report(
            [],
            missing_tables=["publication_attempts"],
            **kwargs,
        )

    missing_required = sorted(REQUIRED_COLUMNS - columns)
    if not columns.intersection(SUCCESS_COLUMNS):
        missing_required.append("success/status")
    optional_missing = [column for column in OPTIONAL_COLUMNS if column not in columns]
    missing_columns = {"publication_attempts": missing_required + optional_missing} if missing_required or optional_missing else {}
    rows = [] if missing_required else _load_rows(conn, columns)
    return build_publication_attempt_platform_identity_conflicts_report(rows, missing_columns=missing_columns, **kwargs)


def format_publication_attempt_platform_identity_conflicts_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_attempt_platform_identity_conflicts_text(report: dict[str, Any]) -> str:
    """Render the report as terminal-friendly text."""
    totals = report["totals"]
    lines = [
        "Publication Attempt Platform Identity Conflicts",
        f"Generated: {report['generated_at']}",
        f"Filters: lookback_days={report['filters']['lookback_days']} limit={report['filters']['limit']}",
        (
            "Totals: "
            f"attempts={totals['attempt_count']} "
            f"successful_attempts={totals['successful_attempt_count']} "
            f"findings={totals['finding_count']} "
            f"shown={totals['shown_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "reason | content_id | platform | platform_post_id | platform_url | attempts | detail"])
    for group in report["findings"]:
        for item in group["items"]:
            lines.append(
                f"{item['reason']} | {_display(item.get('content_id'))} | {_display(item.get('platform'))} | "
                f"{_display(item.get('platform_post_id'))} | {_display(item.get('platform_url'))} | "
                f"{len(item.get('attempts', []))} | {_detail(item)}"
            )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    attempted_at = _column_expr(columns, "attempted_at", "created_at", "published_at", fallback="NULL")
    selected = [
        f"{_column_expr(columns, 'id', fallback='NULL')} AS attempt_id",
        "content_id",
        "platform",
        f"{attempted_at} AS attempted_at",
        f"{_column_expr(columns, 'platform_post_id', 'post_id', fallback='NULL')} AS platform_post_id",
        f"{_column_expr(columns, 'platform_url', 'url', fallback='NULL')} AS platform_url",
        f"{_column_expr(columns, 'success', fallback='NULL')} AS success",
        f"{_column_expr(columns, 'status', 'outcome', 'result', fallback='NULL')} AS status",
        f"{_column_expr(columns, 'response_metadata', fallback='NULL')} AS response_metadata",
    ]
    return [
        dict(row)
        for row in conn.execute(
            f"SELECT {', '.join(selected)} FROM publication_attempts ORDER BY datetime(COALESCE({attempted_at}, '1970-01-01')) ASC, attempt_id ASC"
        ).fetchall()
    ]


def _duplicate_post_id_findings(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row["platform_post_id"]:
            grouped[(row["platform"], row["platform_post_id"])].append(row)
    findings = []
    for (platform, post_id), group in grouped.items():
        content_ids = sorted({row["content_id"] for row in group if row["content_id"]}, key=_int_or_text)
        if len(content_ids) > 1:
            findings.append(
                _finding(
                    "duplicate_platform_post_id",
                    platform=platform,
                    platform_post_id=post_id,
                    content_ids=content_ids,
                    attempts=group,
                )
            )
    return findings


def _conflicting_success_identity_findings(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["content_id"], row["platform"])].append(row)
    findings = []
    for (content_id, platform), group in grouped.items():
        post_ids = _distinct(group, "platform_post_id")
        urls = _distinct(group, "platform_url")
        if len(post_ids) > 1 or len(urls) > 1:
            findings.append(
                _finding(
                    "conflicting_success_identity",
                    content_id=content_id,
                    platform=platform,
                    platform_post_ids=post_ids,
                    platform_urls=urls,
                    attempts=group,
                )
            )
    return findings


def _metadata_mismatch_findings(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    findings = []
    for row in rows:
        metadata = _metadata_object(row.get("response_metadata"))
        if not metadata:
            continue
        metadata_post_id = _first_metadata_value(metadata, POST_ID_KEYS)
        metadata_url = _first_metadata_value(metadata, URL_KEYS)
        mismatches = []
        if row["platform_post_id"] and metadata_post_id and row["platform_post_id"] != metadata_post_id:
            mismatches.append("platform_post_id")
        if row["platform_url"] and metadata_url and row["platform_url"] != metadata_url:
            mismatches.append("platform_url")
        if mismatches:
            findings.append(
                _finding(
                    "metadata_identity_mismatch",
                    content_id=row["content_id"],
                    platform=row["platform"],
                    platform_post_id=row["platform_post_id"] or None,
                    platform_url=row["platform_url"] or None,
                    metadata_platform_post_id=metadata_post_id,
                    metadata_platform_url=metadata_url,
                    mismatch_fields=mismatches,
                    attempts=[row],
                )
            )
    return findings


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "attempt_id": row.get("attempt_id", row.get("id")),
        "content_id": _clean(row.get("content_id")),
        "platform": _clean(row.get("platform")).lower() or "unknown",
        "attempted_at": _clean(row.get("attempted_at") or row.get("created_at") or row.get("published_at")) or None,
        "platform_post_id": _clean(row.get("platform_post_id") or row.get("post_id")),
        "platform_url": _clean(row.get("platform_url") or row.get("url")),
        "success": row.get("success"),
        "status": row.get("status") or row.get("outcome") or row.get("result"),
        "response_metadata": row.get("response_metadata"),
    }


def _finding(reason: str, **values: Any) -> dict[str, Any]:
    attempts = sorted(values.pop("attempts", []), key=_attempt_sort_key)
    return {
        "reason": reason,
        "content_id": values.pop("content_id", None),
        "content_ids": values.pop("content_ids", None),
        "platform": values.pop("platform", None),
        "platform_post_id": values.pop("platform_post_id", None),
        "platform_url": values.pop("platform_url", None),
        "attempts": [_attempt_summary(row) for row in attempts],
        **values,
    }


def _attempt_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "attempt_id": row.get("attempt_id"),
        "content_id": row.get("content_id"),
        "platform": row.get("platform"),
        "attempted_at": row.get("attempted_at"),
        "platform_post_id": row.get("platform_post_id") or None,
        "platform_url": row.get("platform_url") or None,
    }


def _group_findings(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[item["reason"]].append(item)
    return [{"reason": reason, "count": len(grouped[reason]), "items": grouped[reason]} for reason in REASONS if reason in grouped]


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        REASONS.index(item["reason"]),
        _clean(item.get("platform")),
        _int_or_text(item.get("content_id") or (item.get("content_ids") or [""])[0]),
        _clean(item.get("platform_post_id") or ""),
        _clean(item.get("platform_url") or ""),
    )


def _attempt_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (_clean(row.get("attempted_at")), _int_or_text(row.get("attempt_id")))


def _is_success(row: dict[str, Any]) -> bool:
    if _clean(row.get("success")).lower() in {"1", "true", "yes"}:
        return True
    return _clean(row.get("status")).lower() in SUCCESS_STATUSES


def _metadata_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if raw is None or _clean(raw) == "":
        return {}
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _first_metadata_value(metadata: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key, value in metadata.items():
        if key in keys:
            found = _metadata_scalar(value, keys)
            if found:
                return found
        if isinstance(value, dict):
            found = _first_metadata_value(value, keys)
            if found:
                return found
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    found = _first_metadata_value(item, keys)
                    if found:
                        return found
    return None


def _metadata_scalar(value: Any, keys: tuple[str, ...]) -> str | None:
    if isinstance(value, dict):
        return _first_metadata_value(value, keys)
    if isinstance(value, (list, tuple)):
        for item in value:
            if _clean(item):
                return _clean(item)
        return None
    return _clean(value) or None


def _distinct(rows: list[dict[str, Any]], key: str) -> list[str]:
    return sorted({_clean(row.get(key)) for row in rows if _clean(row.get(key))}, key=_int_or_text)


def _parse_ts(value: Any) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    for parser in (
        lambda raw: datetime.fromisoformat(raw.replace("Z", "+00:00")),
        lambda raw: datetime.strptime(raw, "%Y-%m-%d %H:%M:%S"),
        lambda raw: datetime.strptime(raw, "%Y-%m-%d"),
    ):
        try:
            return _utc(parser(text))
        except ValueError:
            continue
    return None


def _coerce_ts(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return _utc(value)
    parsed = _parse_ts(value)
    if parsed is None:
        raise ValueError(f"invalid timestamp: {value}")
    return parsed


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({_quote(str(row[0]))})")} for row in rows}


def _column_expr(columns: set[str], *columns_to_try: str, fallback: str) -> str:
    for column in columns_to_try:
        if column in columns:
            return _quote(column)
    return fallback


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _detail(item: dict[str, Any]) -> str:
    if item["reason"] == "duplicate_platform_post_id":
        return "content_ids=" + ",".join(item.get("content_ids") or [])
    if item["reason"] == "conflicting_success_identity":
        return "post_ids=" + ",".join(item.get("platform_post_ids") or []) + " urls=" + ",".join(item.get("platform_urls") or [])
    return "mismatch_fields=" + ",".join(item.get("mismatch_fields") or [])


def _display(value: Any) -> str:
    return "-" if value is None or value == "" else str(value)


def _quote(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_text(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, "" if value is None else str(value))


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
