"""Cluster repeated publication attempt authentication failures."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_HOURS = 24
DEFAULT_MIN_ATTEMPTS = 2
ARTIFACT_TYPE = "publication_attempt_auth_failure_clusters"

AUTH_STATUS_WORDS = {"auth", "authentication", "authorization", "unauthorized", "forbidden"}
AUTH_CODE_WORDS = {
    "401",
    "403",
    "auth",
    "auth_failed",
    "authentication_failed",
    "authorization_failed",
    "unauthorized",
    "forbidden",
    "invalid_token",
    "expired_token",
    "invalid_credentials",
    "permission_denied",
}
AUTH_MESSAGE_RE = re.compile(
    r"\b(401|403|unauthori[sz]ed|forbidden|invalid token|expired token|access token|credential|permission denied|oauth|auth(?:entication|ori[sz]ation)?)\b",
    re.IGNORECASE,
)
FAILED_STATUSES = {"failed", "failure", "error", "errored", "rejected", "unauthorized", "forbidden"}
SUCCESS_STATUSES = {"success", "succeeded", "published", "ok", "complete", "completed"}

_URL_RE = re.compile(r"\b(?:[a-z][a-z0-9+.-]*://|www\.)\S+", re.IGNORECASE)
_UUID_RE = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b", re.IGNORECASE)
_TOKEN_RE = re.compile(r"\b[A-Za-z0-9_-]{16,}\b")
_NUMBER_RE = re.compile(r"\b\d+\b")
_SPACE_RE = re.compile(r"\s+")


def build_publication_attempt_auth_failure_clusters_report(
    rows: list[dict[str, Any]],
    *,
    min_attempts: int = DEFAULT_MIN_ATTEMPTS,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic report of repeated auth failures by platform/signature."""
    if min_attempts <= 0:
        raise ValueError("min_attempts must be positive")
    if lookback_hours <= 0:
        raise ValueError("lookback_hours must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(hours=lookback_hours)
    attempts = []
    for row in rows:
        attempt = _normalize_attempt(row)
        if attempt["attempted_at_dt"] is None or attempt["attempted_at_dt"] < cutoff:
            continue
        attempts.append(attempt)

    auth_attempts = [attempt for attempt in attempts if _is_auth_failure(attempt)]
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for attempt in auth_attempts:
        grouped[(attempt["platform"], attempt["auth_signature"])].append(attempt)

    findings = []
    for (platform, signature), group in grouped.items():
        if len(group) < min_attempts:
            continue
        group.sort(key=lambda item: (item["attempted_at_dt"], item["attempt_id"] or 0))
        content_ids = sorted({item["content_id"] for item in group if item["content_id"] is not None})
        findings.append(
            {
                "platform": platform,
                "auth_signature": signature,
                "attempt_count": len(group),
                "distinct_content_count": len(content_ids),
                "content_ids": content_ids[:20],
                "first_attempted_at": group[0]["attempted_at"],
                "last_attempted_at": group[-1]["attempted_at"],
                "latest_attempt_id": group[-1]["attempt_id"],
                "latest_error_code": group[-1]["error_code"],
                "latest_error_message": group[-1]["error_message"],
                "statuses": sorted({item["status"] for item in group if item["status"]}),
            }
        )

    findings.sort(key=lambda item: (-item["attempt_count"], -item["distinct_content_count"], item["platform"], item["auth_signature"]))
    counts = Counter(item["platform"] for item in auth_attempts)
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"lookback_hours": lookback_hours, "min_attempts": min_attempts},
        "summary": {
            "attempt_count": len(attempts),
            "auth_failure_attempt_count": len(auth_attempts),
            "finding_count": len(findings),
            "by_platform": dict(sorted(counts.items())),
        },
        "findings": findings,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items()) if columns},
        "empty_state": {
            "is_empty": not findings,
            "message": "No repeated publication attempt auth failure clusters found." if not findings else None,
        },
    }


def build_publication_attempt_auth_failure_clusters_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load publication attempts from SQLite and build the auth failure cluster report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "publication_attempts" not in schema:
        return build_publication_attempt_auth_failure_clusters_report([], missing_tables=["publication_attempts"], **kwargs)
    columns = schema["publication_attempts"]
    required = {"platform", "attempted_at"}
    error_columns = {"status", "error_code", "error_message", "error", "error_category", "last_error", "message"}
    missing: list[str] = sorted(required - columns)
    if not error_columns & columns:
        missing.append("status|error_code|error_message|error|error_category|last_error|message")
    if missing:
        return build_publication_attempt_auth_failure_clusters_report([], missing_columns={"publication_attempts": missing}, **kwargs)
    return build_publication_attempt_auth_failure_clusters_report(_load_rows(conn, columns), **kwargs)


def format_publication_attempt_auth_failure_clusters_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_attempt_auth_failure_clusters_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    summary = report["summary"]
    lines = [
        "Publication Attempt Auth Failure Clusters",
        f"Generated: {report['generated_at']}",
        f"Filters: lookback_hours={report['filters']['lookback_hours']} min_attempts={report['filters']['min_attempts']}",
        (
            "Totals: "
            f"attempts={summary['attempt_count']} "
            f"auth_failures={summary['auth_failure_attempt_count']} "
            f"findings={summary['finding_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "platform | signature | attempts | content | first_attempted_at | last_attempted_at"])
    for item in report["findings"]:
        lines.append(
            f"{item['platform']} | {item['auth_signature']} | {item['attempt_count']} | "
            f"{item['distinct_content_count']} | {item['first_attempted_at']} | {item['last_attempted_at']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        _expr(columns, "id", "attempt_id", "NULL"),
        _expr(columns, "content_id", "content_id", "NULL"),
        "platform AS platform",
        "attempted_at AS attempted_at",
        _expr(columns, "status", "status", "NULL"),
        _expr(columns, "success", "success", "NULL"),
        _expr(columns, "error_code", "error_code", "NULL"),
        _expr(columns, "error_message", "error_message", "NULL"),
        _expr(columns, "error", "error", "NULL"),
        _expr(columns, "error_category", "error_category", "NULL"),
        _expr(columns, "last_error", "last_error", "NULL"),
        _expr(columns, "message", "message", "NULL"),
    ]
    order = _expr(columns, "id", "id", "rowid").rsplit(" AS ", 1)[0]
    return [
        dict(row)
        for row in conn.execute(
            f"SELECT {', '.join(select)} FROM publication_attempts ORDER BY datetime(attempted_at) ASC, {order} ASC"
        )
    ]


def _normalize_attempt(row: dict[str, Any]) -> dict[str, Any]:
    error_message = _clean(_first(row, "error_message", "error", "last_error", "message"))
    error_code = _clean(_first(row, "error_code", "error_category"))
    status = _clean(_first(row, "status")).lower()
    attempted_at_dt = _parse_timestamp(_first(row, "attempted_at", "created_at"))
    attempted_at = attempted_at_dt.isoformat() if attempted_at_dt else _clean(_first(row, "attempted_at", "created_at")) or None
    return {
        "attempt_id": _int_or_none(_first(row, "attempt_id", "id")),
        "content_id": _int_or_none(row.get("content_id")),
        "platform": _clean(row.get("platform")).lower() or "unknown",
        "attempted_at": attempted_at,
        "attempted_at_dt": attempted_at_dt,
        "success": _bool_or_none(row.get("success")),
        "status": status,
        "error_code": error_code or None,
        "error_message": error_message or None,
        "auth_signature": _auth_signature(error_code, error_message, status),
    }


def _is_auth_failure(attempt: dict[str, Any]) -> bool:
    if attempt["success"] is True or attempt["status"] in SUCCESS_STATUSES:
        return False
    status_hit = any(word in attempt["status"] for word in AUTH_STATUS_WORDS)
    code = _clean(attempt["error_code"]).lower()
    code_hit = code in AUTH_CODE_WORDS or any(word in code for word in AUTH_CODE_WORDS if word not in {"401", "403"})
    message_hit = bool(AUTH_MESSAGE_RE.search(_clean(attempt["error_message"])))
    failed = attempt["success"] is False or attempt["status"] in FAILED_STATUSES or bool(code or attempt["error_message"])
    return failed and (status_hit or code_hit or message_hit)


def _auth_signature(error_code: str, error_message: str, status: str) -> str:
    source = error_message or error_code or status or "auth failure"
    text = source.lower()
    text = _URL_RE.sub("<url>", text)
    text = _UUID_RE.sub("<id>", text)
    text = _TOKEN_RE.sub("<token>", text)
    text = _NUMBER_RE.sub("<id>", text)
    text = _SPACE_RE.sub(" ", text).strip(" .")
    return text or "auth failure"


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
    return _utc(parsed)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({_quote_identifier(str(row[0]))})")} for row in rows}


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{_quote_identifier(column)} AS {output}" if column in columns else f"{default} AS {output}"


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    text = _clean(value).lower()
    if text in {"1", "true", "yes", "success", "succeeded"}:
        return True
    if text in {"0", "false", "no", "failed", "failure"}:
        return False
    return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
