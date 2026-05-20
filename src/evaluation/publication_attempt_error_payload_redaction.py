"""Audit failed publication attempts for unredacted error payloads."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


ARTIFACT_TYPE = "publication_attempt_error_payload_redaction"
DEFAULT_DAYS = 7
DEFAULT_LIMIT = 100
DEFAULT_METADATA_BYTES = 2048
SECRET_KEY_RE = re.compile(r"(api[_-]?key|access[_-]?token|refresh[_-]?token|id[_-]?token|client[_-]?secret|oauth|password|secret|authorization)", re.I)
BEARER_RE = re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]{12,}", re.I)
API_KEY_VALUE_RE = re.compile(r"\b(?:api[_-]?key|token|secret|authorization)\s*[:=]\s*['\"]?[A-Za-z0-9._~+/=-]{12,}", re.I)
TOKEN_VALUE_RE = re.compile(r"\b(?:sk|tok|ghp|pat)_[A-Za-z0-9._~+/=-]{12,}\b", re.I)
OAUTH_FIELD_RE = re.compile(r"\b(?:access_token|refresh_token|id_token|client_secret|oauth_token)\b", re.I)
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
FINDING_ORDER = ("secret_key", "secret_value", "oauth_field", "email_address", "oversized_metadata", "malformed_metadata")
SUCCESS_STATUSES = {"success", "succeeded", "published", "ok", "complete", "completed"}
FAILED_STATUSES = {"failed", "failure", "error", "errored", "rejected", "unauthorized", "forbidden", "timeout"}


def build_publication_attempt_error_payload_redaction_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    metadata_bytes: int = DEFAULT_METADATA_BYTES,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic report of likely leaked publication error payloads."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if metadata_bytes <= 0:
        raise ValueError("metadata_bytes must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    attempts = [_normalize(row) for row in rows]
    scoped = [row for row in attempts if row["attempted_at_dt"] is None or row["attempted_at_dt"] >= cutoff]
    failed = [row for row in scoped if _is_failed(row)]
    findings: list[dict[str, Any]] = []
    for row in failed:
        findings.extend(_row_findings(row, metadata_bytes=metadata_bytes))

    findings.sort(key=_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "limit": limit, "metadata_bytes": metadata_bytes},
        "totals": {
            "attempt_count": len(scoped),
            "failed_attempt_count": len(failed),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_finding_type": _counts_by_finding_type(findings),
        },
        "groups": _groups(findings),
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items()) if columns},
        "empty_state": {
            "is_empty": not findings,
            "message": "No publication attempt error payload redaction gaps found." if not findings else None,
        },
    }


def build_publication_attempt_error_payload_redaction_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load publication attempts from SQLite and build the redaction report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    columns = schema.get("publication_attempts")
    if columns is None:
        return build_publication_attempt_error_payload_redaction_report([], missing_tables=["publication_attempts"], **kwargs)

    fatal: list[str] = []
    if "response_metadata" not in columns:
        fatal.append("response_metadata")
    if not {"success", "status", "outcome", "result"} & columns:
        fatal.append("success|status|outcome|result")
    if fatal:
        return build_publication_attempt_error_payload_redaction_report([], missing_columns={"publication_attempts": fatal}, **kwargs)

    optional = _missing_optional(columns)
    return build_publication_attempt_error_payload_redaction_report(
        _load_rows(conn, columns),
        missing_columns={"publication_attempts": optional} if optional else None,
        **kwargs,
    )


def format_publication_attempt_error_payload_redaction_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_attempt_error_payload_redaction_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    totals = report["totals"]
    filters = report["filters"]
    lines = [
        "Publication Attempt Error Payload Redaction",
        f"Generated: {report['generated_at']}",
        f"Filters: days={filters['days']} limit={filters['limit']} metadata_bytes={filters['metadata_bytes']}",
        (
            "Totals: "
            f"attempts={totals['attempt_count']} failed={totals['failed_attempt_count']} "
            f"findings={totals['finding_count']} shown={totals['shown_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "platform | error_category | finding_type | count"])
    for group in report["groups"]:
        lines.append(f"{group['platform']} | {group['error_category']} | {group['finding_type']} | {group['count']}")
    lines.extend(["", "attempt_id | platform | error_category | finding_type | source | path | excerpt"])
    for item in report["findings"]:
        lines.append(
            f"{_display(item['attempt_id'])} | {item['platform']} | {item['error_category']} | "
            f"{item['finding_type']} | {item['source']} | {item['path']} | {item['excerpt'] or '-'}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    attempted = _expr(columns, ("attempted_at", "created_at", "published_at"), "NULL")
    select = [
        _expr(columns, ("id", "attempt_id"), "rowid") + " AS attempt_id",
        _expr(columns, ("content_id",), "NULL") + " AS content_id",
        _expr(columns, ("platform",), "NULL") + " AS platform",
        attempted + " AS attempted_at",
        _expr(columns, ("success",), "NULL") + " AS success",
        _expr(columns, ("status", "outcome", "result"), "NULL") + " AS status",
        _expr(columns, ("error_category", "category"), "NULL") + " AS error_category",
        _expr(columns, ("error", "error_message", "last_error", "message"), "NULL") + " AS error",
        "response_metadata AS response_metadata",
    ]
    order = _expr(columns, ("attempted_at", "created_at", "id"), "rowid")
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM publication_attempts ORDER BY datetime({order}) ASC, rowid ASC")]


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    attempted_at_dt = _parse_timestamp(row.get("attempted_at"))
    return {
        "attempt_id": _int_or_none(row.get("attempt_id") or row.get("id")),
        "content_id": _int_or_none(row.get("content_id")),
        "platform": _clean(row.get("platform")).lower() or "unknown",
        "attempted_at": attempted_at_dt.isoformat() if attempted_at_dt else (_clean(row.get("attempted_at")) or None),
        "attempted_at_dt": attempted_at_dt,
        "success": _bool_or_none(row.get("success")),
        "status": _clean(row.get("status")).lower(),
        "error_category": _clean(row.get("error_category")).lower() or "unknown",
        "error": _clean(row.get("error")),
        "response_metadata": row.get("response_metadata"),
    }


def _is_failed(row: dict[str, Any]) -> bool:
    if row["success"] is True or row["status"] in SUCCESS_STATUSES:
        return False
    if row["success"] is False or row["status"] in FAILED_STATUSES:
        return True
    return bool(row["error"])


def _row_findings(row: dict[str, Any], *, metadata_bytes: int) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    raw = row["response_metadata"]
    if raw is not None and len(str(raw).encode("utf-8")) > metadata_bytes:
        findings.append(_finding(row, "oversized_metadata", "response_metadata", "$", f"{len(str(raw).encode('utf-8'))} bytes"))

    metadata = _parse_metadata(raw)
    if isinstance(metadata, str):
        findings.append(_finding(row, "malformed_metadata", "response_metadata", "$", metadata))
    elif isinstance(metadata, dict):
        for path, key, value in _walk(metadata):
            key_text = str(key)
            value_text = _clean(value)
            if SECRET_KEY_RE.search(key_text):
                findings.append(_finding(row, "secret_key", "response_metadata", path, _redact_key(key_text)))
            if OAUTH_FIELD_RE.search(key_text):
                findings.append(_finding(row, "oauth_field", "response_metadata", path, _redact_key(key_text)))
            if not isinstance(value, (dict, list)):
                findings.extend(_text_findings(row, value_text, "response_metadata", path))
    findings.extend(_text_findings(row, row["error"], "error", "$"))
    return findings


def _text_findings(row: dict[str, Any], text: str, source: str, path: str) -> list[dict[str, Any]]:
    if not text:
        return []
    findings = []
    if BEARER_RE.search(text) or API_KEY_VALUE_RE.search(text) or TOKEN_VALUE_RE.search(text):
        findings.append(_finding(row, "secret_value", source, path, _redact_excerpt(text)))
    if OAUTH_FIELD_RE.search(text):
        findings.append(_finding(row, "oauth_field", source, path, _redact_excerpt(text)))
    if EMAIL_RE.search(text):
        findings.append(_finding(row, "email_address", source, path, _redact_excerpt(text)))
    return findings


def _finding(row: dict[str, Any], finding_type: str, source: str, path: str, excerpt: str) -> dict[str, Any]:
    return {
        "attempt_id": row["attempt_id"],
        "content_id": row["content_id"],
        "platform": row["platform"],
        "error_category": row["error_category"],
        "attempted_at": row["attempted_at"],
        "finding_type": finding_type,
        "source": source,
        "path": path,
        "excerpt": excerpt[:160],
    }


def _parse_metadata(raw: Any) -> dict[str, Any] | str | None:
    if raw is None or _clean(raw) == "":
        return None
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(str(raw))
    except (TypeError, json.JSONDecodeError) as exc:
        return f"response_metadata is not valid JSON: {exc}"
    return parsed if isinstance(parsed, dict) else "response_metadata must be a JSON object"


def _walk(value: Any, path: str = "$") -> list[tuple[str, Any, Any]]:
    items: list[tuple[str, Any, Any]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            items.append((child_path, key, child))
            items.extend(_walk(child, child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            items.extend(_walk(child, f"{path}[{index}]"))
    return items


def _groups(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter((item["platform"], item["error_category"], item["finding_type"]) for item in findings)
    return [
        {"platform": platform, "error_category": category, "finding_type": kind, "count": count}
        for (platform, category, kind), count in sorted(counts.items(), key=lambda item: (item[0][0], item[0][1], _rank(item[0][2])))
    ]


def _counts_by_finding_type(findings: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(item["finding_type"] for item in findings)
    return {kind: counts[kind] for kind in FINDING_ORDER}


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (_rank(item["finding_type"]), item["platform"], item["error_category"], item["attempt_id"] or 0, item["source"], item["path"])


def _rank(kind: str) -> int:
    return FINDING_ORDER.index(kind) if kind in FINDING_ORDER else len(FINDING_ORDER)


def _missing_optional(columns: set[str]) -> list[str]:
    missing = []
    for name in ("id", "content_id", "platform", "error", "error_category"):
        if name not in columns:
            missing.append(name)
    if not {"attempted_at", "created_at", "published_at"} & columns:
        missing.append("attempted_at|created_at|published_at")
    return missing


def _expr(columns: set[str], choices: tuple[str, ...], fallback: str) -> str:
    for column in choices:
        if column in columns:
            return column
    return fallback


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


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


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "success", "succeeded"}:
        return True
    if text in {"0", "false", "no", "failed", "failure", "error"}:
        return False
    return None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _display(value: Any) -> str:
    return "-" if value is None or value == "" else str(value)


def _redact_key(value: str) -> str:
    return f"{value[:2]}...{value[-2:]}" if len(value) > 4 else "<redacted>"


def _redact_excerpt(value: str) -> str:
    text = BEARER_RE.sub("Bearer <redacted>", value)
    text = API_KEY_VALUE_RE.sub(lambda match: match.group(0).split(match.group(0)[-12:])[0] + "<redacted>", text)
    text = TOKEN_VALUE_RE.sub("<redacted>", text)
    text = EMAIL_RE.sub("<email>", text)
    return " ".join(text.split())[:160]


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
