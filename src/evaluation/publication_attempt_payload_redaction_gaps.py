"""Find likely redaction gaps in stored publication attempt payloads."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any
from urllib.parse import parse_qsl, urlsplit


ARTIFACT_TYPE = "publication_attempt_payload_redaction_gaps"
DEFAULT_LIMIT = 100
REDACTED_RE = re.compile(r"(\[redacted\]|<redacted>|\*\*\*|REDACTED|xxxxx|••••)", re.I)
SECRET_FIELD_RE = re.compile(r"(authorization|api[_-]?key|access[_-]?token|refresh[_-]?token|id[_-]?token|client[_-]?secret|password|secret|signature)", re.I)
BEARER_RE = re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]{16,}", re.I)
TOKEN_RE = re.compile(r"\b(?:sk|tok|ghp|pat|xox[baprs])[_-][A-Za-z0-9._~+/=-]{16,}\b", re.I)
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.I)
QUERY_SECRET_NAMES = {"access_token", "refresh_token", "id_token", "api_key", "apikey", "client_secret", "signature", "auth", "token"}


def build_publication_attempt_payload_redaction_gaps_report(
    rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a deterministic redaction gap report from publication attempt rows."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    findings: list[dict[str, Any]] = []
    for row in rows:
        normalized = _normalize(row)
        for field in ("error", "request_payload", "response_payload", "request_headers", "response_headers", "url"):
            findings.extend(_scan_value(normalized, field, normalized.get(field), field))
    findings = _dedupe(findings)
    findings.sort(key=lambda item: (_severity_rank(item["severity"]), str(item["attempt_id"]), item["platform"], item["field_path"], item["matched_kind"]))
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit},
        "totals": {"attempt_count": len(rows), "finding_count": len(findings), "shown_count": len(shown)},
        "rows": shown,
        "empty_state": {
            "is_empty": not findings,
            "message": "No publication attempt payload redaction gaps found." if not findings else None,
        },
    }


def build_publication_attempt_payload_redaction_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    columns = _schema(conn).get("publication_attempts")
    if columns is None:
        return build_publication_attempt_payload_redaction_gaps_report([], **kwargs) | {"missing_tables": ["publication_attempts"], "missing_columns": {}}
    select = [
        _expr(columns, ("id", "attempt_id"), "rowid") + " AS attempt_id",
        _expr(columns, ("platform",), "NULL") + " AS platform",
        _expr(columns, ("error", "error_message", "last_error"), "NULL") + " AS error",
        _expr(columns, ("request_payload", "request_body", "request_metadata"), "NULL") + " AS request_payload",
        _expr(columns, ("response_payload", "response_body", "response_metadata"), "NULL") + " AS response_payload",
        _expr(columns, ("request_headers", "headers"), "NULL") + " AS request_headers",
        _expr(columns, ("response_headers",), "NULL") + " AS response_headers",
        _expr(columns, ("url", "request_url", "endpoint"), "NULL") + " AS url",
    ]
    rows = [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM publication_attempts ORDER BY rowid ASC")]
    report = build_publication_attempt_payload_redaction_gaps_report(rows, **kwargs)
    report["missing_tables"] = []
    report["missing_columns"] = {}
    return report


def format_publication_attempt_payload_redaction_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_attempt_payload_redaction_gaps_text(report: dict[str, Any]) -> str:
    lines = [
        "Publication Attempt Payload Redaction Gaps",
        f"Generated: {report['generated_at']}",
        f"Findings: {report['totals']['finding_count']} shown={report['totals']['shown_count']}",
    ]
    rows = report["rows"]
    if not rows:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("attempt_id | platform | severity | matched_kind | field_path | redaction_issue")
    for row in rows:
        lines.append(
            f"{row['attempt_id']} | {row['platform']} | {row['severity']} | {row['matched_kind']} | "
            f"{row['field_path']} | {row['redaction_issue']}"
        )
    return "\n".join(lines)


def _scan_value(row: dict[str, Any], root: str, value: Any, path: str) -> list[dict[str, Any]]:
    if value in (None, ""):
        return []
    if isinstance(value, (dict, list)):
        return [finding for child_path, child in _walk(value, path) for finding in _scan_scalar(row, child_path, child)]
    parsed = _parse_json(value)
    if isinstance(parsed, (dict, list)):
        return [finding for child_path, child in _walk(parsed, path) for finding in _scan_scalar(row, child_path, child)]
    return _scan_scalar(row, path, value)


def _scan_scalar(row: dict[str, Any], path: str, value: Any) -> list[dict[str, Any]]:
    text = str(value)
    if not text or REDACTED_RE.search(text):
        return []
    findings: list[dict[str, Any]] = []
    key = path.rsplit(".", 1)[-1].strip("[]").lower()
    if SECRET_FIELD_RE.search(key) and len(text) >= 8:
        findings.append(_row(row, path, "sensitive field value is stored without a redaction placeholder", "secret_field", "high"))
    if BEARER_RE.search(text):
        findings.append(_row(row, path, "bearer credential appears in stored text", "bearer_token", "critical"))
    if TOKEN_RE.search(text):
        findings.append(_row(row, path, "provider token appears in stored text", "provider_token", "critical"))
    if EMAIL_RE.search(text):
        findings.append(_row(row, path, "email address appears in stored diagnostics or payload", "email_address", "medium"))
    for url in URL_RE.findall(text):
        findings.extend(_scan_url(row, path, url))
    if path == "url":
        findings.extend(_scan_url(row, path, text))
    return findings


def _scan_url(row: dict[str, Any], path: str, url: str) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    try:
        query = parse_qsl(urlsplit(url).query, keep_blank_values=True)
    except ValueError:
        return findings
    for name, value in query:
        if name.lower() in QUERY_SECRET_NAMES and value and not REDACTED_RE.search(value):
            findings.append(_row(row, f"{path}.query.{name}", "sensitive URL query parameter is not redacted", "url_query_secret", "high"))
    return findings


def _row(row: dict[str, Any], field_path: str, issue: str, kind: str, severity: str) -> dict[str, Any]:
    return {
        "attempt_id": row["attempt_id"],
        "platform": row["platform"],
        "field_path": field_path,
        "redaction_issue": issue,
        "matched_kind": kind,
        "severity": severity,
    }


def _walk(value: Any, path: str):
    if isinstance(value, dict):
        for key in sorted(value):
            yield from _walk(value[key], f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _walk(item, f"{path}[{index}]")
    else:
        yield path, value


def _parse_json(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except ValueError:
        return value


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "attempt_id": row.get("attempt_id") or row.get("id"),
        "platform": str(row.get("platform") or "unknown").lower(),
        **row,
    }


def _dedupe(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    deduped = []
    for row in rows:
        key = (row["attempt_id"], row["platform"], row["field_path"], row["matched_kind"])
        if key not in seen:
            seen.add(key)
            deduped.append(row)
    return deduped


def _severity_rank(severity: str) -> int:
    return {"critical": 0, "high": 1, "medium": 2, "low": 3}.get(severity, 9)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    if isinstance(db_or_conn, sqlite3.Connection):
        db_or_conn.row_factory = sqlite3.Row
        return db_or_conn
    conn = sqlite3.connect(db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _expr(columns: set[str], names: tuple[str, ...], fallback: str) -> str:
    return next((name for name in names if name in columns), fallback)
