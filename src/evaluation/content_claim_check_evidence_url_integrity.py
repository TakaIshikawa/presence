"""Audit content claim-check evidence URLs for integrity gaps."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any
from urllib.parse import urlparse


ARTIFACT_TYPE = "content_claim_check_evidence_url_integrity"
DEFAULT_LIMIT = 100
URL_RE = re.compile(r"\b(?:[a-z][a-z0-9+.-]*://|www\.)[^\s<>)\"']+", re.I)
ISSUE_ORDER = (
    "missing_evidence_url",
    "passed_without_evidence",
    "non_http_scheme",
    "duplicate_url",
    "placeholder_domain",
    "malformed_json",
)
PLACEHOLDER_HOSTS = {"example.com", "example.org", "example.net", "localhost", "127.0.0.1", "yourdomain.com", "placeholder.com", "test.com"}
PASSED_STATUSES = {"passed", "pass", "supported", "success", "ok", "complete", "completed"}


def build_content_claim_check_evidence_url_integrity_report(
    rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic report of content claim-check evidence URL issues."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    checks = [_normalize(row) for row in rows]
    findings: list[dict[str, Any]] = []
    for row in checks:
        findings.extend(_row_findings(row))

    findings.sort(key=_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit},
        "totals": {
            "claim_check_count": len(checks),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": _counts_by_issue(findings),
        },
        "findings": shown,
        "status_summaries": _status_summaries(checks, findings),
        "content_summaries": _content_summaries(findings),
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items()) if columns},
        "empty_state": {
            "is_empty": not findings,
            "message": "No content claim-check evidence URL integrity issues found." if not findings else None,
        },
    }


def build_content_claim_check_evidence_url_integrity_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load content claim checks from SQLite and build the integrity report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "content_claim_checks" not in schema:
        return build_content_claim_check_evidence_url_integrity_report([], missing_tables=["content_claim_checks"], **kwargs)
    columns = schema["content_claim_checks"]
    if "content_id" not in columns:
        return build_content_claim_check_evidence_url_integrity_report(
            [],
            missing_columns={"content_claim_checks": ["content_id"]},
            **kwargs,
        )
    return build_content_claim_check_evidence_url_integrity_report(
        _load_rows(conn, schema),
        missing_columns=_optional_schema_gaps(schema),
        **kwargs,
    )


def format_content_claim_check_evidence_url_integrity_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_claim_check_evidence_url_integrity_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    totals = report["totals"]
    lines = [
        "Content Claim Check Evidence URL Integrity",
        f"Generated: {report['generated_at']}",
        f"Limit: {report['filters']['limit']}",
        f"Totals: checks={totals['claim_check_count']} findings={totals['finding_count']} shown={totals['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "status | content_id | issue_type | count"])
    for item in report["content_summaries"]:
        lines.append(f"{item['status']} | {item['content_id']} | {item['issue_type']} | {item['count']}")
    lines.extend(["", "claim_check_id | content_id | status | issue_type | url | source"])
    for item in report["findings"]:
        lines.append(
            f"{_display(item['claim_check_id'])} | {item['content_id']} | {item['status']} | "
            f"{item['issue_type']} | {item['url'] or '-'} | {item['source']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cc = schema["content_claim_checks"]
    gc = schema.get("generated_content", set())
    join = "LEFT JOIN generated_content gc ON gc.id = cc.content_id" if "id" in gc else ""
    select = [
        _qualified(cc, "id", "cc", "cc.content_id") + " AS claim_check_id",
        "cc.content_id AS content_id",
        _qualified(cc, "status", "cc", "NULL") + " AS status",
        _qualified(cc, "passed", "cc", "NULL") + " AS passed",
        _qualified(cc, "supported_count", "cc", "NULL") + " AS supported_count",
        _qualified(cc, "unsupported_count", "cc", "NULL") + " AS unsupported_count",
        _qualified(cc, "annotation_text", "cc", "NULL") + " AS annotation_text",
        _qualified(cc, "metadata", "cc", "NULL") + " AS metadata",
        _qualified(cc, "result", "cc", "NULL") + " AS result",
        _qualified(cc, "evidence", "cc", "NULL") + " AS evidence",
        _qualified(cc, "evidence_url", "cc", "NULL") + " AS evidence_url",
        _qualified(cc, "evidence_urls", "cc", "NULL") + " AS evidence_urls",
        _qualified(gc, "status", "gc", "NULL") + " AS content_status",
    ]
    rows = conn.execute(f"SELECT {', '.join(select)} FROM content_claim_checks cc {join} ORDER BY cc.content_id ASC").fetchall()
    return [dict(row) for row in rows]


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    status = _clean(row.get("status")).lower()
    content_status = _clean(row.get("content_status")).lower()
    if not status:
        status = "passed" if _passed(row) else content_status or "unknown"
    return {
        "claim_check_id": row.get("claim_check_id") or row.get("id") or row.get("content_id"),
        "content_id": _int_or_none(row.get("content_id")),
        "status": status,
        "passed": _passed(row),
        "fields": {
            "annotation_text": row.get("annotation_text"),
            "metadata": row.get("metadata"),
            "result": row.get("result"),
            "evidence": row.get("evidence"),
            "evidence_url": row.get("evidence_url"),
            "evidence_urls": row.get("evidence_urls"),
        },
    }


def _row_findings(row: dict[str, Any]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    urls: list[dict[str, str]] = []
    for field, value in row["fields"].items():
        extracted, malformed = _extract_urls(value, field)
        urls.extend(extracted)
        if malformed:
            findings.append(_finding(row, "malformed_json", None, field, malformed))
    if not urls:
        findings.append(_finding(row, "missing_evidence_url", None, "evidence", "no inspectable evidence URL"))
        if row["passed"]:
            findings.append(_finding(row, "passed_without_evidence", None, "evidence", "claim check passed without inspectable evidence"))
        return findings

    canonical_counts = Counter(_canonical_url(item["url"]) for item in urls)
    duplicate_urls = {url for url, count in canonical_counts.items() if url and count > 1}
    for item in urls:
        url = item["url"]
        parsed = urlparse(url if "://" in url else f"http://{url}")
        if parsed.scheme.lower() not in {"http", "https"}:
            findings.append(_finding(row, "non_http_scheme", url, item["source"], "evidence URL must use http or https"))
        if _placeholder_host(parsed.hostname):
            findings.append(_finding(row, "placeholder_domain", url, item["source"], "replace placeholder evidence domain"))
        if _canonical_url(url) in duplicate_urls:
            findings.append(_finding(row, "duplicate_url", url, item["source"], "duplicate evidence URL within claim check"))
    return findings


def _extract_urls(value: Any, source: str) -> tuple[list[dict[str, str]], str | None]:
    if value is None or _clean(value) == "":
        return [], None
    if source in {"metadata", "result", "evidence", "evidence_urls"}:
        parsed = _json_value(value)
        if isinstance(parsed, str):
            urls = _urls_from_text(_clean(value), source)
            return urls, parsed
        return _urls_from_json(parsed, source), None
    return _urls_from_text(_clean(value), source), None


def _urls_from_json(value: Any, source: str) -> list[dict[str, str]]:
    urls: list[dict[str, str]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            if "url" in str(key).lower() and isinstance(child, str):
                urls.extend(_urls_from_text(child, f"{source}.{key}"))
            else:
                urls.extend(_urls_from_json(child, f"{source}.{key}"))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            urls.extend(_urls_from_json(child, f"{source}[{index}]"))
    elif isinstance(value, str):
        urls.extend(_urls_from_text(value, source))
    return urls


def _urls_from_text(text: str, source: str) -> list[dict[str, str]]:
    return [{"url": match.group(0).rstrip(".,;"), "source": source} for match in URL_RE.finditer(text)]


def _finding(row: dict[str, Any], issue_type: str, url: str | None, source: str, detail: str) -> dict[str, Any]:
    return {
        "claim_check_id": row["claim_check_id"],
        "content_id": row["content_id"],
        "status": row["status"],
        "issue_type": issue_type,
        "url": url,
        "source": source,
        "detail": detail,
    }


def _status_summaries(rows: list[dict[str, Any]], findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    checks = Counter(row["status"] for row in rows)
    issues = Counter(item["status"] for item in findings)
    return [
        {"status": status, "claim_check_count": checks[status], "finding_count": issues[status]}
        for status in sorted(set(checks) | set(issues))
    ]


def _content_summaries(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter((item["status"], item["content_id"], item["issue_type"]) for item in findings)
    return [
        {"status": status, "content_id": content_id, "issue_type": issue_type, "count": count}
        for (status, content_id, issue_type), count in sorted(counts.items(), key=lambda item: (item[0][0], item[0][1] or 0, _rank(item[0][2])))
    ]


def _counts_by_issue(findings: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(item["issue_type"] for item in findings)
    return {issue: counts[issue] for issue in ISSUE_ORDER}


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (_rank(item["issue_type"]), item["status"], item["content_id"] or 0, str(item["url"] or ""))


def _rank(issue_type: str) -> int:
    return ISSUE_ORDER.index(issue_type) if issue_type in ISSUE_ORDER else len(ISSUE_ORDER)


def _json_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, json.JSONDecodeError) as exc:
        return f"{exc}"


def _passed(row: dict[str, Any]) -> bool:
    passed = _bool_or_none(row.get("passed"))
    if passed is not None:
        return passed
    status = _clean(row.get("status")).lower()
    if status in PASSED_STATUSES:
        return True
    supported = _int_or_none(row.get("supported_count")) or 0
    unsupported = _int_or_none(row.get("unsupported_count")) or 0
    return supported > 0 and unsupported == 0


def _canonical_url(value: str) -> str:
    parsed = urlparse(value if "://" in value else f"http://{value}")
    if not parsed.netloc:
        return ""
    return parsed._replace(scheme=parsed.scheme.lower(), netloc=parsed.netloc.lower(), fragment="").geturl().rstrip("/")


def _placeholder_host(host: str | None) -> bool:
    if not host:
        return False
    lower = host.lower()
    return lower in PLACEHOLDER_HOSTS or lower.endswith((".example.com", ".example.org", ".example.net")) or "placeholder" in lower


def _optional_schema_gaps(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    cc = schema["content_claim_checks"]
    missing = [name for name in ("annotation_text", "metadata", "result", "evidence", "evidence_url", "evidence_urls", "status", "passed") if name not in cc]
    gaps = {"content_claim_checks": missing} if missing else {}
    if "generated_content" in schema and "id" not in schema["generated_content"]:
        gaps["generated_content"] = ["id"]
    return gaps


def _qualified(columns: set[str], column: str, alias: str, fallback: str) -> str:
    return f"{alias}.{column}" if column in columns else fallback


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


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "passed", "success", "ok"}:
        return True
    if text in {"0", "false", "no", "failed", "failure"}:
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


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
