"""Preflight newsletter sends and drafts for deliverability risks."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 25
LINK_HEAVY_THRESHOLD = 12

SPAMMY_TERMS = (
    "act now",
    "buy now",
    "cash",
    "free",
    "guarantee",
    "limited time",
    "risk-free",
    "urgent",
    "winner",
)
FOOTER_MARKERS = ("copyright", "preferences", "manage your subscription", "newsletter")
UNSUBSCRIBE_MARKERS = ("unsubscribe", "opt out", "manage preferences")
URL_RE = re.compile(r"""(?i)\bhttps?://[^\s<>"')]+|href\s*=\s*["']([^"']+)["']""")


@dataclass(frozen=True)
class NewsletterDeliverabilityIssue:
    """Deliverability preflight result for one newsletter row."""

    issue_id: str
    subject: str
    status: str
    timestamp: str
    risk_score: int
    warnings: tuple[str, ...]
    link_count: int
    subject_flags: tuple[str, ...]
    source_count: int

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["warnings"] = list(self.warnings)
        payload["subject_flags"] = list(self.subject_flags)
        return payload


@dataclass(frozen=True)
class NewsletterDeliverabilityPreflightReport:
    """Newsletter deliverability preflight report."""

    days: int
    limit: int
    generated_at: str
    total_rows_inspected: int
    risky_rows: int
    issues: tuple[NewsletterDeliverabilityIssue, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_deliverability_preflight",
            "days": self.days,
            "generated_at": self.generated_at,
            "issues": [issue.to_dict() for issue in self.issues],
            "limit": self.limit,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "risky_rows": self.risky_rows,
            "total_rows_inspected": self.total_rows_inspected,
        }


def build_newsletter_deliverability_preflight_report(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
) -> NewsletterDeliverabilityPreflightReport:
    """Inspect recent newsletter sends/drafts for common deliverability risks."""
    period_days = int(days)
    if period_days <= 0:
        raise ValueError("days must be positive")
    row_limit = int(limit)
    if row_limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = datetime.now(timezone.utc).isoformat()
    conn = _connection(db)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return NewsletterDeliverabilityPreflightReport(
            days=period_days,
            limit=row_limit,
            generated_at=generated_at,
            total_rows_inspected=0,
            risky_rows=0,
            issues=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_newsletter_rows(conn, schema["newsletter_sends"], days=period_days, limit=row_limit)
    issues = tuple(_preflight_issue(row) for row in rows)
    return NewsletterDeliverabilityPreflightReport(
        days=period_days,
        limit=row_limit,
        generated_at=generated_at,
        total_rows_inspected=len(issues),
        risky_rows=sum(1 for issue in issues if issue.warnings),
        issues=issues,
    )


def format_newsletter_deliverability_preflight_json(
    report: NewsletterDeliverabilityPreflightReport,
) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_deliverability_preflight_text(
    report: NewsletterDeliverabilityPreflightReport,
) -> str:
    """Render a compact human-readable preflight report."""
    lines = [
        "Newsletter Deliverability Preflight",
        f"Period: last {report.days} days",
        f"Limit: {report.limit}",
        (
            "Summary: "
            f"{report.total_rows_inspected} rows inspected, "
            f"{report.risky_rows} rows with warnings"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        formatted = [
            f"{table}.{column}"
            for table, columns in sorted(report.missing_columns.items())
            for column in columns
        ]
        lines.append("Missing columns: " + ", ".join(formatted))
    if not report.issues:
        lines.append("No newsletter rows found.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Rows:")
    for issue in report.issues:
        subject = issue.subject or "untitled"
        warning_text = ", ".join(issue.warnings) if issue.warnings else "clean"
        lines.append(
            f"- {issue.issue_id or 'unknown'} [{issue.status or 'unknown'}] "
            f"{subject}; risk={issue.risk_score}; links={issue.link_count}; "
            f"sources={issue.source_count}; warnings={warning_text}"
        )
    return "\n".join(lines)


def _preflight_issue(row: Mapping[str, Any]) -> NewsletterDeliverabilityIssue:
    subject = str(row.get("subject") or "")
    body_text = _body_text(row)
    source_ids, source_warnings = _parse_source_content_ids(row.get("source_content_ids"))
    subject_flags = tuple(_subject_flags(subject))
    link_count = _link_count(body_text)
    warnings: list[str] = []
    warnings.extend(subject_flags)
    warnings.extend(source_warnings)
    if not source_ids:
        warnings.append("missing_source_content_ids")
    if not body_text.strip():
        warnings.append("empty_body")
    else:
        if link_count > LINK_HEAVY_THRESHOLD:
            warnings.append("too_many_links")
        normalized = body_text.casefold()
        if not any(marker in normalized for marker in UNSUBSCRIBE_MARKERS):
            warnings.append("missing_unsubscribe_marker")
        if not any(marker in normalized for marker in FOOTER_MARKERS):
            warnings.append("missing_footer_marker")

    unique_warnings = tuple(dict.fromkeys(warnings))
    return NewsletterDeliverabilityIssue(
        issue_id=str(row.get("issue_id") or row.get("id") or ""),
        subject=subject,
        status=str(row.get("status") or ""),
        timestamp=str(row.get("sent_at") or row.get("created_at") or ""),
        risk_score=_risk_score(unique_warnings),
        warnings=unique_warnings,
        link_count=link_count,
        subject_flags=subject_flags,
        source_count=len(source_ids),
    )


def _subject_flags(subject: str) -> list[str]:
    normalized = subject.casefold()
    flags: list[str] = []
    if any(term in normalized for term in SPAMMY_TERMS):
        flags.append("spammy_subject_terms")
    if subject.count("!") + subject.count("?") >= 3 or re.search(r"[!?]{2,}", subject):
        flags.append("excessive_subject_punctuation")
    letters = [char for char in subject if char.isalpha()]
    if len(letters) >= 8 and sum(1 for char in letters if char.isupper()) / len(letters) >= 0.8:
        flags.append("all_caps_subject")
    return flags


def _risk_score(warnings: tuple[str, ...]) -> int:
    weights = {
        "all_caps_subject": 10,
        "empty_body": 25,
        "excessive_subject_punctuation": 10,
        "malformed_source_content_ids": 10,
        "missing_footer_marker": 10,
        "missing_source_content_ids": 15,
        "missing_unsubscribe_marker": 15,
        "spammy_subject_terms": 15,
        "too_many_links": 20,
    }
    return min(100, sum(weights.get(warning, 5) for warning in warnings))


def _body_text(row: Mapping[str, Any]) -> str:
    texts: list[str] = []
    for key in ("body", "content", "html", "text", "markdown"):
        if row.get(key):
            texts.append(str(row[key]))
    metadata = _parse_json(row.get("metadata"))
    texts.extend(_metadata_texts(metadata))
    return "\n".join(texts)


def _metadata_texts(value: Any) -> list[str]:
    texts: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_lower = str(key).casefold()
            if isinstance(item, str) and any(
                marker in key_lower
                for marker in ("body", "content", "footer", "html", "markdown", "text")
            ):
                texts.append(item)
            elif isinstance(item, (Mapping, list, tuple)):
                texts.extend(_metadata_texts(item))
    elif isinstance(value, (list, tuple)):
        for item in value:
            if isinstance(item, str):
                texts.append(item)
            elif isinstance(item, (Mapping, list, tuple)):
                texts.extend(_metadata_texts(item))
    return texts


def _link_count(text: str) -> int:
    links = set()
    for match in URL_RE.finditer(text):
        links.add((match.group(1) or match.group(0)).strip())
    return len(links)


def _parse_source_content_ids(raw_value: Any) -> tuple[list[int], list[str]]:
    if raw_value in (None, ""):
        return [], []
    try:
        parsed = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    except (TypeError, json.JSONDecodeError):
        return [], ["malformed_source_content_ids"]
    if not isinstance(parsed, list):
        return [], ["malformed_source_content_ids"]

    source_ids: list[int] = []
    malformed = False
    for item in parsed:
        try:
            content_id = int(item)
        except (TypeError, ValueError):
            malformed = True
            continue
        if content_id <= 0:
            malformed = True
            continue
        source_ids.append(content_id)
    warnings = ["malformed_source_content_ids"] if malformed else []
    return source_ids, warnings


def _load_newsletter_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    days: int,
    limit: int,
) -> list[dict[str, Any]]:
    selected = [
        column
        for column in (
            "id",
            "issue_id",
            "subject",
            "status",
            "sent_at",
            "created_at",
            "source_content_ids",
            "metadata",
            "body",
            "content",
            "html",
            "text",
            "markdown",
        )
        if column in columns
    ]
    timestamp_expr = _timestamp_expr(columns)
    where = f"datetime({timestamp_expr}) >= datetime('now', ?)" if timestamp_expr else ""
    sql = f"SELECT {', '.join(selected)} FROM newsletter_sends"
    params: list[Any] = []
    if where:
        sql += f" WHERE {where}"
        params.append(f"-{days} days")
    order_expr = f"datetime({timestamp_expr}) DESC, " if timestamp_expr else ""
    sql += f" ORDER BY {order_expr}id DESC LIMIT ?"
    params.append(limit)
    cursor = conn.execute(sql, params)
    columns_by_index = [description[0] for description in cursor.description or ()]
    return [
        {
            columns_by_index[index]: value
            for index, value in enumerate(row)
        }
        for row in cursor.fetchall()
    ]


def _timestamp_expr(columns: set[str]) -> str:
    timestamps = [column for column in ("sent_at", "created_at") if column in columns]
    if not timestamps:
        return ""
    if len(timestamps) == 1:
        return timestamps[0]
    return "COALESCE(sent_at, created_at)"


def _parse_json(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, (Mapping, list, tuple)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return None


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    if "newsletter_sends" not in schema:
        return ("newsletter_sends",), {}
    missing = tuple(sorted({"id"} - schema["newsletter_sends"]))
    return (), {"newsletter_sends": missing} if missing else {}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[str(table)] = {
            column[1] for column in conn.execute(f"PRAGMA table_info({table})")
        }
    return schema
