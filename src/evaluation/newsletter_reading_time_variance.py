"""Report newsletter reading-time variance against a recent baseline."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from html import unescape
import json
import re
import sqlite3
from statistics import median
from typing import Any, Mapping


DEFAULT_DAYS = 90
DEFAULT_LIMIT = 25
DEFAULT_MIN_WORDS_PER_MINUTE = 200
LONG_RATIO_THRESHOLD = 1.5
SHORT_RATIO_THRESHOLD = 0.5
_TEXT_KEYS = ("body", "content", "html", "text")
_TIMESTAMP_COLUMNS = ("sent_at", "created_at")
_TAG_RE = re.compile(r"<[^>]+>")
_SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b[^>]*>.*?</\1>", re.IGNORECASE | re.DOTALL)
_URL_RE = re.compile(r"""(?i)\bhttps?://[^\s<>"')]+""")
_WORD_RE = re.compile(r"[A-Za-z0-9]+(?:[-'][A-Za-z0-9]+)?")


@dataclass(frozen=True)
class NewsletterReadingTimeFinding:
    """Reading-time variance for one sent newsletter."""

    newsletter_send_id: int
    issue_id: str
    subject: str
    status: str
    timestamp: str
    content_source: str
    word_count: int
    estimated_read_minutes: float
    median_estimated_read_minutes: float
    variance_from_median_minutes: float
    absolute_variance_from_median_minutes: float
    ratio_to_median: float | None
    warnings: tuple[str, ...]

    @property
    def has_warnings(self) -> bool:
        return bool(self.warnings)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["warnings"] = list(self.warnings)
        payload["has_warnings"] = self.has_warnings
        return payload


@dataclass(frozen=True)
class NewsletterReadingTimeVarianceReport:
    """Newsletter reading-time variance report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    findings: tuple[NewsletterReadingTimeFinding, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_reading_time_variance",
            "filters": dict(self.filters),
            "findings": [finding.to_dict() for finding in self.findings],
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_newsletter_reading_time_variance_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    min_words_per_minute: int = DEFAULT_MIN_WORDS_PER_MINUTE,
    now: datetime | None = None,
) -> NewsletterReadingTimeVarianceReport:
    """Load recent sent newsletters and rank reading-time variance."""
    days = _positive_int(days, "days")
    limit = _positive_int(limit, "limit")
    words_per_minute = _positive_int(min_words_per_minute, "min_words_per_minute")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    filters = {
        "days": days,
        "limit": limit,
        "min_words_per_minute": words_per_minute,
        "long_ratio_threshold": LONG_RATIO_THRESHOLD,
        "short_ratio_threshold": SHORT_RATIO_THRESHOLD,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    rows = _load_newsletter_rows(
        conn,
        schema["newsletter_sends"],
        days=days,
        limit=limit,
        now=generated_at,
    )
    return _build_report_from_rows(
        rows,
        generated_at=generated_at,
        filters=filters,
        min_words_per_minute=words_per_minute,
        missing_tables=(),
        missing_columns={},
    )


def format_newsletter_reading_time_variance_json(
    report: NewsletterReadingTimeVarianceReport,
) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_reading_time_variance_text(
    report: NewsletterReadingTimeVarianceReport,
) -> str:
    """Render a concise human-readable report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Newsletter Reading Time Variance",
        f"Generated: {report.generated_at}",
        (
            f"Window: days={filters['days']} limit={filters['limit']} "
            f"wpm={filters['min_words_per_minute']}"
        ),
        (
            f"Totals: sends={totals['sends_scanned']} "
            f"with_content={totals['sends_with_content']} "
            f"findings={totals['finding_count']} "
            f"flagged={totals['flagged_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + missing)
    lines.append("")

    if not report.findings:
        if report.missing_tables or report.missing_columns:
            lines.append("No newsletter reading-time findings available until schema gaps are resolved.")
        else:
            lines.append("No recent sent newsletters with usable body content found.")
        return "\n".join(lines)

    median_minutes = report.findings[0].median_estimated_read_minutes
    lines.append(f"Median estimated read: {median_minutes:.2f} minutes")
    lines.append("Ranked issues:")
    for finding in report.findings:
        ratio = "-" if finding.ratio_to_median is None else f"{finding.ratio_to_median:.2f}x"
        warnings = ", ".join(finding.warnings) if finding.warnings else "within_baseline"
        lines.append(
            f"- {finding.issue_id or finding.newsletter_send_id} subject={finding.subject or '-'} "
            f"words={finding.word_count} read={finding.estimated_read_minutes:.2f}m "
            f"variance={finding.variance_from_median_minutes:+.2f}m ratio={ratio} "
            f"warnings={warnings}"
        )
    return "\n".join(lines)


def _build_report_from_rows(
    rows: list[dict[str, Any]],
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    min_words_per_minute: int,
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> NewsletterReadingTimeVarianceReport:
    measured: list[dict[str, Any]] = []
    missing_content_count = 0
    for row in rows:
        content, source = _body_text(row)
        word_count = _word_count(_strip_html(content))
        if not word_count:
            missing_content_count += 1
            continue
        estimated = round(word_count / min_words_per_minute, 2)
        measured.append(
            {
                "row": row,
                "content_source": source,
                "word_count": word_count,
                "estimated_read_minutes": estimated,
            }
        )

    median_minutes = round(median(item["estimated_read_minutes"] for item in measured), 2) if measured else 0.0
    findings = tuple(
        _finding(item, median_minutes=median_minutes)
        for item in sorted(
            measured,
            key=lambda item: (
                -abs(item["estimated_read_minutes"] - median_minutes),
                -item["estimated_read_minutes"],
                str(item["row"].get("timestamp") or ""),
                int(item["row"].get("newsletter_send_id") or 0),
            ),
        )
    )

    return NewsletterReadingTimeVarianceReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "sends_scanned": len(rows),
            "sends_with_content": len(measured),
            "missing_content_count": missing_content_count,
            "finding_count": len(findings),
            "flagged_count": sum(1 for finding in findings if finding.has_warnings),
        },
        findings=findings,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _finding(
    item: dict[str, Any],
    *,
    median_minutes: float,
) -> NewsletterReadingTimeFinding:
    row = item["row"]
    estimated = item["estimated_read_minutes"]
    variance = round(estimated - median_minutes, 2)
    absolute_variance = round(abs(variance), 2)
    ratio = round(estimated / median_minutes, 2) if median_minutes > 0 else None
    warnings: list[str] = []
    if ratio is not None and ratio >= LONG_RATIO_THRESHOLD:
        warnings.append("unusually_long")
    elif ratio is not None and ratio <= SHORT_RATIO_THRESHOLD:
        warnings.append("unusually_short")
    return NewsletterReadingTimeFinding(
        newsletter_send_id=int(row.get("newsletter_send_id") or 0),
        issue_id=str(row.get("issue_id") or ""),
        subject=str(row.get("subject") or ""),
        status=str(row.get("status") or ""),
        timestamp=str(row.get("timestamp") or ""),
        content_source=item["content_source"],
        word_count=item["word_count"],
        estimated_read_minutes=estimated,
        median_estimated_read_minutes=median_minutes,
        variance_from_median_minutes=variance,
        absolute_variance_from_median_minutes=absolute_variance,
        ratio_to_median=ratio,
        warnings=tuple(warnings),
    )


def _load_newsletter_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    days: int,
    limit: int,
    now: datetime,
) -> list[dict[str, Any]]:
    selected = [
        "id AS newsletter_send_id",
        _column_expr(columns, "issue_id", "''"),
        _column_expr(columns, "subject", "''"),
        _column_expr(columns, "status", "'sent'"),
        _column_expr(columns, "metadata", "NULL"),
        f"{_timestamp_expr(columns)} AS timestamp",
    ]
    selected.extend(column for column in _TEXT_KEYS if column in columns)
    where = [f"datetime({_timestamp_expr(columns)}) >= datetime(?)"]
    params: list[Any] = [(now.replace(microsecond=0)).isoformat(), f"-{days} days"]
    where[0] = f"datetime({_timestamp_expr(columns)}) >= datetime(?, ?)"
    if "status" in columns:
        where.append("LOWER(COALESCE(status, 'sent')) = 'sent'")
    sql = f"SELECT {', '.join(selected)} FROM newsletter_sends WHERE {' AND '.join(where)}"
    sql += f" ORDER BY datetime({_timestamp_expr(columns)}) DESC, id DESC LIMIT ?"
    params.append(limit)
    cursor = conn.execute(sql, params)
    names = [description[0] for description in cursor.description or ()]
    return [
        {names[index]: value for index, value in enumerate(row)}
        for row in cursor.fetchall()
    ]


def _body_text(row: Mapping[str, Any]) -> tuple[str, str]:
    for key in _TEXT_KEYS:
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value, f"newsletter_sends.{key}"
    metadata = _parse_json(row.get("metadata"))
    metadata_text = _metadata_text(metadata)
    if metadata_text:
        key, value = metadata_text
        return value, f"newsletter_sends.metadata.{key}"
    return "", ""


def _metadata_text(value: Any, *, prefix: str = "") -> tuple[str, str] | None:
    if isinstance(value, Mapping):
        for key in _TEXT_KEYS:
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return (f"{prefix}.{key}".strip("."), item)
        for key, item in value.items():
            if isinstance(item, (Mapping, list, tuple)):
                found = _metadata_text(item, prefix=f"{prefix}.{key}".strip("."))
                if found:
                    return found
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            if isinstance(item, (Mapping, list, tuple)):
                found = _metadata_text(item, prefix=f"{prefix}.{index}".strip("."))
                if found:
                    return found
    return None


def _strip_html(value: str) -> str:
    text = _SCRIPT_STYLE_RE.sub(" ", value or "")
    text = re.sub(r"(?i)<br\s*/?>|</p>|</div>|</li>|</h[1-6]>", " ", text)
    text = _TAG_RE.sub(" ", text)
    return unescape(text)


def _word_count(text: str) -> int:
    return len(_WORD_RE.findall(_URL_RE.sub(" ", text or "")))


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    if "newsletter_sends" not in schema:
        return ("newsletter_sends",), {}
    columns = schema["newsletter_sends"]
    missing_required = {"id"} - columns
    if not any(column in columns for column in _TIMESTAMP_COLUMNS):
        missing_required.add("sent_at")
    missing: list[str] = sorted(missing_required)
    if not any(column in columns for column in (*_TEXT_KEYS, "metadata")):
        missing.append("body|content|html|text|metadata")
    return (), {"newsletter_sends": tuple(missing)} if missing else {}


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> NewsletterReadingTimeVarianceReport:
    return NewsletterReadingTimeVarianceReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "sends_scanned": 0,
            "sends_with_content": 0,
            "missing_content_count": 0,
            "finding_count": 0,
            "flagged_count": 0,
        },
        findings=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[str(table)] = {column[1] for column in conn.execute(f"PRAGMA table_info({table})")}
    return schema


def _column_expr(columns: set[str], column: str, fallback: str) -> str:
    return column if column in columns else f"{fallback} AS {column}"


def _timestamp_expr(columns: set[str]) -> str:
    present = [column for column in _TIMESTAMP_COLUMNS if column in columns]
    if len(present) == 1:
        return present[0]
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


def _positive_int(value: int, name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be positive") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
