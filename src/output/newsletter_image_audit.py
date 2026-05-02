"""Audit newsletter drafts for image references that can break after send."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import sqlite3
from typing import Any, Iterable, Sequence
from urllib.parse import urlparse


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 50
DEFAULT_STATUSES = ("draft", "pending", "queued", "scheduled")
SUPPORTED_SCHEMES = ("http", "https")
TEXT_COLUMNS = ("subject", "body", "content", "html", "text", "markdown", "preview")
SEVERITIES = ("high", "medium", "low")

_MARKDOWN_IMAGE_RE = re.compile(r"!\[[^\]]*\]\(\s*(?P<ref><[^>]*>|[^)\s]*)(?:\s+\"[^\"]*\")?\s*\)")
_WINDOWS_ABSOLUTE_RE = re.compile(r"^[a-zA-Z]:[\\/]")
_LOCAL_PREFIXES = ("/Users/", "/home/", "/tmp/", "/var/", "/private/", "~/", "./", "../")


@dataclass(frozen=True)
class NewsletterImageReference:
    """One image reference found in newsletter draft content."""

    reference_type: str
    reference: str
    line: int
    column: int
    source: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "reference_type": self.reference_type,
            "reference": self.reference,
            "line": self.line,
            "column": self.column,
            "source": self.source,
        }


@dataclass(frozen=True)
class NewsletterImageFinding:
    """One image reference likely to fail in an email client."""

    location: dict[str, Any]
    reference_type: str
    offending_reference: str
    issue: str
    severity: str
    suggested_fix: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "location": dict(self.location),
            "reference_type": self.reference_type,
            "offending_reference": self.offending_reference,
            "issue": self.issue,
            "severity": self.severity,
            "suggested_fix": self.suggested_fix,
        }


@dataclass(frozen=True)
class NewsletterImageAuditReport:
    """Aggregated image-reference audit report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    findings: tuple[NewsletterImageFinding, ...]
    records: tuple[dict[str, Any], ...] = ()
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_image_audit",
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "totals": dict(self.totals),
            "findings": [finding.to_dict() for finding in self.findings],
            "records": [dict(record) for record in self.records],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def audit_newsletter_image_text(
    text: str,
    *,
    source: str = "draft",
    allow_relative: bool = False,
    allowed_schemes: Sequence[str] = SUPPORTED_SCHEMES,
) -> tuple[NewsletterImageFinding, ...]:
    """Inspect Markdown and HTML image references in one text payload."""
    allowed = {scheme.casefold() for scheme in allowed_schemes}
    references = [
        *_extract_markdown_images(text, source),
        *_HtmlImageParser.references_from(text, source),
    ]
    findings = [
        finding
        for reference in references
        if (
            finding := _classify_reference(
                reference,
                allow_relative=allow_relative,
                allowed_schemes=allowed,
            )
        )
        is not None
    ]
    return tuple(findings)


def build_newsletter_image_file_report(
    path: str | Any,
    *,
    allow_relative: bool = False,
    allowed_schemes: Sequence[str] = SUPPORTED_SCHEMES,
    now: datetime | None = None,
) -> NewsletterImageAuditReport:
    """Build an image-reference audit for a local draft file."""
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    draft_path = str(path)
    text = _read_text_path(path)
    findings = audit_newsletter_image_text(
        text,
        source=draft_path,
        allow_relative=allow_relative,
        allowed_schemes=allowed_schemes,
    )
    return _report(
        generated_at=generated_at,
        filters={
            "source": "file",
            "path": draft_path,
            "allow_relative": allow_relative,
            "allowed_schemes": list(allowed_schemes),
        },
        records=({"source": draft_path},),
        findings=findings,
    )


def build_newsletter_image_queue_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    status: str | Sequence[str] | None = DEFAULT_STATUSES,
    limit: int | None = DEFAULT_LIMIT,
    allow_relative: bool = False,
    allowed_schemes: Sequence[str] = SUPPORTED_SCHEMES,
    now: datetime | None = None,
) -> NewsletterImageAuditReport:
    """Audit recent queued newsletter records from ``newsletter_sends``."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit < 0:
        raise ValueError("limit must be non-negative")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    statuses = _normalise_statuses(status)
    filters = {
        "source": "queue",
        "days": days,
        "updated_after": cutoff.isoformat(),
        "status": list(statuses),
        "limit": limit,
        "allow_relative": allow_relative,
        "allowed_schemes": list(allowed_schemes),
    }
    if "newsletter_sends" not in schema:
        return _empty_report(generated_at, filters, ("newsletter_sends",), {})

    if "id" not in schema["newsletter_sends"]:
        return _empty_report(generated_at, filters, (), {"newsletter_sends": ("id",)})

    rows = _load_queued_newsletters(conn, schema["newsletter_sends"], cutoff, statuses, limit)
    findings: list[NewsletterImageFinding] = []
    records: list[dict[str, Any]] = []
    for row in rows:
        record = _record_summary(row)
        records.append(record)
        for source, text in _row_texts(row):
            scoped_source = f"newsletter_sends:{record['newsletter_send_id']}:{source}"
            findings.extend(
                audit_newsletter_image_text(
                    text,
                    source=scoped_source,
                    allow_relative=allow_relative,
                    allowed_schemes=allowed_schemes,
                )
            )

    return _report(
        generated_at=generated_at,
        filters=filters,
        records=tuple(records),
        findings=tuple(findings),
    )


def format_newsletter_image_audit_json(report: NewsletterImageAuditReport) -> str:
    """Serialize the audit as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _extract_markdown_images(text: str, source: str) -> tuple[NewsletterImageReference, ...]:
    references: list[NewsletterImageReference] = []
    line_starts = _line_starts(text)
    for match in _MARKDOWN_IMAGE_RE.finditer(text):
        raw = match.group("ref")
        if raw.startswith("<") and raw.endswith(">"):
            raw = raw[1:-1]
        line, column = _line_column(line_starts, match.start("ref"))
        references.append(
            NewsletterImageReference(
                reference_type="markdown",
                reference=raw.strip(),
                line=line,
                column=column,
                source=source,
            )
        )
    return tuple(references)


class _HtmlImageParser(HTMLParser):
    def __init__(self, source: str) -> None:
        super().__init__(convert_charrefs=True)
        self.source = source
        self.references: list[NewsletterImageReference] = []

    @classmethod
    def references_from(cls, text: str, source: str) -> tuple[NewsletterImageReference, ...]:
        parser = cls(source)
        parser.feed(text)
        parser.close()
        return tuple(parser.references)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() != "img":
            return
        attr_map = {name.casefold(): value for name, value in attrs}
        line, column = self.getpos()
        self.references.append(
            NewsletterImageReference(
                reference_type="html",
                reference=(attr_map.get("src") or "").strip(),
                line=line,
                column=column + 1,
                source=self.source,
            )
        )


def _classify_reference(
    reference: NewsletterImageReference,
    *,
    allow_relative: bool,
    allowed_schemes: set[str],
) -> NewsletterImageFinding | None:
    raw = reference.reference.strip()
    issue: str
    severity: str
    fix: str

    if not raw:
        issue = "empty_src"
        severity = "high"
        fix = "Add an absolute HTTPS image URL or remove the empty image tag."
    elif _is_local_filesystem_path(raw):
        issue = "local_filesystem_path"
        severity = "high"
        fix = "Upload the image to a public HTTPS host and replace the local path."
    else:
        parsed = urlparse(raw)
        scheme = parsed.scheme.casefold()
        if raw.startswith("//"):
            issue = "protocol_relative_url"
            severity = "medium"
            fix = "Use a full HTTPS URL so email clients do not infer the scheme."
        elif scheme:
            if scheme in allowed_schemes and parsed.netloc:
                return None
            issue = "unsupported_scheme"
            severity = "high"
            fix = "Use an absolute HTTPS image URL supported by email clients."
        elif allow_relative:
            return None
        else:
            issue = "relative_path"
            severity = "medium"
            fix = "Use an absolute HTTPS URL, or rerun with --allow-relative if the sender rewrites assets."

    return NewsletterImageFinding(
        location={
            "source": reference.source,
            "line": reference.line,
            "column": reference.column,
        },
        reference_type=reference.reference_type,
        offending_reference=raw,
        issue=issue,
        severity=severity,
        suggested_fix=fix,
    )


def _is_local_filesystem_path(value: str) -> bool:
    if value.startswith("file://"):
        return True
    if _WINDOWS_ABSOLUTE_RE.match(value):
        return True
    return value.startswith(_LOCAL_PREFIXES)


def _report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    records: tuple[dict[str, Any], ...],
    findings: tuple[NewsletterImageFinding, ...],
) -> NewsletterImageAuditReport:
    sorted_findings = tuple(
        sorted(
            findings,
            key=lambda item: (
                SEVERITIES.index(item.severity),
                str(item.location.get("source", "")),
                int(item.location.get("line", 0)),
                int(item.location.get("column", 0)),
                item.issue,
            ),
        )
    )
    return NewsletterImageAuditReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "record_count": len(records),
            "finding_count": len(sorted_findings),
            "severity_totals": _severity_totals(sorted_findings),
            "missing_tables": 0,
        },
        records=records,
        findings=sorted_findings,
    )


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> NewsletterImageAuditReport:
    return NewsletterImageAuditReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "record_count": 0,
            "finding_count": 0,
            "severity_totals": _severity_totals(()),
            "missing_tables": len(missing_tables),
        },
        findings=(),
        records=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _severity_totals(findings: Iterable[NewsletterImageFinding]) -> dict[str, int]:
    counts = Counter(finding.severity for finding in findings)
    return {severity: counts.get(severity, 0) for severity in SEVERITIES}


def _load_queued_newsletters(
    conn: sqlite3.Connection,
    columns: set[str],
    cutoff: datetime,
    statuses: tuple[str, ...],
    limit: int | None,
) -> list[dict[str, Any]]:
    selected = [
        column
        for column in (
            "id",
            "issue_id",
            "subject",
            "status",
            *TEXT_COLUMNS,
            "metadata",
            "created_at",
            "updated_at",
            "scheduled_at",
            "sent_at",
        )
        if column in columns
    ]
    where: list[str] = []
    params: list[Any] = []
    if statuses and "status" in columns:
        where.append(f"LOWER(status) IN ({','.join('?' for _ in statuses)})")
        params.extend(statuses)
    date_column = next(
        (
            column
            for column in ("updated_at", "created_at", "scheduled_at", "sent_at")
            if column in columns
        ),
        None,
    )
    if date_column:
        where.append(f"({date_column} IS NULL OR datetime({date_column}) >= datetime(?))")
        params.append(cutoff.isoformat())
    sql = f"SELECT {', '.join(selected)} FROM newsletter_sends"
    if where:
        sql += " WHERE " + " AND ".join(where)
    order_column = date_column or ("id" if "id" in columns else None)
    if order_column and order_column != "id":
        sql += f" ORDER BY datetime({order_column}) DESC, id DESC"
    else:
        sql += " ORDER BY id DESC"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    cursor = conn.execute(sql, params)
    return [_row_dict(cursor, row) for row in cursor.fetchall()]


def _row_texts(row: dict[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for column in TEXT_COLUMNS:
        value = row.get(column)
        if value:
            texts.append((column, str(value)))
    metadata = _parse_json(row.get("metadata"))
    if isinstance(metadata, (dict, list, tuple)):
        texts.extend(_metadata_texts(metadata, prefix="metadata"))
    return texts


def _metadata_texts(value: Any, *, prefix: str) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    if isinstance(value, dict):
        for key, item in value.items():
            child_prefix = f"{prefix}.{key}"
            if isinstance(item, str):
                texts.append((child_prefix, item))
            elif isinstance(item, (dict, list, tuple)):
                texts.extend(_metadata_texts(item, prefix=child_prefix))
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            child_prefix = f"{prefix}[{index}]"
            if isinstance(item, str):
                texts.append((child_prefix, item))
            elif isinstance(item, (dict, list, tuple)):
                texts.extend(_metadata_texts(item, prefix=child_prefix))
    return texts


def _record_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "newsletter_send_id": int(row["id"]),
        "issue_id": row.get("issue_id") or "",
        "subject": row.get("subject") or "",
        "status": row.get("status") or "",
    }


def _normalise_statuses(status: str | Sequence[str] | None) -> tuple[str, ...]:
    if status is None:
        return ()
    if isinstance(status, str):
        return (status.casefold(),) if status else ()
    return tuple(str(item).casefold() for item in status if str(item))


def _parse_json(raw_value: Any) -> Any:
    if raw_value is None or raw_value == "":
        return None
    if isinstance(raw_value, (dict, list)):
        return raw_value
    try:
        return json.loads(str(raw_value))
    except (TypeError, json.JSONDecodeError):
        return None


def _line_starts(text: str) -> list[int]:
    return [0, *[match.end() for match in re.finditer("\n", text)]]


def _line_column(line_starts: list[int], offset: int) -> tuple[int, int]:
    line_index = 0
    for index, start in enumerate(line_starts):
        if start > offset:
            break
        line_index = index
    return line_index + 1, offset - line_starts[line_index] + 1


def _read_text_path(path: Any) -> str:
    return Path(path).read_text(encoding="utf-8")


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or Database-like object")
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        columns = conn.execute(f"PRAGMA table_info({table})").fetchall()
        schema[str(table)] = {
            column["name"] if isinstance(column, sqlite3.Row) else column[1]
            for column in columns
        }
    return schema


def _row_dict(cursor: sqlite3.Cursor, row: Any) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        return dict(row)
    names = [description[0] for description in cursor.description or ()]
    return dict(zip(names, row))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
