"""Report whether draft newsletters include a clear call to action."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_LIMIT = 25
COVERAGE_STATUSES = ("strong", "weak", "missing")

_TEXT_KEYS = ("body_markdown", "body", "content", "markdown", "text", "html")
_TITLE_KEYS = ("title", "headline", "name")
_URL_RE = re.compile(
    r"(?:https?://|mailto:)[^\s<>'\")]+|"
    r"\[[^\]]+\]\((?:https?://|mailto:)[^)]+\)|"
    r"\bhref\s*=\s*(['\"])(?:https?://|mailto:).*?\1",
    re.IGNORECASE | re.DOTALL,
)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")

_LINK_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "link",
        re.compile(
            r"\b(?:read|review|watch|listen|try|download|register|join|book|"
            r"reserve|sign up|get|start|open|visit|check out|learn more)\b",
            re.IGNORECASE,
        ),
    ),
)
_REPLY_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "reply_prompt",
        re.compile(
            r"\b(?:reply|respond|hit reply|send me|tell me|let me know)\b",
            re.IGNORECASE,
        ),
    ),
)
_SUBSCRIBE_SHARE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "subscription",
        re.compile(r"\b(?:subscribe|sign up|join the list|upgrade)\b", re.IGNORECASE),
    ),
    (
        "share",
        re.compile(r"\b(?:share|forward|send this to|pass this along)\b", re.IGNORECASE),
    ),
)
_WEAK_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "weak_link",
        re.compile(r"\b(?:click here|read more|more here|learn more)\b", re.IGNORECASE),
    ),
    (
        "weak_interest",
        re.compile(
            r"\b(?:if interested|if you are interested|worth a look)\b",
            re.IGNORECASE,
        ),
    ),
)


@dataclass(frozen=True)
class NewsletterCtaCoverageRow:
    """CTA coverage classification for one draft newsletter."""

    draft_id: str
    subject: str
    title: str
    status: str
    timestamp: str
    coverage: str
    cta_type: str
    link_present: bool
    reason: str
    matched_text: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterCtaCoverageReport:
    """CTA coverage review report for newsletter drafts."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[NewsletterCtaCoverageRow, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_cta_coverage",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
            "warnings": list(self.warnings),
        }


def analyze_newsletter_cta_coverage(
    newsletter: Any,
    *,
    draft_id: str = "input",
    subject: str = "",
    title: str = "",
    status: str = "",
    timestamp: str = "",
) -> NewsletterCtaCoverageRow:
    """Classify one draft body for deterministic CTA coverage."""

    body = _body_text(newsletter)
    if not title and isinstance(newsletter, Mapping):
        title = _first_present_text(newsletter, _TITLE_KEYS) or ""
    return _coverage_row(
        draft_id=draft_id,
        subject=subject,
        title=title,
        status=status,
        timestamp=timestamp,
        body=body,
    )


def build_newsletter_cta_coverage_report(
    db_or_conn: Any,
    *,
    limit: int = DEFAULT_LIMIT,
    coverage_filter: Sequence[str] | None = None,
    now: datetime | None = None,
) -> NewsletterCtaCoverageReport:
    """Load recent draft newsletters and classify CTA coverage."""

    row_limit = int(limit)
    if row_limit <= 0:
        raise ValueError("limit must be positive")
    filters = _normalized_filters(coverage_filter)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc)).isoformat()
    report_filters = {
        "coverage": list(filters) if filters else [],
        "limit": row_limit,
        "status": "draft",
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return NewsletterCtaCoverageReport(
            generated_at=generated_at,
            filters=report_filters,
            totals=_totals(()),
            rows=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    drafts = _load_drafts(conn, schema["newsletter_sends"], limit=row_limit)
    rows = tuple(_row_from_draft(draft) for draft in drafts)
    if filters:
        rows = tuple(row for row in rows if row.coverage in filters)
    warnings = ("no draft newsletter rows found",) if not drafts else ()
    return NewsletterCtaCoverageReport(
        generated_at=generated_at,
        filters=report_filters,
        totals=_totals(rows),
        rows=rows,
        missing_columns={},
        warnings=warnings,
    )


def format_newsletter_cta_coverage_json(report: NewsletterCtaCoverageReport) -> str:
    """Serialize a CTA coverage report as deterministic JSON."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _coverage_row(
    *,
    draft_id: str,
    subject: str,
    title: str,
    status: str,
    timestamp: str,
    body: str,
) -> NewsletterCtaCoverageRow:
    paragraphs = _paragraphs(body)
    link_present = bool(_URL_RE.search(body))

    for cta_type, pattern in (*_REPLY_PATTERNS, *_SUBSCRIBE_SHARE_PATTERNS):
        match = _first_match(paragraphs, pattern)
        if match:
            return NewsletterCtaCoverageRow(
                draft_id=draft_id,
                subject=subject,
                title=title,
                status=status,
                timestamp=timestamp,
                coverage="strong",
                cta_type=cta_type,
                link_present=link_present,
                reason=f"clear {cta_type.replace('_', ' ')} detected",
                matched_text=match,
            )

    for cta_type, pattern in _LINK_PATTERNS:
        match = _first_match(paragraphs, pattern, require_link=True)
        if match:
            return NewsletterCtaCoverageRow(
                draft_id=draft_id,
                subject=subject,
                title=title,
                status=status,
                timestamp=timestamp,
                coverage="strong",
                cta_type=cta_type,
                link_present=True,
                reason="clear action phrase includes a link",
                matched_text=match,
            )

    for cta_type, pattern in _WEAK_PATTERNS:
        match = _first_match(paragraphs, pattern)
        if match:
            return NewsletterCtaCoverageRow(
                draft_id=draft_id,
                subject=subject,
                title=title,
                status=status,
                timestamp=timestamp,
                coverage="weak",
                cta_type=cta_type,
                link_present=link_present,
                reason="CTA wording is vague or low intent",
                matched_text=match,
            )

    if link_present:
        return NewsletterCtaCoverageRow(
            draft_id=draft_id,
            subject=subject,
            title=title,
            status=status,
            timestamp=timestamp,
            coverage="weak",
            cta_type="link",
            link_present=True,
            reason="link present without a clear reader action",
            matched_text=_first_link_context(paragraphs),
        )

    return NewsletterCtaCoverageRow(
        draft_id=draft_id,
        subject=subject,
        title=title,
        status=status,
        timestamp=timestamp,
        coverage="missing",
        cta_type="none",
        link_present=False,
        reason="no clear CTA phrase or link detected",
    )


def _row_from_draft(draft: Mapping[str, Any]) -> NewsletterCtaCoverageRow:
    metadata = _parse_json(draft.get("metadata"))
    title = ""
    if isinstance(metadata, Mapping):
        title = _first_present_text(metadata, _TITLE_KEYS) or ""
        if not title:
            for key in ("assembled_payload", "payload", "newsletter", "draft"):
                value = metadata.get(key)
                if isinstance(value, Mapping):
                    title = _first_present_text(value, _TITLE_KEYS) or ""
                    if title:
                        break
    return analyze_newsletter_cta_coverage(
        _draft_payload(draft),
        draft_id=str(draft.get("issue_id") or draft.get("id") or ""),
        subject=str(draft.get("subject") or ""),
        title=title,
        status=str(draft.get("status") or ""),
        timestamp=str(draft.get("sent_at") or draft.get("created_at") or ""),
    )


def _paragraphs(text: str) -> list[str]:
    normalized = re.sub(r"(?i)</p\s*>", "\n\n", text)
    normalized = re.sub(r"(?i)<br\s*/?>", "\n", normalized)
    normalized = _HTML_TAG_RE.sub(" ", normalized)
    blocks = re.split(r"(?:\r?\n\s*){2,}", normalized)
    paragraphs = [_WHITESPACE_RE.sub(" ", block).strip() for block in blocks if block.strip()]
    if paragraphs:
        return paragraphs
    stripped = _WHITESPACE_RE.sub(" ", normalized).strip()
    return [stripped] if stripped else []


def _first_match(
    paragraphs: Sequence[str],
    pattern: re.Pattern[str],
    *,
    require_link: bool = False,
) -> str:
    for paragraph in paragraphs:
        if pattern.search(paragraph) and (not require_link or _URL_RE.search(paragraph)):
            return paragraph[:240]
    return ""


def _first_link_context(paragraphs: Sequence[str]) -> str:
    for paragraph in paragraphs:
        if _URL_RE.search(paragraph):
            return paragraph[:240]
    return ""


def _body_text(value: Any) -> str:
    decoded = _parse_json(value) if isinstance(value, str) else value
    if isinstance(decoded, Mapping):
        texts: list[str] = []
        for key in _TEXT_KEYS:
            if decoded.get(key):
                texts.append(str(decoded[key]))
        if texts:
            return "\n".join(texts)
        return "\n".join(_metadata_texts(decoded))
    if isinstance(decoded, list):
        return "\n".join(_body_text(item) for item in decoded)
    return str(value or "")


def _draft_payload(row: Mapping[str, Any]) -> Any:
    metadata = _parse_json(row.get("metadata"))
    if isinstance(metadata, Mapping):
        for key in ("assembled_payload", "payload", "newsletter", "draft"):
            value = metadata.get(key)
            if isinstance(value, (Mapping, list)):
                return value
        metadata_texts = _metadata_texts(metadata)
    else:
        metadata_texts = []

    row_texts = [str(row[key]) for key in _TEXT_KEYS if row.get(key)]
    if row_texts:
        return "\n".join(row_texts)
    if metadata_texts:
        return "\n".join(metadata_texts)
    return ""


def _metadata_texts(value: Any) -> list[str]:
    texts: list[str] = []
    if isinstance(value, Mapping):
        if any(
            str(key).casefold() in {"url", "href", "label", "text", "title"}
            for key in value
        ):
            combined = " ".join(str(item) for item in value.values() if item)
            if combined:
                texts.append(combined)
        for key, item in value.items():
            key_lower = str(key).casefold()
            if isinstance(item, str) and any(
                marker in key_lower
                for marker in (
                    "body",
                    "content",
                    "cta",
                    "html",
                    "href",
                    "link",
                    "markdown",
                    "text",
                    "url",
                )
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


def _load_drafts(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
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
            "metadata",
            "body_markdown",
            "body",
            "content",
            "markdown",
            "text",
            "html",
        )
        if column in columns
    ]
    where = " WHERE status = 'draft'" if "status" in columns else ""
    timestamp_expr = _timestamp_expr(columns)
    order_expr = f"datetime({timestamp_expr}) DESC, " if timestamp_expr else ""
    sql = f"SELECT {', '.join(selected)} FROM newsletter_sends{where}"
    sql += f" ORDER BY {order_expr}id DESC LIMIT ?"
    cursor = conn.execute(sql, (limit,))
    names = [description[0] for description in cursor.description or ()]
    return [
        {names[index]: value for index, value in enumerate(row)}
        for row in cursor.fetchall()
    ]


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    if "newsletter_sends" not in schema:
        return ("newsletter_sends",), {}
    missing = tuple(sorted({"id"} - schema["newsletter_sends"]))
    return (), {"newsletter_sends": missing} if missing else {}


def _normalized_filters(values: Sequence[str] | None) -> tuple[str, ...]:
    normalized = tuple(
        str(value).strip().casefold() for value in (values or ()) if str(value).strip()
    )
    invalid = sorted(set(normalized) - set(COVERAGE_STATUSES))
    if invalid:
        raise ValueError(f"coverage filter must be one of: {', '.join(COVERAGE_STATUSES)}")
    return tuple(status for status in COVERAGE_STATUSES if status in normalized)


def _totals(rows: Sequence[NewsletterCtaCoverageRow]) -> dict[str, int]:
    return {
        "missing": sum(1 for row in rows if row.coverage == "missing"),
        "rows": len(rows),
        "strong": sum(1 for row in rows if row.coverage == "strong"),
        "weak": sum(1 for row in rows if row.coverage == "weak"),
    }


def _timestamp_expr(columns: set[str]) -> str:
    timestamps = [column for column in ("sent_at", "created_at") if column in columns]
    if not timestamps:
        return ""
    if len(timestamps) == 1:
        return timestamps[0]
    return "COALESCE(sent_at, created_at)"


def _first_present_text(value: Mapping[str, Any], keys: Sequence[str]) -> str | None:
    for key in keys:
        item = value.get(key)
        if item:
            return str(item)
    return None


def _parse_json(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, (Mapping, list, tuple)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return None


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


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
