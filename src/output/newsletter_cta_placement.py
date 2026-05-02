"""Report newsletter draft CTA placement issues."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 25
DEFAULT_PARAGRAPH_THRESHOLD = 4
DEFAULT_CTA_MARKER_PATTERNS = (
    r"\bsubscribe\b",
    r"\bsign up\b",
    r"\bjoin\b",
    r"\bregister\b",
    r"\bupgrade\b",
    r"\bbook (?:a )?(?:call|demo)\b",
    r"\breply\b",
    r"\bshare\b",
    r"\bforward\b",
    r"\bclick here\b",
    r"\blearn more\b",
)


@dataclass(frozen=True)
class NewsletterCtaOccurrence:
    """One detected CTA-like fragment in a newsletter draft."""

    paragraph_index: int
    marker: str
    text: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterCtaPlacementIssue:
    """CTA placement result for one newsletter draft."""

    issue_id: str
    subject: str
    status: str
    timestamp: str
    warnings: tuple[str, ...]
    paragraph_count: int
    cta_count: int
    first_cta_paragraph: int | None
    repeated_ctas: tuple[str, ...]
    occurrences: tuple[NewsletterCtaOccurrence, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["warnings"] = list(self.warnings)
        payload["repeated_ctas"] = list(self.repeated_ctas)
        payload["occurrences"] = [occurrence.to_dict() for occurrence in self.occurrences]
        return payload


@dataclass(frozen=True)
class NewsletterCtaPlacementReport:
    """Newsletter CTA placement review report."""

    days: int
    limit: int
    paragraph_threshold: int
    cta_marker_patterns: tuple[str, ...]
    generated_at: str
    total_rows_inspected: int
    flagged_rows: int
    issues: tuple[NewsletterCtaPlacementIssue, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_cta_placement",
            "cta_marker_patterns": list(self.cta_marker_patterns),
            "days": self.days,
            "flagged_rows": self.flagged_rows,
            "generated_at": self.generated_at,
            "issues": [issue.to_dict() for issue in self.issues],
            "limit": self.limit,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "paragraph_threshold": self.paragraph_threshold,
            "total_rows_inspected": self.total_rows_inspected,
        }


def build_newsletter_cta_placement_report(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    paragraph_threshold: int = DEFAULT_PARAGRAPH_THRESHOLD,
    cta_marker_patterns: Sequence[str] = DEFAULT_CTA_MARKER_PATTERNS,
) -> NewsletterCtaPlacementReport:
    """Inspect recent newsletter drafts/sends for CTA placement problems."""
    period_days = int(days)
    if period_days <= 0:
        raise ValueError("days must be positive")
    row_limit = int(limit)
    if row_limit <= 0:
        raise ValueError("limit must be positive")
    threshold = int(paragraph_threshold)
    if threshold <= 0:
        raise ValueError("paragraph_threshold must be positive")
    patterns = _validate_marker_patterns(cta_marker_patterns)

    generated_at = datetime.now(timezone.utc).isoformat()
    conn = _connection(db)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return NewsletterCtaPlacementReport(
            days=period_days,
            limit=row_limit,
            paragraph_threshold=threshold,
            cta_marker_patterns=patterns,
            generated_at=generated_at,
            total_rows_inspected=0,
            flagged_rows=0,
            issues=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_newsletter_rows(conn, schema["newsletter_sends"], days=period_days, limit=row_limit)
    issues = analyze_newsletter_cta_placements(
        rows,
        paragraph_threshold=threshold,
        cta_marker_patterns=patterns,
    )
    return NewsletterCtaPlacementReport(
        days=period_days,
        limit=row_limit,
        paragraph_threshold=threshold,
        cta_marker_patterns=patterns,
        generated_at=generated_at,
        total_rows_inspected=len(issues),
        flagged_rows=sum(1 for issue in issues if issue.warnings),
        issues=issues,
    )


def analyze_newsletter_cta_placements(
    newsletters: Sequence[Mapping[str, Any]],
    *,
    paragraph_threshold: int = DEFAULT_PARAGRAPH_THRESHOLD,
    cta_marker_patterns: Sequence[str] = DEFAULT_CTA_MARKER_PATTERNS,
) -> tuple[NewsletterCtaPlacementIssue, ...]:
    """Analyze newsletter-like mappings for CTA placement warnings."""
    threshold = int(paragraph_threshold)
    if threshold <= 0:
        raise ValueError("paragraph_threshold must be positive")
    patterns = _validate_marker_patterns(cta_marker_patterns)
    compiled = tuple(re.compile(pattern, re.IGNORECASE) for pattern in patterns)
    return tuple(_placement_issue(row, compiled, threshold) for row in newsletters)


def format_newsletter_cta_placement_json(report: NewsletterCtaPlacementReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_cta_placement_text(report: NewsletterCtaPlacementReport) -> str:
    """Render a compact human-readable CTA placement report."""
    lines = [
        "Newsletter CTA Placement",
        f"Period: last {report.days} days",
        f"Limit: {report.limit}",
        f"Paragraph threshold: {report.paragraph_threshold}",
        (
            "Summary: "
            f"{report.total_rows_inspected} rows inspected, "
            f"{report.flagged_rows} rows with warnings"
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
        first = issue.first_cta_paragraph if issue.first_cta_paragraph is not None else "-"
        lines.append(
            f"- {issue.issue_id or 'unknown'} [{issue.status or 'unknown'}] "
            f"{subject}; paragraphs={issue.paragraph_count}; ctas={issue.cta_count}; "
            f"first_cta={first}; warnings={warning_text}"
        )
        if issue.repeated_ctas:
            lines.append("  repeated: " + " | ".join(issue.repeated_ctas))
    return "\n".join(lines)


def _placement_issue(
    row: Mapping[str, Any],
    marker_patterns: tuple[re.Pattern[str], ...],
    paragraph_threshold: int,
) -> NewsletterCtaPlacementIssue:
    body_text = _body_text(row)
    paragraphs = _paragraphs(body_text)
    occurrences = _cta_occurrences(paragraphs, row, marker_patterns)
    paragraph_indexes = [occurrence.paragraph_index for occurrence in occurrences]
    first_cta_paragraph = min(paragraph_indexes) if paragraph_indexes else None
    repeated = _repeated_ctas(occurrences)

    warnings: list[str] = []
    if not occurrences:
        warnings.append("missing_cta")
    else:
        if set(paragraph_indexes) == {1}:
            warnings.append("cta_only_first_paragraph")
        if repeated:
            warnings.append("repeated_identical_cta")
        if first_cta_paragraph and first_cta_paragraph > paragraph_threshold:
            warnings.append("cta_after_paragraph_threshold")

    return NewsletterCtaPlacementIssue(
        issue_id=str(row.get("issue_id") or row.get("id") or ""),
        subject=str(row.get("subject") or ""),
        status=str(row.get("status") or ""),
        timestamp=str(row.get("sent_at") or row.get("created_at") or ""),
        warnings=tuple(warnings),
        paragraph_count=len(paragraphs),
        cta_count=len(occurrences),
        first_cta_paragraph=first_cta_paragraph,
        repeated_ctas=tuple(repeated),
        occurrences=tuple(occurrences),
    )


def _cta_occurrences(
    paragraphs: Sequence[str],
    row: Mapping[str, Any],
    marker_patterns: tuple[re.Pattern[str], ...],
) -> list[NewsletterCtaOccurrence]:
    occurrences: list[NewsletterCtaOccurrence] = []
    for index, paragraph in enumerate(paragraphs, start=1):
        for pattern in marker_patterns:
            if pattern.search(paragraph):
                occurrences.append(
                    NewsletterCtaOccurrence(
                        paragraph_index=index,
                        marker=pattern.pattern,
                        text=_cta_text(paragraph),
                    )
                )
                break

    body_count = len(occurrences)
    for text in _link_texts(row):
        for pattern in marker_patterns:
            if pattern.search(text):
                paragraph_index = _paragraph_index_for_text(paragraphs, text)
                if paragraph_index is None and body_count:
                    continue
                occurrences.append(
                    NewsletterCtaOccurrence(
                        paragraph_index=paragraph_index or max(1, len(paragraphs)),
                        marker=pattern.pattern,
                        text=_cta_text(text),
                    )
                )
                break
    return occurrences


def _paragraphs(text: str) -> list[str]:
    normalized = re.sub(r"(?i)</p\s*>", "\n\n", text)
    normalized = re.sub(r"(?i)<br\s*/?>", "\n", normalized)
    normalized = re.sub(r"<[^>]+>", " ", normalized)
    blocks = re.split(r"(?:\r?\n\s*){2,}", normalized)
    paragraphs = [" ".join(block.split()) for block in blocks if block.strip()]
    if paragraphs:
        return paragraphs
    stripped = " ".join(normalized.split())
    return [stripped] if stripped else []


def _cta_text(text: str) -> str:
    return " ".join(text.split())[:240]


def _paragraph_index_for_text(paragraphs: Sequence[str], text: str) -> int | None:
    normalized_text = _normalize_cta(text)
    if not normalized_text:
        return None
    for index, paragraph in enumerate(paragraphs, start=1):
        normalized_paragraph = _normalize_cta(paragraph)
        if normalized_text in normalized_paragraph or normalized_paragraph in normalized_text:
            return index
    return None


def _repeated_ctas(occurrences: Sequence[NewsletterCtaOccurrence]) -> list[str]:
    normalized = [_normalize_cta(occurrence.text) for occurrence in occurrences]
    counts = Counter(value for value in normalized if value)
    repeated: list[str] = []
    seen: set[str] = set()
    for occurrence, key in zip(occurrences, normalized, strict=True):
        if counts[key] > 1 and key not in seen:
            repeated.append(occurrence.text)
            seen.add(key)
    return repeated


def _normalize_cta(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s:/.-]+", " ", text.casefold())).strip()


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
                for marker in ("body", "content", "cta", "html", "markdown", "text")
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


def _link_texts(row: Mapping[str, Any]) -> list[str]:
    texts: list[str] = []
    for key in ("link_text", "link_url", "cta_text", "cta_url"):
        if row.get(key):
            texts.append(str(row[key]))
    texts.extend(_metadata_link_texts(_parse_json(row.get("metadata"))))
    return texts


def _metadata_link_texts(value: Any) -> list[str]:
    texts: list[str] = []
    if isinstance(value, Mapping):
        if any(str(key).casefold() in {"url", "href", "label", "text", "title"} for key in value):
            texts.append(" ".join(str(item) for item in value.values() if item))
        for key, item in value.items():
            key_lower = str(key).casefold()
            if isinstance(item, str) and any(marker in key_lower for marker in ("cta", "link", "url")):
                texts.append(item)
            elif isinstance(item, (Mapping, list, tuple)):
                texts.extend(_metadata_link_texts(item))
    elif isinstance(value, (list, tuple)):
        for item in value:
            if isinstance(item, str):
                texts.append(item)
            elif isinstance(item, (Mapping, list, tuple)):
                texts.extend(_metadata_link_texts(item))
    return texts


def _validate_marker_patterns(patterns: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(str(pattern).strip() for pattern in patterns if str(pattern).strip())
    if not normalized:
        raise ValueError("at least one CTA marker pattern is required")
    for pattern in normalized:
        re.compile(pattern)
    return normalized


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
