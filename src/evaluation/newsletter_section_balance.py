"""Analyze generated newsletter drafts for section balance."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 25
DEFAULT_MAX_SECTION_WORD_SHARE = 0.45
DEFAULT_REQUIRED_SECTIONS = ("intro", "shipped", "learned", "links", "cta")

_HEADING_RE = re.compile(r"^\s{0,3}(#{1,6})\s+(.+?)\s*#*\s*$")
_URL_RE = re.compile(r"""(?i)\bhttps?://[^\s<>"')]+|href\s*=\s*["']([^"']+)["']""")
_WORD_RE = re.compile(r"[A-Za-z0-9]+(?:[-'][A-Za-z0-9]+)?")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_SECTION_KEYS = ("sections", "items", "blocks")
_TEXT_KEYS = ("body", "content", "html", "markdown", "text")
_HEADING_KEYS = ("heading", "title", "name", "section", "label")


@dataclass(frozen=True)
class NewsletterSectionMetric:
    """Counts for one newsletter section."""

    heading: str
    normalized_heading: str
    heading_level: int | None
    heading_count: int
    paragraph_count: int
    link_count: int
    word_count: int
    word_share: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterSectionBalanceIssue:
    """Section-balance findings for one newsletter draft."""

    newsletter_id: str
    subject: str
    status: str
    timestamp: str
    total_sections: int
    total_headings: int
    total_paragraphs: int
    total_links: int
    total_words: int
    warnings: tuple[str, ...]
    sections: tuple[NewsletterSectionMetric, ...]

    @property
    def has_warnings(self) -> bool:
        return bool(self.warnings)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["warnings"] = list(self.warnings)
        payload["sections"] = [section.to_dict() for section in self.sections]
        payload["has_warnings"] = self.has_warnings
        return payload


@dataclass(frozen=True)
class NewsletterSectionBalanceReport:
    """Newsletter section-balance report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    issues: tuple[NewsletterSectionBalanceIssue, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_section_balance",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "issues": [issue.to_dict() for issue in self.issues],
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def analyze_newsletter_section_balance(
    newsletter: Any,
    *,
    newsletter_id: str = "",
    subject: str = "",
    status: str = "",
    timestamp: str = "",
    required_sections: Sequence[str] = DEFAULT_REQUIRED_SECTIONS,
    max_section_word_share: float = DEFAULT_MAX_SECTION_WORD_SHARE,
) -> NewsletterSectionBalanceIssue:
    """Analyze one newsletter body or structured section payload."""
    if max_section_word_share <= 0 or max_section_word_share > 1:
        raise ValueError("max_section_word_share must be greater than 0 and at most 1")

    parsed_sections = _parse_sections(newsletter)
    total_words = sum(section["word_count"] for section in parsed_sections)
    sections = tuple(
        NewsletterSectionMetric(
            heading=section["heading"],
            normalized_heading=section["normalized_heading"],
            heading_level=section["heading_level"],
            heading_count=section["heading_count"],
            paragraph_count=section["paragraph_count"],
            link_count=section["link_count"],
            word_count=section["word_count"],
            word_share=(section["word_count"] / total_words if total_words else 0.0),
        )
        for section in parsed_sections
    )
    warnings = _warnings(
        sections,
        required_sections=required_sections,
        max_section_word_share=max_section_word_share,
    )
    return NewsletterSectionBalanceIssue(
        newsletter_id=newsletter_id,
        subject=subject,
        status=status,
        timestamp=timestamp,
        total_sections=len(sections),
        total_headings=sum(section.heading_count for section in sections),
        total_paragraphs=sum(section.paragraph_count for section in sections),
        total_links=sum(section.link_count for section in sections),
        total_words=total_words,
        warnings=warnings,
        sections=sections,
    )


def build_newsletter_section_balance_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    required_sections: Sequence[str] = DEFAULT_REQUIRED_SECTIONS,
    max_section_word_share: float = DEFAULT_MAX_SECTION_WORD_SHARE,
    now: datetime | None = None,
) -> NewsletterSectionBalanceReport:
    """Load recent newsletter candidates and analyze section balance."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    filters = {
        "days": days,
        "limit": limit,
        "max_section_word_share": max_section_word_share,
        "required_sections": list(required_sections),
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return NewsletterSectionBalanceReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            totals={"drafts_scanned": 0, "warning_count": 0, "drafts_with_warnings": 0},
            issues=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_newsletter_rows(conn, schema["newsletter_sends"], days=days, limit=limit)
    issues = tuple(
        analyze_newsletter_section_balance(
            _body_text(row),
            newsletter_id=str(row.get("issue_id") or row.get("id") or ""),
            subject=str(row.get("subject") or ""),
            status=str(row.get("status") or ""),
            timestamp=str(row.get("sent_at") or row.get("created_at") or ""),
            required_sections=required_sections,
            max_section_word_share=max_section_word_share,
        )
        for row in rows
    )
    return NewsletterSectionBalanceReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "drafts_scanned": len(issues),
            "drafts_with_warnings": sum(1 for issue in issues if issue.has_warnings),
            "warning_count": sum(len(issue.warnings) for issue in issues),
        },
        issues=issues,
        missing_tables=(),
        missing_columns={},
    )


def build_newsletter_section_balance_report_from_text(
    text: str,
    *,
    newsletter_id: str = "input",
    subject: str = "",
    required_sections: Sequence[str] = DEFAULT_REQUIRED_SECTIONS,
    max_section_word_share: float = DEFAULT_MAX_SECTION_WORD_SHARE,
    now: datetime | None = None,
) -> NewsletterSectionBalanceReport:
    """Analyze one explicit text input without database access."""
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    issue = analyze_newsletter_section_balance(
        text,
        newsletter_id=newsletter_id,
        subject=subject,
        required_sections=required_sections,
        max_section_word_share=max_section_word_share,
    )
    return NewsletterSectionBalanceReport(
        generated_at=generated_at.isoformat(),
        filters={
            "input": newsletter_id,
            "max_section_word_share": max_section_word_share,
            "required_sections": list(required_sections),
        },
        totals={
            "drafts_scanned": 1,
            "drafts_with_warnings": int(issue.has_warnings),
            "warning_count": len(issue.warnings),
        },
        issues=(issue,),
    )


def format_newsletter_section_balance_json(report: NewsletterSectionBalanceReport) -> str:
    """Serialize a section-balance report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_section_balance_text(report: NewsletterSectionBalanceReport) -> str:
    """Format a concise human-readable report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Newsletter Section Balance",
        f"Generated: {report.generated_at}",
        (
            f"Mode: max_section_word_share={filters['max_section_word_share']} "
            f"required={','.join(filters['required_sections'])}"
        ),
        (
            f"Drafts: scanned={totals['drafts_scanned']} "
            f"with_warnings={totals['drafts_with_warnings']} "
            f"warnings={totals['warning_count']}"
        ),
    ]
    if "days" in filters:
        lines.append(f"Window: days={filters['days']} limit={filters['limit']}")
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + missing)
    if not report.issues:
        lines.extend(["", "No newsletter drafts found."])
        return "\n".join(lines)

    lines.extend(["", "Drafts:"])
    for issue in report.issues:
        warning_text = ", ".join(issue.warnings) if issue.warnings else "clean"
        lines.append(
            f"- {issue.newsletter_id or 'unknown'} subject={issue.subject or '-'} "
            f"sections={issue.total_sections} words={issue.total_words} "
            f"links={issue.total_links} warnings={warning_text}"
        )
        for section in issue.sections:
            lines.append(
                f"  - {section.heading}: words={section.word_count} "
                f"share={section.word_share:.2f} paragraphs={section.paragraph_count} "
                f"links={section.link_count}"
            )
    return "\n".join(lines)


def _parse_sections(newsletter: Any) -> list[dict[str, Any]]:
    decoded = _decode_json(newsletter) if isinstance(newsletter, str) else newsletter
    structured = _structured_sections(decoded)
    if structured is not None:
        return structured
    return _sections_from_text(str(newsletter or ""))


def _structured_sections(value: Any) -> list[dict[str, Any]] | None:
    sequence: Any
    if isinstance(value, Mapping):
        sequence = None
        for key in _SECTION_KEYS:
            if isinstance(value.get(key), list):
                sequence = value[key]
                break
        if sequence is None:
            return None
    elif isinstance(value, list):
        sequence = value
    else:
        return None

    sections: list[dict[str, Any]] = []
    for index, item in enumerate(sequence, start=1):
        if isinstance(item, Mapping):
            heading = _first_present_text(item, _HEADING_KEYS) or f"Section {index}"
            text = _first_present_text(item, _TEXT_KEYS)
            if text is None and "paragraphs" in item:
                text = "\n\n".join(str(part) for part in _as_sequence(item["paragraphs"]))
        else:
            heading = f"Section {index}"
            text = str(item or "")
        sections.append(_section_counts(heading=heading, text=text or "", heading_level=None))
    return sections


def _sections_from_text(text: str) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    current_heading = "Intro"
    current_level: int | None = None
    current_lines: list[str] = []
    heading_count = 0

    for line in text.splitlines():
        match = _HEADING_RE.match(line)
        if match:
            if current_lines or sections:
                sections.append(
                    _section_counts(
                        heading=current_heading,
                        text="\n".join(current_lines),
                        heading_level=current_level,
                        heading_count=heading_count,
                    )
                )
            current_heading = _clean_heading(match.group(2))
            current_level = len(match.group(1))
            current_lines = []
            heading_count = 1
        else:
            current_lines.append(line)

    if current_lines or not sections:
        sections.append(
            _section_counts(
                heading=current_heading,
                text="\n".join(current_lines),
                heading_level=current_level,
                heading_count=heading_count,
            )
        )
    return [section for section in sections if section["word_count"] or section["heading_count"]]


def _section_counts(
    *,
    heading: str,
    text: str,
    heading_level: int | None,
    heading_count: int = 1,
) -> dict[str, Any]:
    return {
        "heading": heading,
        "normalized_heading": _normalize_section_name(heading),
        "heading_level": heading_level,
        "heading_count": heading_count,
        "paragraph_count": _paragraph_count(text),
        "link_count": _link_count(text),
        "word_count": _word_count(text),
    }


def _warnings(
    sections: tuple[NewsletterSectionMetric, ...],
    *,
    required_sections: Sequence[str],
    max_section_word_share: float,
) -> tuple[str, ...]:
    warnings: list[str] = []
    present = [section.normalized_heading for section in sections]
    for required in required_sections:
        normalized = _normalize_section_name(required)
        if normalized and not any(_section_matches(candidate, normalized) for candidate in present):
            warnings.append(f"missing_required_section:{required}")

    for section in sections:
        if section.word_count > 0 and section.word_share > max_section_word_share:
            warnings.append(f"dominant_section:{section.heading}")
    return tuple(warnings)


def _section_matches(candidate: str, required: str) -> bool:
    if candidate == required:
        return True
    return required in candidate.split() or candidate in required.split()


def _paragraph_count(text: str) -> int:
    paragraphs = [
        part.strip()
        for part in re.split(r"\n\s*\n+", text.strip())
        if part.strip() and not _HEADING_RE.match(part.strip())
    ]
    return len(paragraphs)


def _link_count(text: str) -> int:
    links = set()
    for match in _URL_RE.finditer(text):
        links.add((match.group(1) or match.group(0)).strip())
    return len(links)


def _word_count(text: str) -> int:
    scrubbed = _URL_RE.sub(" ", text)
    return len(_WORD_RE.findall(scrubbed))


def _clean_heading(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().strip("*:_- ")).strip() or "Untitled"


def _normalize_section_name(value: str) -> str:
    normalized = _NON_ALNUM_RE.sub(" ", str(value).casefold()).strip()
    aliases = {
        "call to action": "cta",
        "call for action": "cta",
        "closing": "cta",
        "link": "links",
        "reading": "links",
        "resources": "links",
        "learn": "learned",
        "learning": "learned",
        "learnings": "learned",
        "lesson": "learned",
        "lessons": "learned",
        "what shipped": "shipped",
        "shipped work": "shipped",
        "updates": "shipped",
        "update": "shipped",
    }
    return aliases.get(normalized, normalized)


def _body_text(row: Mapping[str, Any]) -> str:
    texts: list[str] = []
    for key in _TEXT_KEYS:
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
            if isinstance(item, str) and any(marker in key_lower for marker in _TEXT_KEYS):
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
            "markdown",
            "text",
        )
        if column in columns
    ]
    timestamp_expr = _timestamp_expr(columns)
    sql = f"SELECT {', '.join(selected)} FROM newsletter_sends"
    params: list[Any] = []
    if timestamp_expr:
        sql += f" WHERE datetime({timestamp_expr}) >= datetime('now', ?)"
        params.append(f"-{days} days")
    order_expr = f"datetime({timestamp_expr}) DESC, " if timestamp_expr else ""
    sql += f" ORDER BY {order_expr}id DESC LIMIT ?"
    params.append(limit)
    cursor = conn.execute(sql, params)
    column_names = [description[0] for description in cursor.description or ()]
    return [
        {column_names[index]: value for index, value in enumerate(row)}
        for row in cursor.fetchall()
    ]


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    if "newsletter_sends" not in schema:
        return ("newsletter_sends",), {}
    missing = tuple(sorted({"id", "subject"} - schema["newsletter_sends"]))
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


def _timestamp_expr(columns: set[str]) -> str:
    timestamps = [column for column in ("sent_at", "created_at") if column in columns]
    if not timestamps:
        return ""
    if len(timestamps) == 1:
        return timestamps[0]
    return "COALESCE(sent_at, created_at)"


def _decode_json(value: str) -> Any:
    stripped = value.strip()
    if not stripped or stripped[0] not in "[{":
        return value
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return value


def _parse_json(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, (Mapping, list, tuple)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return None


def _first_present_text(item: Mapping[str, Any], keys: Sequence[str]) -> str | None:
    for key in keys:
        value = item.get(key)
        if value is not None:
            return str(value)
    return None


def _as_sequence(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
