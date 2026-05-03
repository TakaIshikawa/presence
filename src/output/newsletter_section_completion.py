"""Report missing or thin sections in draft newsletter assemblies."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_MIN_SECTION_WORDS = 20
DEFAULT_REQUIRED_SECTIONS = (
    "intro",
    "work highlights",
    "curated links",
    "closing note",
)
SECTION_STATUSES = ("complete", "missing", "thin")

_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(.+?)\s*#*\s*$")
_WORD_RE = re.compile(r"[A-Za-z0-9]+(?:[-'][A-Za-z0-9]+)?")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_SECTION_KEYS = ("sections", "items", "blocks")
_TEXT_KEYS = ("body_markdown", "body", "content", "markdown", "text", "html")
_HEADING_KEYS = ("heading", "title", "name", "section", "label")


@dataclass(frozen=True)
class NewsletterSectionCompletionRow:
    """Completion status for one required section in one newsletter draft."""

    newsletter_id: str
    section_name: str
    status: str
    observed_length: int
    reason: str
    minimum_length: int
    matched_heading: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterSectionCompletionReport:
    """Section completion report for one selected draft newsletter."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[NewsletterSectionCompletionRow, ...]
    newsletter_id: str | None = None
    subject: str | None = None
    draft_status: str | None = None
    draft_timestamp: str | None = None
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_section_completion",
            "draft_status": self.draft_status,
            "draft_timestamp": self.draft_timestamp,
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "newsletter_id": self.newsletter_id,
            "rows": [row.to_dict() for row in self.rows],
            "subject": self.subject,
            "totals": dict(sorted(self.totals.items())),
            "warnings": list(self.warnings),
        }


def build_newsletter_section_completion_report(
    db_or_conn: Any,
    *,
    newsletter_id: str | None = None,
    required_sections: Sequence[str] = DEFAULT_REQUIRED_SECTIONS,
    min_section_words: int = DEFAULT_MIN_SECTION_WORDS,
    section_minimums: Mapping[str, int] | None = None,
    now: datetime | None = None,
) -> NewsletterSectionCompletionReport:
    """Load a draft newsletter and return section completeness findings."""

    if min_section_words < 0:
        raise ValueError("min_section_words must be non-negative")
    normalized_required = tuple(section.strip() for section in required_sections if section.strip())
    if not normalized_required:
        raise ValueError("at least one required section is needed")
    minimums = _minimums(
        normalized_required,
        default_minimum=min_section_words,
        overrides=section_minimums or {},
    )

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    filters = {
        "latest_draft": newsletter_id is None,
        "min_section_words": min_section_words,
        "newsletter_id": newsletter_id,
        "required_sections": list(normalized_required),
        "section_minimums": dict(sorted(minimums.items())),
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    draft = _load_draft(conn, schema["newsletter_sends"], newsletter_id=newsletter_id)
    if draft is None:
        warning = "no matching newsletter draft found" if newsletter_id else "no draft newsletter found"
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            warnings=(warning,),
        )

    selected_newsletter_id = str(draft.get("issue_id") or draft.get("id") or "")
    sections = _section_index(_draft_payload(draft))
    rows = tuple(
        _completion_row(
            newsletter_id=selected_newsletter_id,
            section_name=section,
            minimum_length=minimums[_normalize_section_name(section)],
            sections=sections,
        )
        for section in normalized_required
    )
    return NewsletterSectionCompletionReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(rows),
        rows=rows,
        newsletter_id=selected_newsletter_id,
        subject=str(draft.get("subject") or "") or None,
        draft_status=str(draft.get("status") or "") or None,
        draft_timestamp=str(draft.get("sent_at") or draft.get("created_at") or "") or None,
        missing_columns={},
    )


def analyze_newsletter_section_completion(
    newsletter: Any,
    *,
    newsletter_id: str = "input",
    subject: str = "",
    required_sections: Sequence[str] = DEFAULT_REQUIRED_SECTIONS,
    min_section_words: int = DEFAULT_MIN_SECTION_WORDS,
    section_minimums: Mapping[str, int] | None = None,
    now: datetime | None = None,
) -> NewsletterSectionCompletionReport:
    """Analyze an explicit assembled payload without database access."""

    if min_section_words < 0:
        raise ValueError("min_section_words must be non-negative")
    normalized_required = tuple(section.strip() for section in required_sections if section.strip())
    if not normalized_required:
        raise ValueError("at least one required section is needed")
    minimums = _minimums(
        normalized_required,
        default_minimum=min_section_words,
        overrides=section_minimums or {},
    )
    sections = _section_index(newsletter)
    rows = tuple(
        _completion_row(
            newsletter_id=newsletter_id,
            section_name=section,
            minimum_length=minimums[_normalize_section_name(section)],
            sections=sections,
        )
        for section in normalized_required
    )
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    return NewsletterSectionCompletionReport(
        generated_at=generated_at.isoformat(),
        filters={
            "input": newsletter_id,
            "min_section_words": min_section_words,
            "required_sections": list(normalized_required),
            "section_minimums": dict(sorted(minimums.items())),
        },
        totals=_totals(rows),
        rows=rows,
        newsletter_id=newsletter_id,
        subject=subject or None,
    )


def format_newsletter_section_completion_json(
    report: NewsletterSectionCompletionReport,
) -> str:
    """Serialize a section completion report as deterministic JSON."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_section_completion_text(
    report: NewsletterSectionCompletionReport,
) -> str:
    """Render a compact text report for pre-send review."""

    totals = report.totals
    lines = [
        "Newsletter Section Completion",
        f"Generated: {report.generated_at}",
        (
            f"Newsletter: {report.newsletter_id or '-'} subject={report.subject or '-'} "
            f"status={report.draft_status or '-'}"
        ),
        (
            f"Sections: complete={totals['complete']} thin={totals['thin']} "
            f"missing={totals['missing']} total={totals['sections_checked']}"
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
    if report.warnings:
        lines.append("Warnings: " + "; ".join(report.warnings))
    if not report.rows:
        lines.append("No section rows available.")
        return "\n".join(lines)

    lines.append("Rows:")
    for row in report.rows:
        heading = f" matched={row.matched_heading}" if row.matched_heading else ""
        lines.append(
            f"- newsletter_id={row.newsletter_id} section={row.section_name} "
            f"status={row.status} observed_length={row.observed_length} "
            f"minimum_length={row.minimum_length} reason={row.reason}{heading}"
        )
    return "\n".join(lines)


def _completion_row(
    *,
    newsletter_id: str,
    section_name: str,
    minimum_length: int,
    sections: Mapping[str, dict[str, Any]],
) -> NewsletterSectionCompletionRow:
    normalized = _normalize_section_name(section_name)
    section = _matching_section(sections, normalized)
    if section is None:
        return NewsletterSectionCompletionRow(
            newsletter_id=newsletter_id,
            section_name=section_name,
            status="missing",
            observed_length=0,
            minimum_length=minimum_length,
            reason="required section not found",
        )

    observed = int(section["word_count"])
    if observed < minimum_length:
        return NewsletterSectionCompletionRow(
            newsletter_id=newsletter_id,
            section_name=section_name,
            status="thin",
            observed_length=observed,
            minimum_length=minimum_length,
            reason=f"observed length below minimum ({observed} < {minimum_length})",
            matched_heading=str(section["heading"]),
        )
    return NewsletterSectionCompletionRow(
        newsletter_id=newsletter_id,
        section_name=section_name,
        status="complete",
        observed_length=observed,
        minimum_length=minimum_length,
        reason="meets minimum length",
        matched_heading=str(section["heading"]),
    )


def _section_index(newsletter: Any) -> dict[str, dict[str, Any]]:
    decoded = _decode_json(newsletter) if isinstance(newsletter, str) else newsletter
    sections = _structured_sections(decoded)
    if sections is None:
        sections = _sections_from_text(str(newsletter or ""))
    return {section["normalized_heading"]: section for section in sections}


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
        sections.append(_section_counts(heading=heading, text=text or ""))
    return sections


def _sections_from_text(text: str) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    current_heading = "Intro"
    current_lines: list[str] = []

    for line in text.splitlines():
        match = _HEADING_RE.match(line)
        if match:
            if current_lines or sections:
                sections.append(_section_counts(heading=current_heading, text="\n".join(current_lines)))
            current_heading = _clean_heading(match.group(1))
            current_lines = []
        else:
            current_lines.append(line)

    if current_lines or not sections:
        sections.append(_section_counts(heading=current_heading, text="\n".join(current_lines)))
    return [section for section in sections if section["word_count"]]


def _section_counts(*, heading: str, text: str) -> dict[str, Any]:
    return {
        "heading": heading,
        "normalized_heading": _normalize_section_name(heading),
        "word_count": len(_WORD_RE.findall(text)),
    }


def _matching_section(
    sections: Mapping[str, dict[str, Any]],
    required: str,
) -> dict[str, Any] | None:
    if required in sections:
        return sections[required]
    for candidate, section in sections.items():
        if _section_matches(candidate, required):
            return section
    return None


def _section_matches(candidate: str, required: str) -> bool:
    if candidate == required:
        return True
    return required in candidate or candidate in required


def _normalize_section_name(value: str) -> str:
    normalized = _NON_ALNUM_RE.sub(" ", str(value).casefold()).strip()
    aliases = {
        "call to action": "closing note",
        "closing": "closing note",
        "cta": "closing note",
        "outro": "closing note",
        "signoff": "closing note",
        "final note": "closing note",
        "links": "curated links",
        "link": "curated links",
        "reading": "curated links",
        "resources": "curated links",
        "curated reads": "curated links",
        "highlights": "work highlights",
        "shipped": "work highlights",
        "shipped work": "work highlights",
        "updates": "work highlights",
        "work": "work highlights",
        "opening": "intro",
        "introduction": "intro",
    }
    return aliases.get(normalized, normalized)


def _minimums(
    required_sections: Sequence[str],
    *,
    default_minimum: int,
    overrides: Mapping[str, int],
) -> dict[str, int]:
    minimums = {
        _normalize_section_name(section): int(default_minimum) for section in required_sections
    }
    for key, value in overrides.items():
        parsed = int(value)
        if parsed < 0:
            raise ValueError("section minimums must be non-negative")
        minimums[_normalize_section_name(key)] = parsed
    return minimums


def _draft_payload(row: Mapping[str, Any]) -> Any:
    metadata = _parse_json(row.get("metadata"))
    if isinstance(metadata, Mapping):
        for key in ("assembled_payload", "payload", "newsletter", "draft"):
            value = metadata.get(key)
            if isinstance(value, (Mapping, list)):
                return value
        texts = _metadata_texts(metadata)
    else:
        texts = []

    for key in _TEXT_KEYS:
        if row.get(key):
            return str(row[key])
    if texts:
        return "\n".join(texts)
    return ""


def _metadata_texts(value: Any) -> list[str]:
    texts: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            if isinstance(item, str) and key in _TEXT_KEYS:
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


def _load_draft(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    newsletter_id: str | None,
) -> dict[str, Any] | None:
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
    timestamp_expr = _timestamp_expr(columns)
    params: list[Any] = []
    where: list[str] = []
    if newsletter_id:
        where.append("(CAST(id AS TEXT) = ? OR issue_id = ?)")
        params.extend([newsletter_id, newsletter_id])
    elif "status" in columns:
        where.append("status = 'draft'")

    sql = f"SELECT {', '.join(selected)} FROM newsletter_sends"
    if where:
        sql += " WHERE " + " AND ".join(where)
    order_expr = f"datetime({timestamp_expr}) DESC, " if timestamp_expr else ""
    sql += f" ORDER BY {order_expr}id DESC LIMIT 1"
    cursor = conn.execute(sql, params)
    row = cursor.fetchone()
    if row is None:
        return None
    names = [description[0] for description in cursor.description or ()]
    return {names[index]: value for index, value in enumerate(row)}


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    if "newsletter_sends" not in schema:
        return ("newsletter_sends",), {}
    missing = tuple(sorted({"id", "subject"} - schema["newsletter_sends"]))
    return (), {"newsletter_sends": missing} if missing else {}


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
    warnings: tuple[str, ...] = (),
) -> NewsletterSectionCompletionReport:
    return NewsletterSectionCompletionReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={"complete": 0, "missing": 0, "sections_checked": 0, "thin": 0},
        rows=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns or {},
        warnings=warnings,
    )


def _totals(rows: Sequence[NewsletterSectionCompletionRow]) -> dict[str, int]:
    return {
        "complete": sum(1 for row in rows if row.status == "complete"),
        "missing": sum(1 for row in rows if row.status == "missing"),
        "sections_checked": len(rows),
        "thin": sum(1 for row in rows if row.status == "thin"),
    }


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


def _clean_heading(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().strip("*:_- ")).strip() or "Untitled"


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
