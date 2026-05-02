"""Analyze newsletter image density and placement across sections."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 25
DEFAULT_MAX_IMAGES_PER_SECTION = 2
DEFAULT_LONG_SECTION_WORDS = 80
CTA_SECTION_NAMES = ("cta", "call to action", "call-to-action", "closing")

_HEADING_RE = re.compile(r"^\s{0,3}(#{1,6})\s+(.+?)\s*#*\s*$")
_MARKDOWN_IMAGE_RE = re.compile(
    r"!\[(?P<alt>[^\]]*)\]\(\s*(?P<src><[^>]*>|[^)\s]*)(?:\s+\"[^\"]*\")?\s*\)"
)
_WORD_RE = re.compile(r"[A-Za-z0-9]+(?:[-'][A-Za-z0-9]+)?")
_URL_RE = re.compile(r"""(?i)\bhttps?://[^\s<>"')]+|href\s*=\s*["']([^"']+)["']""")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_WHITESPACE_RE = re.compile(r"\s+")
_SECTION_KEYS = ("sections", "items", "blocks")
_TEXT_KEYS = ("body", "content", "html", "markdown", "text")
_HEADING_KEYS = ("heading", "title", "name", "section", "label")


@dataclass(frozen=True)
class NewsletterImagePlacementImage:
    """One image occurrence with section placement metadata."""

    source: str
    image_type: str
    src: str
    section: str
    section_index: int
    line: int
    column: int
    ordinal: int
    is_leading: bool
    is_after_cta: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterImagePlacementSection:
    """Image and text metrics for one newsletter section."""

    heading: str
    normalized_heading: str
    heading_level: int | None
    index: int
    word_count: int
    image_count: int
    images: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["images"] = list(self.images)
        return payload


@dataclass(frozen=True)
class NewsletterImagePlacementIssue:
    """Image-placement findings for one newsletter issue."""

    newsletter_id: str
    subject: str
    status: str
    timestamp: str
    total_sections: int
    total_words: int
    total_images: int
    warnings: tuple[str, ...]
    warning_totals: dict[str, int]
    sections: tuple[NewsletterImagePlacementSection, ...]
    images: tuple[NewsletterImagePlacementImage, ...]

    @property
    def has_warnings(self) -> bool:
        return bool(self.warnings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "has_warnings": self.has_warnings,
            "images": [image.to_dict() for image in self.images],
            "newsletter_id": self.newsletter_id,
            "sections": [section.to_dict() for section in self.sections],
            "status": self.status,
            "subject": self.subject,
            "timestamp": self.timestamp,
            "total_images": self.total_images,
            "total_sections": self.total_sections,
            "total_words": self.total_words,
            "warning_totals": dict(sorted(self.warning_totals.items())),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class NewsletterImagePlacementReport:
    """Newsletter image-placement report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    issues: tuple[NewsletterImagePlacementIssue, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_image_placement",
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


def analyze_newsletter_image_placement(
    newsletter: Any,
    *,
    newsletter_id: str = "",
    subject: str = "",
    status: str = "",
    timestamp: str = "",
    max_images_per_section: int = DEFAULT_MAX_IMAGES_PER_SECTION,
    long_section_words: int = DEFAULT_LONG_SECTION_WORDS,
    source: str = "input",
) -> NewsletterImagePlacementIssue:
    """Analyze one newsletter body or structured section payload."""
    if max_images_per_section <= 0:
        raise ValueError("max_images_per_section must be positive")
    if long_section_words <= 0:
        raise ValueError("long_section_words must be positive")

    parsed = _parse_sections(newsletter, source=source)
    sections = tuple(
        NewsletterImagePlacementSection(
            heading=section["heading"],
            normalized_heading=section["normalized_heading"],
            heading_level=section["heading_level"],
            index=section["index"],
            word_count=section["word_count"],
            image_count=len(section["images"]),
            images=tuple(image.src for image in section["images"]),
        )
        for section in parsed
    )
    images = tuple(image for section in parsed for image in section["images"])
    warnings = _warnings(
        parsed,
        images,
        max_images_per_section=max_images_per_section,
        long_section_words=long_section_words,
    )
    warning_totals = Counter(warning.split(":", 1)[0] for warning in warnings)
    return NewsletterImagePlacementIssue(
        newsletter_id=newsletter_id,
        subject=subject,
        status=status,
        timestamp=timestamp,
        total_sections=len(sections),
        total_words=sum(section.word_count for section in sections),
        total_images=len(images),
        warnings=warnings,
        warning_totals=dict(warning_totals),
        sections=sections,
        images=images,
    )


def build_newsletter_image_placement_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    max_images_per_section: int = DEFAULT_MAX_IMAGES_PER_SECTION,
    now: datetime | None = None,
) -> NewsletterImagePlacementReport:
    """Load recent newsletter sends and report image placement issues."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    filters = {
        "days": days,
        "limit": limit,
        "long_section_words": DEFAULT_LONG_SECTION_WORDS,
        "max_images_per_section": max_images_per_section,
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _report(
            generated_at=generated_at,
            filters=filters,
            issues=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_newsletter_rows(conn, schema["newsletter_sends"], days=days, limit=limit)
    issues = tuple(
        analyze_newsletter_image_placement(
            _body_payload(row),
            newsletter_id=str(row.get("issue_id") or row.get("id") or ""),
            subject=str(row.get("subject") or ""),
            status=str(row.get("status") or ""),
            timestamp=str(row.get("sent_at") or row.get("updated_at") or row.get("created_at") or ""),
            max_images_per_section=max_images_per_section,
            long_section_words=DEFAULT_LONG_SECTION_WORDS,
            source=f"newsletter_sends:{row.get('id')}",
        )
        for row in rows
    )
    return _report(generated_at=generated_at, filters=filters, issues=issues)


def build_newsletter_image_placement_report_from_text(
    text: str,
    *,
    newsletter_id: str = "input",
    subject: str = "",
    max_images_per_section: int = DEFAULT_MAX_IMAGES_PER_SECTION,
    now: datetime | None = None,
) -> NewsletterImagePlacementReport:
    """Analyze one explicit newsletter body without database access."""
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    issue = analyze_newsletter_image_placement(
        text,
        newsletter_id=newsletter_id,
        subject=subject,
        max_images_per_section=max_images_per_section,
        source=newsletter_id,
    )
    return _report(
        generated_at=generated_at,
        filters={
            "input": newsletter_id,
            "long_section_words": DEFAULT_LONG_SECTION_WORDS,
            "max_images_per_section": max_images_per_section,
        },
        issues=(issue,),
    )


def format_newsletter_image_placement_json(report: NewsletterImagePlacementReport) -> str:
    """Serialize an image-placement report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_image_placement_text(report: NewsletterImagePlacementReport) -> str:
    """Format a concise human-readable image-placement report."""
    totals = report.totals
    filters = report.filters
    lines = [
        "Newsletter Image Placement",
        f"Generated: {report.generated_at}",
        f"Mode: max_images_per_section={filters['max_images_per_section']}",
        (
            f"Issues: scanned={totals['issues_scanned']} "
            f"with_warnings={totals['issues_with_warnings']} "
            f"warnings={totals['warning_count']} images={totals['image_count']}"
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
        lines.extend(["", "No newsletter issues found."])
        return "\n".join(lines)

    lines.extend(["", "Issues:"])
    for issue in report.issues:
        warning_text = ", ".join(issue.warnings) if issue.warnings else "clean"
        lines.append(
            f"- {issue.newsletter_id or 'unknown'} subject={issue.subject or '-'} "
            f"sections={issue.total_sections} words={issue.total_words} "
            f"images={issue.total_images} warnings={warning_text}"
        )
        for section in issue.sections:
            lines.append(
                f"  - {section.heading}: words={section.word_count} "
                f"images={section.image_count}"
            )
    return "\n".join(lines)


def _parse_sections(newsletter: Any, *, source: str) -> list[dict[str, Any]]:
    decoded = _decode_json(newsletter) if isinstance(newsletter, str) else newsletter
    structured = _structured_sections(decoded, source=source)
    if structured is not None:
        return structured
    return _sections_from_text(str(newsletter or ""), source=source)


def _structured_sections(value: Any, *, source: str) -> list[dict[str, Any]] | None:
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
    ordinal = 1
    for index, item in enumerate(sequence, start=1):
        if isinstance(item, Mapping):
            heading = _first_present_text(item, _HEADING_KEYS) or f"Section {index}"
            parts: list[str] = []
            text = _first_present_text(item, _TEXT_KEYS)
            if text is not None:
                parts.append(text)
            if "paragraphs" in item:
                parts.extend(str(part) for part in _as_sequence(item["paragraphs"]))
            for image in _as_sequence(item.get("images", ())):
                if isinstance(image, Mapping):
                    src = str(image.get("src") or image.get("url") or "")
                else:
                    src = str(image or "")
                if src:
                    parts.append(f"![image]({src})")
        else:
            heading = f"Section {index}"
            parts = [str(item or "")]
        section = _section_from_parts(
            heading=heading,
            heading_level=None,
            index=index,
            text="\n\n".join(parts),
            source=source,
            ordinal_start=ordinal,
            line_offset=0,
        )
        ordinal += len(section["images"])
        sections.append(section)
    return sections


def _sections_from_text(text: str, *, source: str) -> list[dict[str, Any]]:
    markdown_sections = _markdown_section_bounds(text)
    html_headings = _HtmlHeadingParser.headings_from(text)
    if html_headings:
        bounds = _merge_heading_bounds(text, markdown_sections, html_headings)
    else:
        bounds = markdown_sections

    sections: list[dict[str, Any]] = []
    ordinal = 1
    line_starts = _line_starts(text)
    for index, bound in enumerate(bounds, start=1):
        start, end, heading, heading_level = bound
        section_text = text[start:end]
        line, _column = _line_column(line_starts, start)
        section = _section_from_parts(
            heading=heading,
            heading_level=heading_level,
            index=index,
            text=section_text,
            source=source,
            ordinal_start=ordinal,
            line_offset=line - 1,
        )
        ordinal += len(section["images"])
        sections.append(section)
    return [section for section in sections if section["word_count"] or section["images"]]


def _markdown_section_bounds(text: str) -> list[tuple[int, int, str, int | None]]:
    headings: list[tuple[int, int, str, int]] = []
    offset = 0
    for line in text.splitlines(keepends=True):
        match = _HEADING_RE.match(line.rstrip("\n"))
        if match:
            headings.append((offset, offset + len(line), _clean_heading(match.group(2)), len(match.group(1))))
        offset += len(line)

    if not headings:
        return [(0, len(text), "Intro", None)]

    bounds: list[tuple[int, int, str, int | None]] = []
    if headings[0][0] > 0:
        bounds.append((0, headings[0][0], "Intro", None))
    for index, (heading_start, content_start, heading, level) in enumerate(headings):
        end = headings[index + 1][0] if index + 1 < len(headings) else len(text)
        bounds.append((heading_start, end, heading, level))
    return bounds


def _merge_heading_bounds(
    text: str,
    markdown_bounds: list[tuple[int, int, str, int | None]],
    html_headings: list[tuple[int, str, int]],
) -> list[tuple[int, int, str, int | None]]:
    markdown_headings = [
        (start, heading, level or 0)
        for start, _end, heading, level in markdown_bounds
        if level is not None
    ]
    headings = sorted([*markdown_headings, *html_headings], key=lambda item: item[0])
    if not headings:
        return [(0, len(text), "Intro", None)]
    bounds: list[tuple[int, int, str, int | None]] = []
    if headings[0][0] > 0:
        bounds.append((0, headings[0][0], "Intro", None))
    for index, (start, heading, level) in enumerate(headings):
        end = headings[index + 1][0] if index + 1 < len(headings) else len(text)
        bounds.append((start, end, heading, level or None))
    return bounds


def _section_from_parts(
    *,
    heading: str,
    heading_level: int | None,
    index: int,
    text: str,
    source: str,
    ordinal_start: int,
    line_offset: int,
) -> dict[str, Any]:
    images = _extract_images(
        text,
        source=source,
        section=heading,
        section_index=index,
        ordinal_start=ordinal_start,
        line_offset=line_offset,
    )
    return {
        "heading": heading,
        "normalized_heading": _normalize_section_name(heading),
        "heading_level": heading_level,
        "index": index,
        "word_count": _word_count(text),
        "images": images,
    }


def _extract_images(
    text: str,
    *,
    source: str,
    section: str,
    section_index: int,
    ordinal_start: int,
    line_offset: int,
) -> list[NewsletterImagePlacementImage]:
    line_starts = _line_starts(text)
    images: list[NewsletterImagePlacementImage] = []
    for match in _MARKDOWN_IMAGE_RE.finditer(text):
        src = match.group("src")
        if src.startswith("<") and src.endswith(">"):
            src = src[1:-1]
        line, column = _line_column(line_starts, match.start())
        images.append(
            NewsletterImagePlacementImage(
                source=source,
                image_type="markdown",
                src=src.strip(),
                section=section,
                section_index=section_index,
                line=line + line_offset,
                column=column,
                ordinal=0,
                is_leading=False,
                is_after_cta=False,
            )
        )
    images.extend(
        _HtmlImagePlacementParser.images_from(
            text,
            source=source,
            section=section,
            section_index=section_index,
            line_offset=line_offset,
        )
    )
    images.sort(key=lambda image: (image.line, image.column, image.src, image.image_type))
    return [
        _replace_image_flags(
            image,
            ordinal=ordinal_start + index,
            is_leading=_image_is_leading(text, image.line - line_offset),
            is_after_cta=_is_cta_section(section),
        )
        for index, image in enumerate(images)
    ]


class _HtmlHeadingParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.headings: list[tuple[int, int, str, int]] = []
        self._heading_tag: str | None = None
        self._heading_line = 0
        self._heading_column = 0
        self._heading_parts: list[str] = []

    @classmethod
    def headings_from(cls, text: str) -> list[tuple[int, str, int]]:
        parser = cls()
        parser.feed(text)
        parser.close()
        line_starts = _line_starts(text)
        return [
            (_offset_for_position(line_starts, line, column), heading, level)
            for line, column, heading, level in parser.headings
        ]

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag_name = tag.casefold()
        if tag_name in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            line, column = self.getpos()
            self._heading_tag = tag_name
            self._heading_line = line
            self._heading_column = column
            self._heading_parts = []

    def handle_endtag(self, tag: str) -> None:
        tag_name = tag.casefold()
        if tag_name == self._heading_tag:
            heading = _clean(" ".join(self._heading_parts))
            if heading:
                self.headings.append(
                    (self._heading_line, self._heading_column, heading, int(tag_name[1]))
                )
            self._heading_tag = None
            self._heading_parts = []

    def handle_data(self, data: str) -> None:
        if self._heading_tag is not None:
            clean = _clean(data)
            if clean:
                self._heading_parts.append(clean)


class _HtmlImagePlacementParser(HTMLParser):
    def __init__(
        self,
        *,
        source: str,
        section: str,
        section_index: int,
        line_offset: int,
    ) -> None:
        super().__init__(convert_charrefs=True)
        self.source = source
        self.section = section
        self.section_index = section_index
        self.line_offset = line_offset
        self.images: list[NewsletterImagePlacementImage] = []

    @classmethod
    def images_from(
        cls,
        text: str,
        *,
        source: str,
        section: str,
        section_index: int,
        line_offset: int,
    ) -> list[NewsletterImagePlacementImage]:
        parser = cls(
            source=source,
            section=section,
            section_index=section_index,
            line_offset=line_offset,
        )
        parser.feed(text)
        parser.close()
        return parser.images

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() != "img":
            return
        attr_map = {name.casefold(): value for name, value in attrs}
        line, column = self.getpos()
        self.images.append(
            NewsletterImagePlacementImage(
                source=self.source,
                image_type="html",
                src=(attr_map.get("src") or "").strip(),
                section=self.section,
                section_index=self.section_index,
                line=line + self.line_offset,
                column=column + 1,
                ordinal=0,
                is_leading=False,
                is_after_cta=False,
            )
        )


def _replace_image_flags(
    image: NewsletterImagePlacementImage,
    *,
    ordinal: int,
    is_leading: bool,
    is_after_cta: bool,
) -> NewsletterImagePlacementImage:
    return NewsletterImagePlacementImage(
        source=image.source,
        image_type=image.image_type,
        src=image.src,
        section=image.section,
        section_index=image.section_index,
        line=image.line,
        column=image.column,
        ordinal=ordinal,
        is_leading=is_leading,
        is_after_cta=is_after_cta,
    )


def _warnings(
    sections: list[dict[str, Any]],
    images: tuple[NewsletterImagePlacementImage, ...],
    *,
    max_images_per_section: int,
    long_section_words: int,
) -> tuple[str, ...]:
    warnings: list[str] = []
    first_section = sections[0] if sections else None
    for image in images:
        if (
            first_section
            and first_section["heading_level"] is None
            and image.section_index == first_section["index"]
            and image.is_leading
        ):
            warnings.append(f"leading_image:{image.section}")

    for section in sections:
        image_count = len(section["images"])
        if image_count > max_images_per_section:
            warnings.append(f"clustered_section:{section['heading']}:{image_count}")
        if section["word_count"] >= long_section_words and image_count == 0:
            warnings.append(f"image_free_long_section:{section['heading']}:{section['word_count']}")

    for image in images:
        if image.is_after_cta:
            warnings.append(f"post_cta_image:{image.section}")
    return tuple(warnings)


def _image_is_leading(section_text: str, image_line: int) -> bool:
    before = "\n".join(section_text.splitlines()[: max(image_line - 1, 0)])
    before = _HEADING_RE.sub(" ", before)
    before = re.sub(r"<h[1-6]\b[^>]*>.*?</h[1-6]>", " ", before, flags=re.I | re.S)
    before = re.sub(r"<[^>]+>", " ", before)
    before = _MARKDOWN_IMAGE_RE.sub(" ", before)
    return _word_count(before) == 0


def _body_payload(row: Mapping[str, Any]) -> Any:
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


def _report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    issues: tuple[NewsletterImagePlacementIssue, ...],
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> NewsletterImagePlacementReport:
    warning_totals: Counter[str] = Counter()
    for issue in issues:
        warning_totals.update(issue.warning_totals)
    return NewsletterImagePlacementReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "image_count": sum(issue.total_images for issue in issues),
            "issues_scanned": len(issues),
            "issues_with_warnings": sum(1 for issue in issues if issue.has_warnings),
            "section_count": sum(issue.total_sections for issue in issues),
            "warning_count": sum(len(issue.warnings) for issue in issues),
            "warning_totals": dict(sorted(warning_totals.items())),
        },
        issues=issues,
        missing_tables=missing_tables,
        missing_columns=missing_columns or {},
    )


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
            "updated_at",
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
    return [_row_dict(cursor, row) for row in cursor.fetchall()]


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    if "newsletter_sends" not in schema:
        return ("newsletter_sends",), {}
    missing = tuple(sorted({"id", "subject"} - schema["newsletter_sends"]))
    return (), {"newsletter_sends": missing} if missing else {}


def _timestamp_expr(columns: set[str]) -> str:
    timestamps = [column for column in ("sent_at", "updated_at", "created_at") if column in columns]
    if not timestamps:
        return ""
    if len(timestamps) == 1:
        return timestamps[0]
    return "COALESCE(" + ", ".join(timestamps) + ")"


def _word_count(text: str) -> int:
    scrubbed = _URL_RE.sub(" ", text)
    scrubbed = _MARKDOWN_IMAGE_RE.sub(" ", scrubbed)
    scrubbed = "\n".join(
        " " if _HEADING_RE.match(line) else line for line in scrubbed.splitlines()
    )
    scrubbed = re.sub(r"<h[1-6]\b[^>]*>.*?</h[1-6]>", " ", scrubbed, flags=re.I | re.S)
    scrubbed = re.sub(r"<[^>]+>", " ", scrubbed)
    return len(_WORD_RE.findall(scrubbed))


def _normalize_section_name(value: str) -> str:
    normalized = _NON_ALNUM_RE.sub(" ", str(value).casefold()).strip()
    aliases = {
        "call to action": "cta",
        "call for action": "cta",
        "call-to-action": "cta",
        "closing": "cta",
        "intro": "intro",
        "introduction": "intro",
    }
    return aliases.get(normalized, normalized)


def _is_cta_section(value: str) -> bool:
    normalized = _normalize_section_name(value)
    return normalized == "cta" or any(name in normalized for name in CTA_SECTION_NAMES)


def _clean_heading(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().strip("*:_- ")).strip() or "Untitled"


def _clean(value: Any) -> str:
    return _WHITESPACE_RE.sub(" ", str(value or "").strip())


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
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


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


def _line_starts(text: str) -> list[int]:
    return [0, *[match.end() for match in re.finditer("\n", text)]]


def _line_column(line_starts: list[int], offset: int) -> tuple[int, int]:
    line_index = 0
    for index, start in enumerate(line_starts):
        if start > offset:
            break
        line_index = index
    return line_index + 1, offset - line_starts[line_index] + 1


def _offset_for_position(line_starts: list[int], line: int, column: int) -> int:
    index = max(0, min(line - 1, len(line_starts) - 1))
    return line_starts[index] + max(column, 0)
