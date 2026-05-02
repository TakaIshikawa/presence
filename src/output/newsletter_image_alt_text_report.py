"""Report newsletter image alt text coverage for accessibility review."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
import csv
import io
import json
import re
import sqlite3
from typing import Any, Iterable, Mapping


DEFAULT_DAYS = 30
TEXT_COLUMNS = ("body", "content", "html", "text", "markdown", "preview")
METADATA_TEXT_KEYS = frozenset(TEXT_COLUMNS)
CLASSIFICATIONS = ("missing_alt", "empty_alt", "generic_alt", "descriptive_alt")
ACTIONABLE_CLASSIFICATIONS = frozenset({"missing_alt", "empty_alt", "generic_alt"})
GENERIC_ALT_TEXT = frozenset(
    {
        "graphic",
        "hero",
        "image",
        "image 1",
        "img",
        "photo",
        "photo 1",
        "picture",
        "screenshot",
        "screenshot 1",
        "thumbnail",
    }
)

_MARKDOWN_IMAGE_RE = re.compile(
    r"!\[(?P<alt>[^\]]*)\]\(\s*(?P<src><[^>]*>|[^)\s]*)(?:\s+\"[^\"]*\")?\s*\)"
)
_MARKDOWN_HEADING_RE = re.compile(r"^\s{0,3}(#{1,6})\s+(.+?)\s*#*\s*$")
_WHITESPACE_RE = re.compile(r"\s+")
_GENERIC_NUMBERED_RE = re.compile(r"^(?:image|img|photo|picture|screenshot|graphic)\s*[-_#]?\s*\d+$")


@dataclass(frozen=True)
class NewsletterImageAltTextOccurrence:
    """One image found in newsletter Markdown or HTML."""

    source: str
    image_type: str
    src: str
    alt_text: str | None
    section: str
    title: str
    line: int
    column: int
    classification: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "image_type": self.image_type,
            "src": self.src,
            "alt_text": self.alt_text,
            "section": self.section,
            "title": self.title,
            "line": self.line,
            "column": self.column,
            "classification": self.classification,
        }


@dataclass(frozen=True)
class NewsletterImageAltTextReport:
    """Aggregated newsletter image alt text coverage."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    images: tuple[NewsletterImageAltTextOccurrence, ...]
    records: tuple[dict[str, Any], ...] = ()
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_image_alt_text_report",
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "totals": dict(self.totals),
            "images": [image.to_dict() for image in self.images],
            "records": [dict(record) for record in self.records],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def classify_image_alt_text(alt_text: str | None) -> str:
    """Classify image alt text into deterministic accessibility buckets."""
    if alt_text is None:
        return "missing_alt"
    clean = _clean(alt_text)
    if not clean:
        return "empty_alt"
    normalized = _normalize_alt(clean)
    if normalized in GENERIC_ALT_TEXT or _GENERIC_NUMBERED_RE.match(normalized):
        return "generic_alt"
    return "descriptive_alt"


def extract_newsletter_image_alt_text_occurrences(
    text: str,
    *,
    source: str = "text",
    title: str = "",
) -> tuple[NewsletterImageAltTextOccurrence, ...]:
    """Extract Markdown and HTML image alt text with local section context."""
    value = str(text or "")
    occurrences = [
        *_extract_markdown_images(value, source=source, title=title),
        *_HtmlImageAltTextParser.images_from(value, source=source, title=title),
    ]
    occurrences.sort(key=lambda item: (item.line, item.column, item.src, item.image_type))
    return tuple(occurrences)


def build_newsletter_image_alt_text_report_for_text(
    text: str,
    *,
    newsletter_id: str = "text",
    item_type: str = "text",
    subject: str = "",
    title: str = "",
    status: str = "",
    item_timestamp: str = "",
    include_descriptive: bool = False,
    now: datetime | None = None,
) -> NewsletterImageAltTextReport:
    """Build an alt text report for one rendered newsletter body."""
    generated_at = _as_utc(now or datetime.now(timezone.utc)).isoformat()
    record = {
        "newsletter_id": newsletter_id,
        "item_type": item_type,
        "subject": subject,
        "title": title,
        "status": status,
        "item_timestamp": item_timestamp,
    }
    all_images = extract_newsletter_image_alt_text_occurrences(
        text,
        source=f"{item_type}:{newsletter_id}",
        title=title or subject,
    )
    images = _filter_images(all_images, include_descriptive=include_descriptive)
    return _report(
        generated_at=generated_at,
        filters={"source": "text", "include_descriptive": include_descriptive},
        records=(record,),
        images=images,
        all_images=all_images,
    )


def build_newsletter_image_alt_text_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    include_descriptive: bool = False,
    now: datetime | None = None,
) -> NewsletterImageAltTextReport:
    """Load recent newsletter drafts/sends and report image alt text coverage."""
    if days <= 0:
        raise ValueError("days must be positive")

    conn = _connection(db_or_conn)
    conn.row_factory = sqlite3.Row
    schema = _schema(conn)
    generated = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated - timedelta(days=days)
    filters = {
        "days": days,
        "updated_after": cutoff.isoformat(),
        "include_descriptive": include_descriptive,
    }
    missing_tables, missing_columns = _schema_gaps(schema)
    if "newsletter_sends" in missing_tables and "generated_content" in missing_tables:
        return _empty_report(generated.isoformat(), filters, missing_tables, missing_columns)
    if missing_columns:
        return _empty_report(generated.isoformat(), filters, missing_tables, missing_columns)

    rows = _load_newsletter_rows(conn, schema, cutoff)
    records: list[dict[str, Any]] = []
    all_images: list[NewsletterImageAltTextOccurrence] = []
    for row in rows:
        record = {
            "newsletter_id": str(row["newsletter_id"]),
            "item_type": str(row["item_type"]),
            "subject": _clean(row.get("subject")),
            "title": _clean(row.get("title")),
            "status": _clean(row.get("status")),
            "item_timestamp": _clean(row.get("item_timestamp")),
        }
        records.append(record)
        title = record["title"] or record["subject"]
        for source_name, body in row.get("texts", ()):
            scoped_source = f"{record['item_type']}:{record['newsletter_id']}:{source_name}"
            all_images.extend(
                extract_newsletter_image_alt_text_occurrences(
                    body,
                    source=scoped_source,
                    title=title,
                )
            )

    images = _filter_images(tuple(all_images), include_descriptive=include_descriptive)
    return _report(
        generated_at=generated.isoformat(),
        filters=filters,
        records=tuple(records),
        images=images,
        all_images=tuple(all_images),
    )


def format_newsletter_image_alt_text_json(report: NewsletterImageAltTextReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_image_alt_text_csv(report: NewsletterImageAltTextReport) -> str:
    """Render report rows as stable CSV."""
    fieldnames = [
        "source",
        "image_type",
        "classification",
        "src",
        "alt_text",
        "section",
        "title",
        "line",
        "column",
    ]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for image in report.images:
        row = image.to_dict()
        writer.writerow({key: row[key] for key in fieldnames})
    return buffer.getvalue().rstrip("\n")


def _extract_markdown_images(
    text: str,
    *,
    source: str,
    title: str,
) -> list[NewsletterImageAltTextOccurrence]:
    line_starts = _line_starts(text)
    headings = _markdown_headings(text)
    images: list[NewsletterImageAltTextOccurrence] = []
    for match in _MARKDOWN_IMAGE_RE.finditer(text):
        raw_src = match.group("src")
        if raw_src.startswith("<") and raw_src.endswith(">"):
            raw_src = raw_src[1:-1]
        line, column = _line_column(line_starts, match.start())
        alt_text = match.group("alt")
        images.append(
            NewsletterImageAltTextOccurrence(
                source=source,
                image_type="markdown",
                src=raw_src.strip(),
                alt_text=alt_text,
                section=_section_for_line(headings, line),
                title=title,
                line=line,
                column=column,
                classification=classify_image_alt_text(alt_text),
            )
        )
    return images


class _HtmlImageAltTextParser(HTMLParser):
    def __init__(self, *, source: str, title: str) -> None:
        super().__init__(convert_charrefs=True)
        self.source = source
        self.title = title
        self.images: list[NewsletterImageAltTextOccurrence] = []
        self.section = ""
        self._heading_tag: str | None = None
        self._heading_parts: list[str] = []
        self._title_tag = False
        self._title_parts: list[str] = []

    @classmethod
    def images_from(
        cls,
        html: str,
        *,
        source: str,
        title: str,
    ) -> list[NewsletterImageAltTextOccurrence]:
        parser = cls(source=source, title=title)
        parser.feed(html)
        parser.close()
        return parser.images

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag_name = tag.casefold()
        if tag_name in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._heading_tag = tag_name
            self._heading_parts = []
            return
        if tag_name == "title":
            self._title_tag = True
            self._title_parts = []
            return
        if tag_name != "img":
            return
        attr_map = {name.casefold(): value for name, value in attrs}
        line, column = self.getpos()
        alt_text = attr_map.get("alt")
        self.images.append(
            NewsletterImageAltTextOccurrence(
                source=self.source,
                image_type="html",
                src=(attr_map.get("src") or "").strip(),
                alt_text=alt_text,
                section=self.section,
                title=self.title or _clean(" ".join(self._title_parts)),
                line=line,
                column=column + 1,
                classification=classify_image_alt_text(alt_text),
            )
        )

    def handle_endtag(self, tag: str) -> None:
        tag_name = tag.casefold()
        if tag_name == self._heading_tag:
            heading = _clean(" ".join(self._heading_parts))
            if heading:
                self.section = heading
            self._heading_tag = None
            self._heading_parts = []
        elif tag_name == "title":
            self._title_tag = False

    def handle_data(self, data: str) -> None:
        clean = _clean(data)
        if not clean:
            return
        if self._heading_tag is not None:
            self._heading_parts.append(clean)
        if self._title_tag:
            self._title_parts.append(clean)


def _filter_images(
    images: Iterable[NewsletterImageAltTextOccurrence],
    *,
    include_descriptive: bool,
) -> tuple[NewsletterImageAltTextOccurrence, ...]:
    filtered = [
        image
        for image in images
        if include_descriptive or image.classification in ACTIONABLE_CLASSIFICATIONS
    ]
    return tuple(
        sorted(
            filtered,
            key=lambda image: (
                image.source,
                image.line,
                image.column,
                image.classification,
                image.src,
            ),
        )
    )


def _report(
    *,
    generated_at: str,
    filters: dict[str, Any],
    records: tuple[dict[str, Any], ...],
    images: tuple[NewsletterImageAltTextOccurrence, ...],
    all_images: tuple[NewsletterImageAltTextOccurrence, ...],
) -> NewsletterImageAltTextReport:
    totals = {
        "record_count": len(records),
        "image_count": len(images),
        "total_image_count": len(all_images),
        "actionable_image_count": sum(
            1 for image in all_images if image.classification in ACTIONABLE_CLASSIFICATIONS
        ),
        "classification_totals": _classification_totals(all_images),
    }
    return NewsletterImageAltTextReport(
        generated_at=generated_at,
        filters=filters,
        totals=totals,
        records=records,
        images=images,
    )


def _empty_report(
    generated_at: str,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> NewsletterImageAltTextReport:
    return NewsletterImageAltTextReport(
        generated_at=generated_at,
        filters=filters,
        totals={
            "record_count": 0,
            "image_count": 0,
            "total_image_count": 0,
            "actionable_image_count": 0,
            "classification_totals": _classification_totals(()),
        },
        images=(),
        records=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _classification_totals(
    images: Iterable[NewsletterImageAltTextOccurrence],
) -> dict[str, int]:
    counts = Counter(image.classification for image in images)
    return {classification: counts.get(classification, 0) for classification in CLASSIFICATIONS}


def _load_newsletter_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rows.extend(_load_send_rows(conn, schema, cutoff))
    rows.extend(_load_generated_rows(conn, schema, cutoff))
    rows.sort(
        key=lambda row: (
            row.get("item_timestamp") or "",
            str(row["item_type"]),
            str(row["newsletter_id"]),
        ),
        reverse=True,
    )
    return rows


def _load_send_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
) -> list[dict[str, Any]]:
    if "newsletter_sends" not in schema:
        return []
    columns = schema["newsletter_sends"]
    selected = _selected_send_columns(columns)
    timestamp_expr = _send_timestamp_expr(columns)
    where = ""
    params: tuple[Any, ...] = ()
    if timestamp_expr != "id":
        where = f"WHERE ({timestamp_expr} IS NULL OR datetime({timestamp_expr}) >= datetime(?))"
        params = (cutoff.isoformat(),)
    cursor = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM newsletter_sends
            {where}
            ORDER BY datetime({timestamp_expr}) DESC, id DESC""",
        params,
    )
    return [_send_row(_row_dict(cursor, row), columns) for row in cursor.fetchall()]


def _load_generated_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []
    columns = schema["generated_content"]
    selected = _selected_generated_columns(columns)
    timestamp_expr = "created_at" if "created_at" in columns else "id"
    where = [_newsletter_content_predicate(columns)]
    params: list[Any] = []
    if timestamp_expr != "id":
        where.append(f"({timestamp_expr} IS NULL OR datetime({timestamp_expr}) >= datetime(?))")
        params.append(cutoff.isoformat())
    cursor = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM generated_content
            WHERE {' AND '.join(where)}
            ORDER BY datetime({timestamp_expr}) DESC, id DESC""",
        tuple(params),
    )
    return [_generated_row(_row_dict(cursor, row), columns) for row in cursor.fetchall()]


def _send_row(row: Mapping[str, Any], columns: set[str]) -> dict[str, Any]:
    metadata = _parse_json(row.get("metadata")) if "metadata" in columns else None
    texts = _row_texts(row, columns)
    texts.extend(_metadata_texts(metadata, prefix="metadata"))
    return {
        "newsletter_id": row.get("issue_id") or row.get("id"),
        "item_type": "newsletter_send",
        "subject": row.get("subject") or "",
        "title": "",
        "status": row.get("status") or "",
        "item_timestamp": row.get("sent_at")
        or row.get("updated_at")
        or row.get("created_at")
        or "",
        "texts": texts,
    }


def _generated_row(row: Mapping[str, Any], columns: set[str]) -> dict[str, Any]:
    metadata = _parse_json(row.get("metadata")) if "metadata" in columns else None
    texts = _row_texts(row, columns)
    texts.extend(_metadata_texts(metadata, prefix="metadata"))
    return {
        "newsletter_id": row.get("id"),
        "item_type": "generated_content",
        "subject": "",
        "title": row.get("title") or "",
        "status": row.get("curation_quality") or "",
        "item_timestamp": row.get("created_at") or "",
        "texts": texts,
    }


def _selected_send_columns(columns: set[str]) -> list[str]:
    return [
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
            "sent_at",
        )
        if column in columns
    ]


def _selected_generated_columns(columns: set[str]) -> list[str]:
    return [
        column
        for column in (
            "id",
            "content_type",
            "title",
            *TEXT_COLUMNS,
            "metadata",
            "curation_quality",
            "created_at",
        )
        if column in columns
    ]


def _row_texts(row: Mapping[str, Any], columns: set[str]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for key in ("body", "content", "html", "text", "markdown", "preview"):
        if key in columns and row.get(key):
            texts.append((key, str(row[key])))
    return texts


def _metadata_texts(value: Any, *, prefix: str) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            child_prefix = f"{prefix}.{key}"
            key_lower = str(key).casefold()
            if isinstance(item, str) and any(marker in key_lower for marker in METADATA_TEXT_KEYS):
                texts.append((child_prefix, item))
            elif isinstance(item, (Mapping, list, tuple)):
                texts.extend(_metadata_texts(item, prefix=child_prefix))
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            child_prefix = f"{prefix}[{index}]"
            if isinstance(item, str):
                texts.append((child_prefix, item))
            elif isinstance(item, (Mapping, list, tuple)):
                texts.extend(_metadata_texts(item, prefix=child_prefix))
    return texts


def _newsletter_content_predicate(columns: set[str]) -> str:
    if "content_type" not in columns:
        return "1 = 1"
    return "LOWER(COALESCE(content_type, '')) LIKE '%newsletter%'"


def _send_timestamp_expr(columns: set[str]) -> str:
    candidates = [column for column in ("sent_at", "updated_at", "created_at") if column in columns]
    if not candidates:
        return "id"
    return "COALESCE(" + ", ".join(candidates) + ")"


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    missing_tables = tuple(
        table for table in ("newsletter_sends", "generated_content") if table not in schema
    )
    missing_columns: dict[str, tuple[str, ...]] = {}
    if "newsletter_sends" in schema and "id" not in schema["newsletter_sends"]:
        missing_columns["newsletter_sends"] = ("id",)
    if "generated_content" in schema and "id" not in schema["generated_content"]:
        missing_columns["generated_content"] = ("id",)
    return missing_tables, missing_columns


def _markdown_headings(text: str) -> list[tuple[int, str]]:
    headings: list[tuple[int, str]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        match = _MARKDOWN_HEADING_RE.match(line)
        if match:
            headings.append((line_number, _clean(match.group(2))))
    return headings


def _section_for_line(headings: list[tuple[int, str]], line: int) -> str:
    section = ""
    for heading_line, heading in headings:
        if heading_line >= line:
            break
        section = heading
    return section


def _line_starts(text: str) -> list[int]:
    return [0, *[match.end() for match in re.finditer("\n", text)]]


def _line_column(line_starts: list[int], offset: int) -> tuple[int, int]:
    line_index = 0
    for index, start in enumerate(line_starts):
        if start > offset:
            break
        line_index = index
    return line_index + 1, offset - line_starts[line_index] + 1


def _normalize_alt(value: str) -> str:
    lowered = value.casefold()
    stripped = re.sub(r"[^\w\s#-]+", " ", lowered)
    return _clean(stripped)


def _clean(value: Any) -> str:
    return _WHITESPACE_RE.sub(" ", str(value or "").strip())


def _parse_json(raw_value: Any) -> Any:
    if raw_value is None or raw_value == "":
        return None
    if isinstance(raw_value, (Mapping, list)):
        return raw_value
    try:
        return json.loads(str(raw_value))
    except (TypeError, json.JSONDecodeError):
        return None


def _as_utc(value: datetime) -> datetime:
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
