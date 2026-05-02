"""Suggest deterministic meta descriptions for generated blog posts."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import csv
from io import StringIO
import json
from pathlib import Path
import re
import sqlite3
from typing import Any, Iterable, Mapping

from .blog_frontmatter_validator import parse_markdown_frontmatter


DEFAULT_MIN_CHARS = 120
DEFAULT_MAX_CHARS = 160
BLOG_CONTENT_TYPES = {"blog", "blog_post", "long_post"}


@dataclass(frozen=True)
class BlogMetaDescriptionRow:
    """One exported blog meta-description suggestion."""

    slug: str
    title: str
    suggested_meta_description: str
    character_count: int
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["warnings"] = list(self.warnings)
        return data


def export_blog_meta_descriptions(
    records_or_db: Any,
    *,
    min_chars: int = DEFAULT_MIN_CHARS,
    max_chars: int = DEFAULT_MAX_CHARS,
) -> list[BlogMetaDescriptionRow]:
    """Return deterministic meta-description rows from blog records or a database."""

    _validate_bounds(min_chars, max_chars)
    records = _load_records(records_or_db)
    rows = [
        _description_row(_coerce_record(record), min_chars=min_chars, max_chars=max_chars)
        for record in records
    ]
    return sorted(rows, key=lambda row: (row.slug, row.title))


def export_blog_meta_descriptions_from_markdown(
    paths: Iterable[str | Path],
    *,
    min_chars: int = DEFAULT_MIN_CHARS,
    max_chars: int = DEFAULT_MAX_CHARS,
) -> list[BlogMetaDescriptionRow]:
    """Return meta-description rows from markdown draft files."""

    _validate_bounds(min_chars, max_chars)
    return sorted(
        (
            _description_row(
                _record_from_markdown_path(Path(path)),
                min_chars=min_chars,
                max_chars=max_chars,
            )
            for path in paths
        ),
        key=lambda row: (row.slug, row.title),
    )


def format_blog_meta_descriptions_json(rows: list[BlogMetaDescriptionRow]) -> str:
    """Render export rows as deterministic JSON."""

    return json.dumps([row.to_dict() for row in rows], indent=2, sort_keys=True)


def format_blog_meta_descriptions_csv(rows: list[BlogMetaDescriptionRow]) -> str:
    """Render export rows as CSV with stable columns."""

    output = StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "slug",
            "title",
            "suggested_meta_description",
            "character_count",
            "warnings",
        ],
        lineterminator="\n",
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {
                "slug": row.slug,
                "title": row.title,
                "suggested_meta_description": row.suggested_meta_description,
                "character_count": row.character_count,
                "warnings": ";".join(row.warnings),
            }
        )
    return output.getvalue().rstrip("\n")


def _description_row(
    record: dict[str, Any],
    *,
    min_chars: int,
    max_chars: int,
) -> BlogMetaDescriptionRow:
    title = _clean_inline_text(record.get("title"))
    slug = _clean_inline_text(record.get("slug")) or _slugify(title) or _clean_inline_text(record.get("id"))
    warnings: list[str] = []
    if not title:
        warnings.append("missing_title")
        title = ""
    candidates = _candidate_texts(record)
    source_text = " ".join(candidates)
    description, duplicated_title = _select_description(
        candidates,
        title=title,
        min_chars=min_chars,
        max_chars=max_chars,
    )
    if duplicated_title:
        warnings.append("title_duplication")
    if len(_clean_inline_text(source_text)) < min_chars or len(description) < min_chars:
        warnings.append("too_short_content")
    warnings = _dedupe(warnings)
    return BlogMetaDescriptionRow(
        slug=slug or "untitled",
        title=title,
        suggested_meta_description=description,
        character_count=len(description),
        warnings=tuple(warnings),
    )


def _select_description(
    candidates: list[str],
    *,
    title: str,
    min_chars: int,
    max_chars: int,
) -> tuple[str, bool]:
    duplicated_title = False
    cleaned_candidates: list[str] = []
    for candidate in candidates:
        cleaned = _clean_inline_text(candidate)
        if not cleaned:
            continue
        without_title, removed = _remove_title_text(cleaned, title)
        duplicated_title = duplicated_title or removed
        if without_title:
            cleaned_candidates.append(without_title)

    if not cleaned_candidates:
        return "", duplicated_title

    for candidate in cleaned_candidates:
        if min_chars <= len(candidate) <= max_chars:
            return candidate, duplicated_title

    for candidate in cleaned_candidates:
        if len(candidate) > max_chars:
            return _truncate_description(candidate, max_chars), duplicated_title

    combined = ""
    for candidate in cleaned_candidates:
        next_value = candidate if not combined else f"{combined} {candidate}"
        if len(next_value) > max_chars:
            break
        combined = next_value
        if len(combined) >= min_chars:
            return combined, duplicated_title
    return combined or cleaned_candidates[0], duplicated_title


def _candidate_texts(record: dict[str, Any]) -> list[str]:
    texts: list[str] = []
    for key in ("excerpt", "description", "summary"):
        value = record.get(key)
        if value:
            texts.append(str(value))
    for quote in _pull_quotes(record):
        texts.append(quote)
    texts.extend(_opening_paragraphs(str(record.get("body") or record.get("content") or "")))
    return _dedupe(texts)


def _pull_quotes(record: dict[str, Any]) -> list[str]:
    values: list[Any] = []
    for key in ("pull_quotes", "pull_quote"):
        value = record.get(key)
        if isinstance(value, (list, tuple)):
            values.extend(value)
        elif value:
            values.append(value)
    metadata = _decode_json_object(record.get("metadata"))
    for key in ("pull_quotes", "pull_quote"):
        value = metadata.get(key)
        if isinstance(value, (list, tuple)):
            values.extend(value)
        elif value:
            values.append(value)
    return [_clean_inline_text(value) for value in values if _clean_inline_text(value)]


def _opening_paragraphs(markdown: str, *, limit: int = 3) -> list[str]:
    paragraphs: list[str] = []
    pending: list[str] = []
    in_code = False
    for raw_line in markdown.replace("\r\n", "\n").replace("\r", "\n").splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("```"):
            in_code = not in_code
            if pending:
                paragraphs.append(" ".join(pending))
                pending = []
            continue
        if in_code:
            continue
        if not stripped:
            if pending:
                paragraphs.append(" ".join(pending))
                pending = []
            continue
        if _skip_markdown_line(stripped):
            if pending:
                paragraphs.append(" ".join(pending))
                pending = []
            continue
        pending.append(stripped)
        if len(paragraphs) >= limit:
            break
    if pending and len(paragraphs) < limit:
        paragraphs.append(" ".join(pending))
    return [_clean_inline_text(paragraph) for paragraph in paragraphs[:limit]]


def _skip_markdown_line(line: str) -> bool:
    return (
        bool(re.match(r"^#{1,6}\s+\S", line))
        or bool(re.match(r"^TITLE:\s*\S", line, re.I))
        or bool(re.match(r"^[-*_]{3,}$", line))
        or bool(re.match(r"^\s*[-*+]\s*$", line))
        or line.lower() in {"table of contents", "related posts", "read more"}
    )


def _clean_inline_text(value: Any) -> str:
    text = str(value or "")
    text = re.sub(r"^>\s*", "", text.strip())
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)
    text = re.sub(r"\*([^*]+)\*", r"\1", text)
    text = re.sub(r"!\[[^\]]*\]\([^)]+\)", "", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip(" -")


def _remove_title_text(text: str, title: str) -> tuple[str, bool]:
    if not title:
        return text, False
    normalized_title = _normalize_for_match(title)
    if not normalized_title:
        return text, False
    duplicated = normalized_title in _normalize_for_match(text)
    escaped = re.escape(title)
    without = re.sub(rf"^\s*{escaped}\s*[:\-|–—]\s*", "", text, flags=re.I)
    if _normalize_for_match(without) == normalized_title:
        without = ""
    return without.strip(), duplicated


def _truncate_description(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    sentence = re.split(r"(?<=[.!?])\s+", text, maxsplit=1)[0].strip()
    if 0 < len(sentence) <= max_chars:
        return sentence
    clipped = text[:max_chars].rstrip()
    if " " in clipped:
        clipped = clipped.rsplit(" ", 1)[0].rstrip(",;:")
    return clipped.strip()


def _coerce_record(record: Mapping[str, Any]) -> dict[str, Any]:
    metadata = _decode_json_object(record.get("metadata")) | _decode_json_object(
        record.get("variant_metadata")
    )
    content = str(record.get("content") or record.get("variant_content") or "")
    title, body = _title_body_from_content(content)
    title = _clean_inline_text(
        record.get("title")
        or metadata.get("title")
        or title
    )
    slug = _clean_inline_text(record.get("slug") or metadata.get("slug") or _slugify(title))
    return {
        "id": record.get("id"),
        "slug": slug,
        "title": title,
        "excerpt": record.get("excerpt") or metadata.get("excerpt"),
        "description": record.get("description") or metadata.get("description"),
        "summary": record.get("summary") or metadata.get("summary"),
        "pull_quote": record.get("pull_quote") or metadata.get("pull_quote"),
        "pull_quotes": record.get("pull_quotes") or metadata.get("pull_quotes"),
        "metadata": metadata,
        "body": record.get("body") or body,
        "content": content,
    }


def _record_from_markdown_path(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if text.startswith("---\n"):
        frontmatter, body, _issues = parse_markdown_frontmatter(text, path=str(path))
    else:
        frontmatter = {}
        body = text
    title, fallback_body = _title_body_from_content(body)
    return {
        "slug": frontmatter.get("slug") or path.stem,
        "title": frontmatter.get("title") or title,
        "excerpt": frontmatter.get("excerpt"),
        "description": frontmatter.get("description"),
        "summary": frontmatter.get("summary"),
        "pull_quote": frontmatter.get("pull_quote"),
        "pull_quotes": frontmatter.get("pull_quotes"),
        "body": fallback_body,
    }


def _title_body_from_content(content: str) -> tuple[str, str]:
    title_match = re.search(r"^TITLE:\s*(.+)$", content, flags=re.MULTILINE)
    if title_match:
        return title_match.group(1).strip(), content[title_match.end():].strip()
    heading_match = re.search(r"^#\s+(.+)$", content, flags=re.MULTILINE)
    if heading_match:
        return heading_match.group(1).strip(), content[heading_match.end():].strip()
    return "", content.strip()


def _load_records(records_or_db: Any) -> list[dict[str, Any]]:
    if isinstance(records_or_db, (list, tuple)):
        return [dict(record) for record in records_or_db]
    conn = getattr(records_or_db, "conn", records_or_db)
    if not isinstance(conn, sqlite3.Connection):
        return [dict(record) for record in records_or_db]
    return _load_database_records(conn)


def _load_database_records(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    schema = _schema(conn)
    if "generated_content" not in schema or not {"id", "content_type", "content"}.issubset(
        schema["generated_content"]
    ):
        return []
    generated_columns = schema["generated_content"]
    select_columns = [
        "gc.id",
        "gc.content_type",
        "gc.content",
        _column_expr(generated_columns, "created_at", alias="created_at"),
    ]
    joins = ""
    blog_variant_filter = ""
    if "content_variants" in schema and {"content_id", "content"}.issubset(
        schema["content_variants"]
    ):
        cv_columns = schema["content_variants"]
        variant_select = [
            "content_id",
            "content AS variant_content",
            _column_expr(
                cv_columns,
                "metadata",
                "NULL",
                table_alias=None,
                alias="variant_metadata",
            ),
            "ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY id DESC) AS rn",
        ]
        joins = (
            "LEFT JOIN (SELECT "
            + ", ".join(variant_select)
            + " FROM content_variants WHERE platform = 'blog' OR variant_type LIKE '%blog%') cv "
            + "ON cv.content_id = gc.id AND cv.rn = 1"
        )
        select_columns.extend(["cv.variant_content", "cv.variant_metadata"])
        blog_variant_filter = " OR cv.content_id IS NOT NULL"
    order_column = "gc.created_at" if "created_at" in generated_columns else "gc.id"
    rows = conn.execute(
        f"""SELECT {", ".join(select_columns)}
              FROM generated_content gc
              {joins}
             WHERE gc.content_type IN ({", ".join("?" for _ in BLOG_CONTENT_TYPES)})
                {blog_variant_filter}
             ORDER BY {order_column} DESC, gc.id DESC""",
        tuple(sorted(BLOG_CONTENT_TYPES)),
    ).fetchall()
    return [dict(row) for row in rows]


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    return {
        table: {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        for table in tables
    }


def _column_expr(
    columns: set[str],
    column: str,
    default: str = "NULL",
    *,
    table_alias: str | None = "gc",
    alias: str | None = None,
) -> str:
    prefix = f"{table_alias}." if table_alias else ""
    expression = f"{prefix}{column}" if column in columns else default
    return f"{expression} AS {alias or column}"


def _decode_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9\s-]", "", str(text or "").lower())
    slug = re.sub(r"[\s_]+", "-", slug)
    return re.sub(r"-+", "-", slug).strip("-")


def _normalize_for_match(text: str) -> str:
    text = re.sub(r"[^a-z0-9\s]", " ", str(text or "").casefold())
    return re.sub(r"\s+", " ", text).strip()


def _dedupe(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value).strip()
        key = _normalize_for_match(text)
        if text and key not in seen:
            seen.add(key)
            result.append(text)
    return result


def _validate_bounds(min_chars: int, max_chars: int) -> None:
    if min_chars <= 0:
        raise ValueError("min_chars must be positive")
    if max_chars <= 0:
        raise ValueError("max_chars must be positive")
    if min_chars > max_chars:
        raise ValueError("min_chars must be less than or equal to max_chars")
