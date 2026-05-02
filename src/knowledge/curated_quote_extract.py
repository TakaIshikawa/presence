"""Extract attributable quote candidates from curated knowledge sources."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
import io
import json
import re
import sqlite3
from typing import Any, Iterable


DEFAULT_MIN_CHARS = 80
DEFAULT_MAX_CHARS = 280
CURATED_SOURCE_PREFIX = "curated_"
QUOTE_CSV_FIELDS = (
    "source_id",
    "knowledge_id",
    "source_type",
    "title",
    "url",
    "author",
    "quote",
    "start_offset",
    "end_offset",
)

BOILERPLATE_PATTERNS = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\bsubscribe\b",
        r"\bunsubscribe\b",
        r"\bsign up\b",
        r"\blog in\b",
        r"\bprivacy policy\b",
        r"\bterms of (service|use)\b",
        r"\bcookie(s| policy)?\b",
        r"\ball rights reserved\b",
        r"\bcopyright\b",
        r"\bread more\b",
        r"\bview (this )?(email|message|newsletter) in (your )?browser\b",
        r"\bfollow us\b",
        r"\bshare this\b",
    )
)
SENTENCE_RE = re.compile(r"\S(?:.*?\S)?(?:[.!?](?=\s|$)|$)", re.DOTALL)
WHITESPACE_RE = re.compile(r"\s+")
WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9'-]*")


@dataclass(frozen=True)
class CuratedSourceRecord:
    """One curated knowledge source to scan for quote candidates."""

    source_id: str
    title: str | None
    url: str | None
    text: str
    knowledge_id: int | None = None
    source_type: str | None = None
    author: str | None = None

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> "CuratedSourceRecord":
        text = _first_text(data, ("text", "content", "body", "insight"))
        if not text:
            raise ValueError("fixture record is missing text/content")
        source_id = _clean_string(
            data.get("source_id")
            or data.get("id")
            or data.get("url")
            or data.get("source_url")
        )
        if not source_id:
            raise ValueError("fixture record is missing source_id/id/url")
        return cls(
            source_id=source_id,
            title=_clean_string(data.get("title") or data.get("headline")),
            url=_clean_string(data.get("url") or data.get("source_url")),
            text=text,
            knowledge_id=_int_or_none(data.get("knowledge_id")),
            source_type=_clean_string(data.get("source_type")),
            author=_clean_string(data.get("author")),
        )


@dataclass(frozen=True)
class QuoteCandidate:
    """One short attributable quote candidate."""

    source_id: str
    knowledge_id: int | None
    source_type: str | None
    title: str | None
    url: str | None
    author: str | None
    quote: str
    start_offset: int | None
    end_offset: int | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def extract_quote_candidates(
    records: Iterable[CuratedSourceRecord | dict[str, Any]],
    *,
    min_chars: int = DEFAULT_MIN_CHARS,
    max_chars: int = DEFAULT_MAX_CHARS,
) -> list[QuoteCandidate]:
    """Return de-duplicated quote candidates in deterministic source order."""
    if min_chars < 1:
        raise ValueError("min_chars must be positive")
    if max_chars < min_chars:
        raise ValueError("max_chars must be greater than or equal to min_chars")

    candidates: list[QuoteCandidate] = []
    seen: set[str] = set()
    for raw_record in records:
        record = (
            raw_record
            if isinstance(raw_record, CuratedSourceRecord)
            else CuratedSourceRecord.from_mapping(raw_record)
        )
        for sentence, start, end in segment_sentences(record.text):
            quote = normalize_quote_text(sentence)
            if not is_quote_candidate(quote, min_chars=min_chars, max_chars=max_chars):
                continue
            key = _dedupe_key(quote)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                QuoteCandidate(
                    source_id=record.source_id,
                    knowledge_id=record.knowledge_id,
                    source_type=record.source_type,
                    title=record.title,
                    url=record.url,
                    author=record.author,
                    quote=quote,
                    start_offset=start,
                    end_offset=end,
                )
            )
    return candidates


def segment_sentences(text: str) -> list[tuple[str, int, int]]:
    """Split text into sentence-like spans while retaining character offsets."""
    segments: list[tuple[str, int, int]] = []
    for line_match in re.finditer(r"[^\n\r]+", text or ""):
        line = line_match.group(0)
        line_offset = line_match.start()
        for match in SENTENCE_RE.finditer(line):
            raw = match.group(0)
            leading = len(raw) - len(raw.lstrip())
            trailing = len(raw.rstrip())
            sentence = raw.strip()
            if not sentence:
                continue
            start = line_offset + match.start() + leading
            end = line_offset + match.start() + trailing
            segments.append((sentence, start, end))
    return segments


def is_quote_candidate(
    text: str,
    *,
    min_chars: int = DEFAULT_MIN_CHARS,
    max_chars: int = DEFAULT_MAX_CHARS,
) -> bool:
    """Return whether a sentence is useful enough to export for review."""
    value = normalize_quote_text(text)
    if len(value) < min_chars or len(value) > max_chars:
        return False
    if any(pattern.search(value) for pattern in BOILERPLATE_PATTERNS):
        return False
    words = WORD_RE.findall(value)
    if len(words) < 8:
        return False
    alpha_chars = sum(1 for char in value if char.isalpha())
    if alpha_chars / max(len(value), 1) < 0.55:
        return False
    if value.count("http://") + value.count("https://"):
        return False
    if value.rstrip().endswith(":"):
        return False
    return True


def normalize_quote_text(text: str) -> str:
    """Collapse whitespace for stable line-level quote output."""
    return WHITESPACE_RE.sub(" ", (text or "").strip())


def format_curated_quotes_jsonl(candidates: Iterable[QuoteCandidate]) -> str:
    """Serialize quote candidates as deterministic JSON Lines."""
    lines = [
        json.dumps(candidate.to_dict(), sort_keys=True, ensure_ascii=False)
        for candidate in candidates
    ]
    return "\n".join(lines)


def format_curated_quotes_csv(candidates: Iterable[QuoteCandidate]) -> str:
    """Serialize quote candidates as deterministic CSV."""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=QUOTE_CSV_FIELDS, lineterminator="\n")
    writer.writeheader()
    for candidate in candidates:
        writer.writerow({field: candidate.to_dict().get(field) for field in QUOTE_CSV_FIELDS})
    return output.getvalue().rstrip("\n")


def load_fixture_records_from_paths(paths: Iterable[Any]) -> list[CuratedSourceRecord]:
    """Load JSON or JSONL fixture records from paths."""
    records: list[CuratedSourceRecord] = []
    for path_like in paths:
        path = path_like if hasattr(path_like, "read_text") else None
        if path is None:
            from pathlib import Path

            path = Path(path_like)
        text = path.read_text()
        loaded = _load_fixture_payload(text)
        records.extend(CuratedSourceRecord.from_mapping(item) for item in loaded)
    return sorted(records, key=lambda item: (item.source_id, item.knowledge_id or 0))


def load_curated_source_records(
    db_or_conn: Any,
    *,
    include_unapproved: bool = False,
) -> list[CuratedSourceRecord]:
    """Load curated source records from the knowledge table."""
    conn = _connection(db_or_conn)
    conn.row_factory = sqlite3.Row
    schema = _schema(conn)
    columns = schema.get("knowledge", set())
    if not columns:
        return []
    if not {"id", "source_type", "content"}.issubset(columns):
        raise ValueError("knowledge table must include id, source_type, and content")

    where = ["source_type LIKE ?"]
    params: list[Any] = [f"{CURATED_SOURCE_PREFIX}%"]
    if not include_unapproved and "approved" in columns:
        where.append("approved = 1")
    rows = conn.execute(
        f"""SELECT id,
                   source_type,
                   {_column_expr(columns, "source_id")},
                   {_column_expr(columns, "source_url")},
                   {_column_expr(columns, "author")},
                   content,
                   {_column_expr(columns, "insight")},
                   {_column_expr(columns, "metadata")}
            FROM knowledge
            WHERE {' AND '.join(where)}
              AND COALESCE(content, '') != ''
            ORDER BY source_type ASC, source_id ASC, id ASC""",
        params,
    ).fetchall()
    return [_record_from_knowledge_row(dict(row)) for row in rows]


def _record_from_knowledge_row(row: dict[str, Any]) -> CuratedSourceRecord:
    metadata = _parse_metadata(row.get("metadata"))
    title = _first_text(metadata, ("title", "link_title", "headline", "name"))
    source_url = _clean_string(row.get("source_url"))
    source_id = _clean_string(row.get("source_id") or source_url or row.get("id"))
    return CuratedSourceRecord(
        source_id=source_id or f"knowledge:{row['id']}",
        title=title or source_id or source_url,
        url=source_url,
        text=str(row.get("content") or ""),
        knowledge_id=_int_or_none(row.get("id")),
        source_type=_clean_string(row.get("source_type")),
        author=_clean_string(row.get("author")),
    )


def _load_fixture_payload(text: str) -> list[dict[str, Any]]:
    stripped = text.strip()
    if not stripped:
        return []
    if stripped.startswith("["):
        payload = json.loads(stripped)
        if not isinstance(payload, list):
            raise ValueError("fixture JSON must be an array of records")
        return [_require_mapping(item) for item in payload]
    return [_require_mapping(json.loads(line)) for line in stripped.splitlines() if line.strip()]


def _require_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("fixture records must be JSON objects")
    return value


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[table] = {info[1] for info in conn.execute(f"PRAGMA table_info({table})")}
    return schema


def _column_expr(columns: set[str], column: str) -> str:
    return column if column in columns else f"NULL AS {column}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    return conn


def _parse_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _first_text(data: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = _clean_string(data.get(key))
        if value:
            return value
    return None


def _clean_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _dedupe_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()
