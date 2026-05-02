"""Export short pull-quote candidates from generated blog posts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import re
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_CHARS = 80
DEFAULT_MAX_CHARS = 240
DEFAULT_LIMIT = 20
SOURCE_NAME = "blog_pull_quote"


@dataclass(frozen=True)
class BlogPullQuoteCandidate:
    source_content_id: int
    quote: str
    char_count: int
    created_at: str
    position: int

    @property
    def source_metadata(self) -> dict[str, Any]:
        return {
            "source": SOURCE_NAME,
            "source_content_id": self.source_content_id,
            "quote": self.quote,
            "char_count": self.char_count,
            "position": self.position,
            "created_at": self.created_at,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_content_id": self.source_content_id,
            "quote": self.quote,
            "char_count": self.char_count,
            "created_at": self.created_at,
            "position": self.position,
        }


@dataclass(frozen=True)
class BlogPullQuoteIdeaResult:
    status: str
    source_content_id: int
    quote: str
    char_count: int
    idea_id: int | None
    reason: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "source_content_id": self.source_content_id,
            "quote": self.quote,
            "char_count": self.char_count,
            "idea_id": self.idea_id,
            "reason": self.reason,
            "source_metadata": self.source_metadata,
        }


def extract_blog_pull_quote_candidates(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_chars: int = DEFAULT_MIN_CHARS,
    max_chars: int = DEFAULT_MAX_CHARS,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> list[BlogPullQuoteCandidate]:
    """Return deterministic pull-quote candidates from recent blog posts."""
    _validate_bounds(days=days, min_chars=min_chars, max_chars=max_chars, limit=limit)
    rows = _fetch_blog_rows(db_or_conn, days=days, now=now)
    candidates: list[BlogPullQuoteCandidate] = []
    for row in rows:
        quotes = _extract_quotes(
            str(row.get("content") or ""),
            min_chars=min_chars,
            max_chars=max_chars,
        )
        for position, quote in enumerate(quotes, start=1):
            candidates.append(
                BlogPullQuoteCandidate(
                    source_content_id=int(row["id"]),
                    quote=quote,
                    char_count=len(quote),
                    created_at=str(row.get("created_at") or ""),
                    position=position,
                )
            )
            if limit is not None and len(candidates) >= limit:
                return candidates
    return candidates


def export_blog_pull_quotes(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_chars: int = DEFAULT_MIN_CHARS,
    max_chars: int = DEFAULT_MAX_CHARS,
    limit: int | None = DEFAULT_LIMIT,
    create_ideas: bool = False,
    now: datetime | None = None,
) -> list[BlogPullQuoteIdeaResult]:
    """Preview or create content ideas from blog pull-quote candidates."""
    candidates = extract_blog_pull_quote_candidates(
        db_or_conn,
        days=days,
        min_chars=min_chars,
        max_chars=max_chars,
        limit=limit,
        now=now,
    )
    results: list[BlogPullQuoteIdeaResult] = []
    for candidate in candidates:
        duplicate = _find_duplicate(db_or_conn, candidate)
        if duplicate is not None:
            results.append(
                _result("skipped", candidate, int(duplicate["id"]), f"{duplicate['status']} duplicate")
            )
            continue
        if not create_ideas:
            results.append(_result("candidate", candidate, None, "read-only"))
            continue
        idea_id = _insert_content_idea(db_or_conn, candidate)
        results.append(_result("created", candidate, idea_id, "created"))
    return results


def format_blog_pull_quotes_json(results: list[BlogPullQuoteIdeaResult]) -> str:
    return json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True)


def format_blog_pull_quotes_text(results: list[BlogPullQuoteIdeaResult]) -> str:
    created = sum(1 for result in results if result.status == "created")
    candidates = sum(1 for result in results if result.status == "candidate")
    skipped = sum(1 for result in results if result.status == "skipped")
    lines = [f"created={created} candidate={candidates} skipped={skipped}"]
    lines.append(f"{'Status':9s}  {'ID':>4s}  {'Source':>6s}  {'Chars':>5s}  Quote")
    lines.append(
        f"{'-' * 9:9s}  {'-' * 4:>4s}  {'-' * 6:>6s}  "
        f"{'-' * 5:>5s}  {'-' * 40}"
    )
    if not results:
        lines.append("none       -           -      0  no qualifying blog pull quotes")
        return "\n".join(lines)
    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        lines.append(
            f"{result.status:9s}  {idea_id:>4s}  {result.source_content_id:6d}  "
            f"{result.char_count:5d}  {_shorten(result.quote, 96)}"
        )
    return "\n".join(lines)


def _extract_quotes(content: str, *, min_chars: int, max_chars: int) -> list[str]:
    quotes: list[str] = []
    for block in _markdown_text_blocks(content):
        text = _clean_candidate_text(block)
        if not _is_usable_text(text):
            continue
        if min_chars <= len(text) <= max_chars:
            quotes.append(text)
            continue
        if len(text) > max_chars:
            for sentence in _sentences(text):
                if min_chars <= len(sentence) <= max_chars and _is_usable_text(sentence):
                    quotes.append(sentence)
    return _dedupe_preserving_order(quotes)


def _markdown_text_blocks(content: str) -> list[str]:
    blocks: list[str] = []
    pending: list[str] = []
    in_code_block = False
    for raw_line in content.splitlines():
        line = raw_line.rstrip()
        if line.strip().startswith("```"):
            in_code_block = not in_code_block
            if pending:
                blocks.append(" ".join(pending))
                pending = []
            continue
        if in_code_block:
            continue
        stripped = line.strip()
        if not stripped:
            if pending:
                blocks.append(" ".join(pending))
                pending = []
            continue
        if _is_rejected_line(stripped):
            if pending:
                blocks.append(" ".join(pending))
                pending = []
            continue
        pending.append(stripped)
    if pending:
        blocks.append(" ".join(pending))
    return blocks


def _is_rejected_line(line: str) -> bool:
    if re.match(r"^#{1,6}\s+\S", line):
        return True
    if re.match(r"^[-*_]{3,}$", line):
        return True
    if re.match(r"^\s*[-*+]\s*$", line):
        return True
    return _is_link_only(line) or _is_boilerplate(line)


def _is_usable_text(text: str) -> bool:
    return bool(text) and not _is_boilerplate(text) and not _is_link_only(text)


def _is_link_only(text: str) -> bool:
    normalized = text.strip()
    if re.fullmatch(r"https?://\S+", normalized):
        return True
    return bool(re.fullmatch(r"\[[^\]]+\]\([^)]+\)", normalized))


def _is_boilerplate(text: str) -> bool:
    normalized = re.sub(r"[^a-z0-9 ]+", " ", text.lower())
    normalized = " ".join(normalized.split())
    boilerplate = (
        "table of contents",
        "related posts",
        "read more",
        "share this",
        "subscribe",
        "sign up",
        "newsletter",
        "leave a comment",
        "thanks for reading",
        "back to top",
    )
    return any(phrase in normalized for phrase in boilerplate)


def _clean_candidate_text(text: str) -> str:
    text = re.sub(r"^>\s*", "", text.strip())
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _sentences(text: str) -> list[str]:
    parts = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9\"'])", text)
    return [_clean_candidate_text(part) for part in parts if _clean_candidate_text(part)]


def _fetch_blog_rows(db_or_conn: Any, *, days: int, now: datetime | None) -> list[dict[str, Any]]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "generated_content" not in schema:
        return []
    required = {"id", "content_type", "content"}
    if not required.issubset(schema["generated_content"]):
        return []
    timestamp_column = "created_at" if "created_at" in schema["generated_content"] else "id"
    start = _ensure_utc(now or datetime.now(timezone.utc)) - timedelta(days=days)
    selected = ["id", "content"]
    if "created_at" in schema["generated_content"]:
        selected.append("created_at")
    else:
        selected.append("'' AS created_at")
    cursor = conn.execute(
        f"""SELECT {', '.join(selected)}
              FROM generated_content
             WHERE content_type = 'blog_post'
               AND ({'datetime(created_at) >= datetime(?)' if timestamp_column == 'created_at' else '1 = 1'})
             ORDER BY {('datetime(created_at) DESC, id DESC' if timestamp_column == 'created_at' else 'id DESC')}""",
        (start.isoformat(),) if timestamp_column == "created_at" else (),
    )
    return _cursor_rows_to_dicts(cursor)


def _find_duplicate(db_or_conn: Any, candidate: BlogPullQuoteCandidate) -> dict[str, Any] | None:
    rows = _content_idea_rows(db_or_conn)
    for row in rows:
        if row.get("status") not in {"open", "promoted"}:
            continue
        metadata = _decode_json_object(row.get("source_metadata"))
        if row.get("source") != SOURCE_NAME and metadata.get("source") != SOURCE_NAME:
            continue
        if (
            int(metadata.get("source_content_id") or -1) == candidate.source_content_id
            and _normalize_quote(metadata.get("quote")) == _normalize_quote(candidate.quote)
        ):
            return row
    return None


def _content_idea_rows(db_or_conn: Any) -> list[dict[str, Any]]:
    getter = getattr(db_or_conn, "get_content_ideas", None)
    if callable(getter):
        return [_row_to_dict(row) for row in getter(status=None, limit=1000, include_snoozed=True)]
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "content_ideas" not in schema:
        return []
    cursor = conn.execute(
        """SELECT *
             FROM content_ideas
            WHERE status IN ('open', 'promoted')
            ORDER BY created_at ASC, id ASC"""
    )
    return _cursor_rows_to_dicts(cursor)


def _insert_content_idea(db_or_conn: Any, candidate: BlogPullQuoteCandidate) -> int:
    note = f"Repurpose this blog pull quote as a short social post: {candidate.quote}"
    add_idea = getattr(db_or_conn, "add_content_idea", None) or getattr(
        db_or_conn, "insert_content_idea", None
    )
    if callable(add_idea):
        return int(
            add_idea(
                note=note,
                topic="blog pull quote",
                priority="normal",
                source=SOURCE_NAME,
                source_metadata=candidate.source_metadata,
            )
        )
    conn = _connection(db_or_conn)
    cursor = conn.execute(
        """INSERT INTO content_ideas
           (note, topic, priority, status, source, source_metadata)
           VALUES (?, 'blog pull quote', 'normal', 'open', ?, ?)""",
        (note, SOURCE_NAME, json.dumps(candidate.source_metadata, sort_keys=True)),
    )
    conn.commit()
    return int(cursor.lastrowid)


def _result(
    status: str,
    candidate: BlogPullQuoteCandidate,
    idea_id: int | None,
    reason: str,
) -> BlogPullQuoteIdeaResult:
    return BlogPullQuoteIdeaResult(
        status=status,
        source_content_id=candidate.source_content_id,
        quote=candidate.quote,
        char_count=candidate.char_count,
        idea_id=idea_id,
        reason=reason,
        source_metadata=candidate.source_metadata,
    )


def _validate_bounds(*, days: int, min_chars: int, max_chars: int, limit: int | None) -> None:
    if days <= 0:
        raise ValueError("days must be positive")
    if min_chars <= 0:
        raise ValueError("min_chars must be positive")
    if max_chars < min_chars:
        raise ValueError("max_chars must be greater than or equal to min_chars")
    if limit is not None and limit < 0:
        raise ValueError("limit must be non-negative")


def _connection(db_or_conn: Any) -> Any:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: Any) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = _row_value(row, "name", 0)
        schema[str(table)] = {
            str(_row_value(column, "name", 1))
            for column in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
    return schema


def _row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    if hasattr(row, "keys"):
        return {key: row[key] for key in row.keys()}
    return dict(row)


def _cursor_rows_to_dicts(cursor: Any) -> list[dict[str, Any]]:
    columns = [description[0] for description in cursor.description]
    rows = cursor.fetchall()
    hydrated: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict) or hasattr(row, "keys"):
            hydrated.append(_row_to_dict(row))
        else:
            hydrated.append(dict(zip(columns, row, strict=False)))
    return hydrated


def _row_value(row: Any, key: str, index: int) -> Any:
    if isinstance(row, dict):
        return row[key]
    if hasattr(row, "keys"):
        return row[key]
    return row[index]


def _decode_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        decoded = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _normalize_quote(value: Any) -> str:
    return " ".join(str(value or "").lower().split())


def _dedupe_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        key = _normalize_quote(value)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def _shorten(value: str, width: int) -> str:
    text = " ".join(value.split())
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)].rstrip() + "..."
