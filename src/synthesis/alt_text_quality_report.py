"""Quality report for generated visual post alt text."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import sqlite3
from typing import Any, Mapping


DEFAULT_DAYS = 30
DEFAULT_MIN_CHARS = 20
PASS = "pass"
MISSING = "missing"
TOO_SHORT = "too_short"
GENERIC = "generic"
REDUNDANT = "redundant"
STATUSES = (PASS, MISSING, TOO_SHORT, GENERIC, REDUNDANT)

_VISUAL_CONTENT_TYPES = {"x_visual", "visual", "image", "social_preview_card"}
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_GENERIC_PHRASES = {
    "image",
    "photo",
    "picture",
    "graphic",
    "illustration",
    "visual",
    "screenshot",
    "generated image",
    "an image",
    "a photo",
    "a picture",
    "a graphic",
    "an illustration",
    "a visual",
    "a screenshot",
    "alt text",
}
_GENERIC_WORDS = {
    "image",
    "photo",
    "picture",
    "graphic",
    "illustration",
    "visual",
    "screenshot",
}
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "into",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "this",
    "to",
    "with",
}


@dataclass(frozen=True)
class AltTextQualityRow:
    """One generated visual content row and its alt-text quality status."""

    content_id: int
    content_type: str | None
    created_at: str | None
    image_path: str | None
    image_alt_text: str | None
    status: str
    quality_flags: tuple[str, ...]
    remediation: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "content_id": self.content_id,
            "content_type": self.content_type,
            "created_at": self.created_at,
            "image_alt_text": self.image_alt_text,
            "image_path": self.image_path,
            "quality_flags": list(self.quality_flags),
            "remediation": self.remediation,
            "status": self.status,
        }


def build_alt_text_quality_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    status: str | None = None,
    now: datetime | None = None,
) -> list[AltTextQualityRow]:
    """Return alt-text quality rows for recent generated visual content."""
    if days <= 0:
        raise ValueError("days must be positive")
    if status is not None and status not in STATUSES:
        raise ValueError(f"status must be one of: {', '.join(STATUSES)}")

    conn = _connection(db_or_conn)
    columns = _table_columns(conn, "generated_content")
    if not columns:
        return []

    rows = _fetch_visual_rows(
        conn,
        columns,
        days=days,
        now=_as_utc(now or datetime.now(timezone.utc)),
    )
    report = [score_alt_text_row(row) for row in rows]
    if status is not None:
        report = [row for row in report if row.status == status]
    return report


def score_alt_text_row(row: Mapping[str, Any]) -> AltTextQualityRow:
    """Score one generated_content-like row for alt-text quality."""
    alt_text = _clean_text(row.get("image_alt_text"))
    content = _clean_text(row.get("content"))
    image_path = _clean_text(row.get("image_path")) or None
    flags: list[str] = []

    if not alt_text:
        flags.append("missing_alt_text")
        status = MISSING
    else:
        if len(alt_text) < DEFAULT_MIN_CHARS or len(_tokens(alt_text)) < 4:
            flags.append("too_short")
        if _is_generic(alt_text):
            flags.append("generic_wording")
        if _file_name_leaks(alt_text, image_path):
            flags.append("filename_leakage")
        if _is_redundant(alt_text, content):
            flags.append("body_redundancy")

        if "too_short" in flags:
            status = TOO_SHORT
        elif "generic_wording" in flags or "filename_leakage" in flags:
            status = GENERIC
        elif "body_redundancy" in flags:
            status = REDUNDANT
        else:
            status = PASS

    return AltTextQualityRow(
        content_id=int(row.get("content_id") or row.get("id") or 0),
        content_type=row.get("content_type"),
        created_at=row.get("created_at"),
        image_path=image_path,
        image_alt_text=alt_text or None,
        status=status,
        quality_flags=tuple(flags),
        remediation=_remediation(status, flags),
    )


def format_alt_text_quality_json(rows: list[AltTextQualityRow]) -> str:
    """Format report rows as deterministic JSON."""
    return json.dumps([row.to_dict() for row in rows], indent=2, sort_keys=True)


def format_alt_text_quality_text(rows: list[AltTextQualityRow]) -> str:
    """Format report rows for terminal review."""
    counts = {status: sum(1 for row in rows if row.status == status) for status in STATUSES}
    lines = [
        "Alt Text Quality Report",
        " ".join(f"{status}={counts[status]}" for status in STATUSES),
        f"{'Status':10s}  {'ID':>5s}  {'Type':14s}  {'Created':19s}  Flags",
        f"{'-' * 10:10s}  {'-' * 5:>5s}  {'-' * 14:14s}  {'-' * 19:19s}  {'-' * 32}",
    ]
    if not rows:
        lines.append("none             -  -               -                    no matching visual posts")
        return "\n".join(lines)
    for row in rows:
        flags = ", ".join(row.quality_flags) or "ok"
        lines.append(
            f"{row.status:10s}  {row.content_id:5d}  "
            f"{str(row.content_type or '-')[:14]:14s}  "
            f"{str(row.created_at or '-')[:19]:19s}  {flags}"
        )
        if row.status != PASS:
            lines.append(f"{'':10s}  {'':5s}  {'':14s}  {'':19s}  fix: {row.remediation}")
    return "\n".join(lines)


def _fetch_visual_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    days: int,
    now: datetime,
) -> list[dict[str, Any]]:
    selected = [
        _column_expr(columns, "id", "NULL"),
        _column_expr(columns, "content_type", "NULL"),
        _column_expr(columns, "created_at", "NULL"),
        _column_expr(columns, "content", "NULL"),
        _column_expr(columns, "image_path", "NULL"),
        _column_expr(columns, "image_alt_text", "NULL"),
    ]
    filters = [_visual_filter(columns)]
    params: list[Any] = []
    if "created_at" in columns:
        cutoff = now - timedelta(days=days)
        filters.append("(created_at IS NULL OR datetime(created_at) >= datetime(?))")
        params.append(cutoff.isoformat())

    order = (
        "datetime(created_at) DESC, id DESC"
        if {"created_at", "id"}.issubset(columns)
        else "rowid DESC"
    )
    query = (
        f"SELECT {', '.join(selected)} FROM generated_content "
        f"WHERE {' AND '.join(filters)} ORDER BY {order}"
    )
    return [_row_dict(row) for row in conn.execute(query, tuple(params)).fetchall()]


def _visual_filter(columns: set[str]) -> str:
    filters: list[str] = []
    if "image_path" in columns:
        filters.append("(image_path IS NOT NULL AND TRIM(image_path) != '')")
    if "content_type" in columns:
        quoted = ", ".join(f"'{content_type}'" for content_type in sorted(_VISUAL_CONTENT_TYPES))
        filters.append(f"content_type IN ({quoted})")
    return "(" + " OR ".join(filters) + ")" if filters else "0 = 1"


def _column_expr(columns: set[str], column: str, fallback: str) -> str:
    if column in columns:
        return column
    return f"{fallback} AS {column}"


def _is_generic(text: str) -> bool:
    normalized = _normalize(text)
    if normalized in _GENERIC_PHRASES:
        return True
    tokens = _tokens(text)
    if not tokens:
        return True
    meaningful = [token for token in tokens if token not in _GENERIC_WORDS]
    return len(tokens) <= 4 and len(meaningful) <= 1 and any(
        token in _GENERIC_WORDS for token in tokens
    )


def _file_name_leaks(alt_text: str, image_path: str | None) -> bool:
    if not image_path:
        return False
    normalized_alt = _normalize(alt_text)
    path = Path(image_path)
    basename = path.name.lower()
    stem = path.stem.lower()
    if basename and basename in normalized_alt:
        return True
    if any(suffix in normalized_alt for suffix in (".png", ".jpg", ".jpeg", ".webp")):
        return True
    filename_like_stem = bool(re.search(r"[_-]|\d", stem))
    normalized_stem = _normalize(stem)
    return filename_like_stem and len(stem) > 5 and (
        stem in normalized_alt or normalized_stem in normalized_alt
    )


def _is_redundant(alt_text: str, content: str) -> bool:
    if not alt_text or not content:
        return False
    normalized_alt = _normalize(alt_text)
    normalized_content = _normalize(content)
    if normalized_alt == normalized_content:
        return True
    if len(normalized_alt) >= 24 and normalized_alt in normalized_content:
        return True

    alt_terms = _meaningful_tokens(alt_text)
    content_terms = _meaningful_tokens(content)
    if len(alt_terms) < 4 or not content_terms:
        return False
    overlap = len(set(alt_terms).intersection(content_terms)) / len(set(alt_terms))
    return overlap >= 0.9


def _remediation(status: str, flags: list[str]) -> str:
    if status == PASS:
        return "No remediation needed."
    if status == MISSING:
        return "Add concise alt text describing the visual's key information and context."
    if status == TOO_SHORT:
        return "Expand the alt text into a short, specific sentence about the visual."
    if status == REDUNDANT:
        return "Rewrite the alt text to describe the image itself instead of repeating the post body."
    if "filename_leakage" in flags:
        return "Remove file names or extensions and describe what the visual shows."
    return "Replace generic wording with specific objects, labels, charts, or scene details."


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _normalize(value: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", " ", value.lower()).strip()


def _tokens(value: str) -> list[str]:
    return _TOKEN_RE.findall(value.lower())


def _meaningful_tokens(value: str) -> set[str]:
    return {token for token in _tokens(value) if len(token) > 2 and token not in _STOPWORDS}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _row_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return {key: row[key] for key in row.keys()}


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
