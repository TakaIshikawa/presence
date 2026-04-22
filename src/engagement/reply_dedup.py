"""Deduplicate reply drafts before they enter review queues."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from typing import Iterable


DEFAULT_LOOKBACK_HOURS = 72
DEFAULT_SIMILARITY_THRESHOLD = 0.90


@dataclass(frozen=True)
class ReplyDedupMatch:
    """A recent reply draft that is too similar to a new draft."""

    source_table: str
    id: int
    similarity: float
    reason: str
    draft_text: str


_URL_RE = re.compile(r"https?://\S+")
_HANDLE_RE = re.compile(r"(?<!\w)@\w+")
_NON_WORD_RE = re.compile(r"[^\w\s']")
_SPACE_RE = re.compile(r"\s+")


def normalize_reply_text(text: str | None) -> str:
    """Normalize reply text for near-duplicate comparison."""
    if not text:
        return ""
    normalized = _URL_RE.sub(" ", text.casefold())
    normalized = _HANDLE_RE.sub(" ", normalized)
    normalized = _NON_WORD_RE.sub(" ", normalized)
    return _SPACE_RE.sub(" ", normalized).strip()


def reply_similarity(left: str | None, right: str | None) -> float:
    """Return a stable near-duplicate score between two reply drafts."""
    left_norm = normalize_reply_text(left)
    right_norm = normalize_reply_text(right)
    if not left_norm or not right_norm:
        return 0.0
    if left_norm == right_norm:
        return 1.0
    return SequenceMatcher(None, left_norm, right_norm).ratio()


def find_duplicate_reply_draft(
    *,
    db,
    draft_text: str,
    author_handle: str,
    platform_target_id: str | None,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
    now: datetime | None = None,
) -> ReplyDedupMatch | None:
    """Find a recent near-identical draft for the same author or target."""
    if lookback_hours <= 0:
        return None
    if not normalize_reply_text(draft_text):
        return None

    candidates = db.get_recent_reply_dedup_candidates(
        author_handle=author_handle,
        platform_target_id=platform_target_id,
        lookback_hours=lookback_hours,
        now=now,
    )
    best_match = None
    best_similarity = 0.0
    for row in _iter_rows(candidates):
        similarity = reply_similarity(draft_text, row.get("draft_text"))
        if similarity < similarity_threshold or similarity < best_similarity:
            continue
        best_similarity = similarity
        best_match = ReplyDedupMatch(
            source_table=str(row.get("source_table") or ""),
            id=int(row.get("id") or 0),
            similarity=similarity,
            reason=_match_reason(row, author_handle, platform_target_id),
            draft_text=str(row.get("draft_text") or ""),
        )
    return best_match


def _iter_rows(rows) -> Iterable[dict]:
    if rows is None:
        return ()
    return (dict(row) for row in rows)


def _normalize_handle(handle: str | None) -> str:
    return (handle or "").lstrip("@").casefold()


def _match_reason(
    row: dict,
    author_handle: str,
    platform_target_id: str | None,
) -> str:
    if _normalize_handle(row.get("author_handle")) == _normalize_handle(author_handle):
        return "same_author"
    if platform_target_id and row.get("platform_target_id") == platform_target_id:
        return "same_platform_target"
    return "same_author_or_platform_target"
