"""Seed content ideas from unanswered inbound mentions."""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Any

from synthesis.content_gaps import classify_source_topics


SOURCE_NAME = "mention_idea_seeder"

QUESTION_STARTERS = {
    "what",
    "why",
    "how",
    "when",
    "where",
    "who",
    "which",
    "can",
    "could",
    "would",
    "should",
    "do",
    "does",
    "did",
    "is",
    "are",
}

LOW_VALUE_FLAGS = {
    "spam",
    "low_value",
    "low-value",
    "generic",
    "dismissed",
    "sycophantic",
}


@dataclass(frozen=True)
class MentionIdeaCandidate:
    kind: str
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SeedResult:
    status: str
    kind: str
    topic: str
    idea_id: int | None
    reason: str
    note: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _compact(text: str | None) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _shorten(text: str | None, width: int = 100) -> str:
    value = _compact(text)
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _tokens(text: str | None) -> list[str]:
    return re.findall(r"[a-z0-9']+", str(text or "").lower())


def _looks_question_like(row: dict[str, Any]) -> bool:
    text = str(row.get("inbound_text") or "")
    if str(row.get("intent") or "").strip().lower() == "question":
        return True
    tokens = _tokens(text)
    return "?" in text or bool(tokens and tokens[0] in QUESTION_STARTERS)


def _quality_flags(row: dict[str, Any]) -> set[str]:
    raw = row.get("quality_flags")
    if not raw:
        return set()
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        parsed = [raw]
    if not isinstance(parsed, list):
        parsed = [parsed]
    flags = set()
    for item in parsed:
        value = str(item or "").strip().lower()
        if value:
            flags.add(value)
            flags.update(part for part in re.split(r"[:\s]+", value) if part)
    return flags


def _is_low_value(row: dict[str, Any], *, min_quality_score: float) -> bool:
    intent = str(row.get("intent") or "").strip().lower()
    priority = str(row.get("priority") or "").strip().lower()
    if intent in {"spam", "appreciation", "other"}:
        return True
    if priority == "low":
        return True
    score = row.get("quality_score")
    if score is not None:
        try:
            if float(score) < min_quality_score:
                return True
        except (TypeError, ValueError):
            pass
    return bool(_quality_flags(row) & LOW_VALUE_FLAGS)


def _topic_for_mentions(rows: list[dict[str, Any]]) -> str:
    text = " ".join(
        _compact(f"{row.get('inbound_text') or ''} {row.get('our_post_text') or ''}")
        for row in rows
    )
    topics = classify_source_topics(text)
    return topics[0] if topics else "audience-questions"


def _theme_key(row: dict[str, Any]) -> str:
    text = str(row.get("inbound_text") or "").lower()
    text = re.sub(r"https?://\S+|@\w+", " ", text)
    text = re.sub(r"[^a-z0-9+#.\s-]+", " ", text)
    words = [
        word
        for word in text.split()
        if word not in QUESTION_STARTERS
        and word
        not in {
            "you",
            "your",
            "the",
            "a",
            "an",
            "to",
            "for",
            "with",
            "about",
            "this",
            "that",
            "it",
            "i",
        }
    ]
    return " ".join(words[:8]) or _compact(row.get("inbound_text")).lower()


def _mention_identity(row: dict[str, Any]) -> str:
    platform = str(row.get("platform") or "x")
    inbound_id = row.get("inbound_tweet_id") or row.get("id")
    return f"{platform}:{inbound_id}"


def _metadata_for_rows(
    rows: list[dict[str, Any]],
    *,
    topic: str,
    kind: str,
) -> dict[str, Any]:
    first = rows[0]
    mention_ids = [row["id"] for row in rows]
    inbound_ids = [row.get("inbound_tweet_id") for row in rows]
    metadata: dict[str, Any] = {
        "source": SOURCE_NAME,
        "kind": kind,
        "topic": topic,
        "mention_id": first["id"],
        "mention_ids": mention_ids,
        "source_id": _mention_identity(first),
        "source_ids": [_mention_identity(row) for row in rows],
        "inbound_tweet_id": first.get("inbound_tweet_id"),
        "inbound_tweet_ids": inbound_ids,
        "platform": first.get("platform") or "x",
        "inbound_url": first.get("inbound_url"),
        "inbound_author_handle": first.get("inbound_author_handle"),
        "our_tweet_id": first.get("our_tweet_id"),
        "our_content_id": first.get("our_content_id"),
        "intent": first.get("intent"),
        "detected_at": first.get("detected_at"),
        "mention_count": len(rows),
    }
    raw = json.dumps(
        {"topic": topic, "source_ids": metadata["source_ids"]},
        sort_keys=True,
    )
    metadata["theme_fingerprint"] = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return metadata


def _candidate_from_rows(rows: list[dict[str, Any]]) -> MentionIdeaCandidate:
    topic = _topic_for_mentions(rows)
    kind = "theme" if len(rows) > 1 else "mention"
    metadata = _metadata_for_rows(rows, topic=topic, kind=kind)
    if len(rows) > 1:
        note = (
            f"Create a standalone post answering recurring audience questions about {topic}. "
            f"{len(rows)} unanswered mentions ask about this; start from "
            f"{_shorten(rows[0].get('inbound_text'), 120)}"
        )
        priority = "high"
    else:
        row = rows[0]
        note = (
            f"Create a standalone post answering @{row.get('inbound_author_handle') or 'someone'}'s "
            f"question: {_shorten(row.get('inbound_text'), 140)}"
        )
        priority = "normal" if str(row.get("priority") or "").lower() != "high" else "high"
    return MentionIdeaCandidate(
        kind=kind,
        topic=topic,
        note=note,
        priority=priority,
        source_metadata=metadata,
    )


def build_candidates(
    mentions: list[dict[str, Any]],
    *,
    min_quality_score: float = 5.0,
    recurring_min_count: int = 2,
) -> list[MentionIdeaCandidate]:
    """Build deterministic idea candidates from raw reply_queue rows."""
    eligible = []
    for row in mentions:
        if str(row.get("status") or "").lower() in {"dismissed", "posted"}:
            continue
        if row.get("posted_at") or row.get("posted_tweet_id") or row.get("posted_platform_id"):
            continue
        if not _looks_question_like(row):
            continue
        if _is_low_value(row, min_quality_score=min_quality_score):
            continue
        eligible.append(row)

    by_theme: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in eligible:
        topic = _topic_for_mentions([row])
        by_theme[f"{topic}:{_theme_key(row)}"].append(row)

    candidates: list[MentionIdeaCandidate] = []
    consumed_ids: set[int] = set()
    for rows in by_theme.values():
        if len(rows) >= recurring_min_count:
            candidates.append(_candidate_from_rows(rows))
            consumed_ids.update(row["id"] for row in rows)

    for row in eligible:
        if row["id"] not in consumed_ids:
            candidates.append(_candidate_from_rows([row]))

    return candidates


class MentionIdeaSeeder:
    """Create content idea rows from unanswered audience questions."""

    def __init__(
        self,
        db,
        *,
        min_quality_score: float = 5.0,
        recurring_min_count: int = 2,
    ) -> None:
        self.db = db
        self.min_quality_score = min_quality_score
        self.recurring_min_count = recurring_min_count

    def seed(
        self,
        *,
        dry_run: bool = False,
        limit: int | None = None,
    ) -> list[SeedResult]:
        if limit is not None and limit <= 0:
            return []

        mentions = self.db.get_unanswered_inbound_mentions(limit=limit or 100)
        candidates = build_candidates(
            mentions,
            min_quality_score=self.min_quality_score,
            recurring_min_count=self.recurring_min_count,
        )
        if limit is not None:
            candidates = candidates[:limit]

        results: list[SeedResult] = []
        for candidate in candidates:
            duplicate = self._find_duplicate(candidate)
            if duplicate is not None:
                results.append(
                    SeedResult(
                        status="skipped",
                        kind=candidate.kind,
                        topic=candidate.topic,
                        idea_id=duplicate.get("id"),
                        reason=duplicate["reason"],
                        note=candidate.note,
                        source_metadata=candidate.source_metadata,
                    )
                )
                continue

            if dry_run:
                results.append(
                    SeedResult(
                        status="candidate",
                        kind=candidate.kind,
                        topic=candidate.topic,
                        idea_id=None,
                        reason="dry run",
                        note=candidate.note,
                        source_metadata=candidate.source_metadata,
                    )
                )
                continue

            idea_id = self.db.add_content_idea(
                note=candidate.note,
                topic=candidate.topic,
                priority=candidate.priority,
                source=SOURCE_NAME,
                source_metadata=candidate.source_metadata,
            )
            results.append(
                SeedResult(
                    status="created",
                    kind=candidate.kind,
                    topic=candidate.topic,
                    idea_id=idea_id,
                    reason="created",
                    note=candidate.note,
                    source_metadata=candidate.source_metadata,
                )
            )
        return results

    def _find_duplicate(self, candidate: MentionIdeaCandidate) -> dict[str, Any] | None:
        existing = self.db.find_active_content_idea_for_source_metadata(
            note=candidate.note,
            topic=candidate.topic,
            source=SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        if existing:
            return {
                "id": existing["id"],
                "reason": f"{existing['status']} content idea duplicate",
            }

        planned = self.db.find_similar_planned_topic(topic=candidate.topic)
        if planned:
            return {
                "id": planned["id"],
                "reason": f"{planned['status']} planned topic duplicate",
            }
        return None
