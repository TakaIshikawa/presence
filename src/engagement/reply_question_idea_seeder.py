"""Seed content ideas from repeated unanswered reply questions."""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from synthesis.content_gaps import classify_source_topics


SOURCE_NAME = "reply_question"
DEFAULT_DAYS = 30
DEFAULT_LIMIT = 20
DEFAULT_MIN_CLUSTER_SIZE = 2

LOW_VALUE_FLAGS = {
    "spam",
    "low_value",
    "low-value",
    "generic",
    "bot",
    "promo",
    "promotion",
    "solicitation",
}

STOPWORDS = {
    "a",
    "about",
    "am",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "can",
    "could",
    "did",
    "do",
    "does",
    "for",
    "from",
    "how",
    "i",
    "in",
    "is",
    "it",
    "me",
    "of",
    "on",
    "or",
    "our",
    "should",
    "that",
    "the",
    "this",
    "to",
    "what",
    "when",
    "where",
    "which",
    "who",
    "why",
    "with",
    "would",
    "you",
    "your",
}


@dataclass(frozen=True)
class ReplyQuestionCluster:
    topic: str
    note: str
    priority: str
    reply_ids: list[int]
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReplyQuestionSeedResult:
    status: str
    topic: str
    idea_id: int | None
    reason: str
    note: str
    reply_ids: list[int]
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def seed_reply_question_ideas(
    db,
    *,
    days: int = DEFAULT_DAYS,
    min_cluster_size: int = DEFAULT_MIN_CLUSTER_SIZE,
    limit: int | None = DEFAULT_LIMIT,
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[ReplyQuestionSeedResult]:
    """Create or preview content ideas for repeated unanswered audience questions."""
    clusters = build_reply_question_clusters(
        db,
        days=days,
        min_cluster_size=min_cluster_size,
        limit=limit,
        now=now,
    )
    add_idea = getattr(db, "add_content_idea", None) or getattr(db, "insert_content_idea", None)
    if not callable(add_idea):
        return []

    results: list[ReplyQuestionSeedResult] = []
    for cluster in clusters:
        duplicate = _find_duplicate_for_reply_ids(db, cluster.reply_ids)
        if duplicate is not None:
            results.append(_result(cluster, "skipped", int(duplicate["id"]), f"{duplicate['status']} duplicate"))
            continue
        if dry_run:
            results.append(_result(cluster, "candidate", None, "dry run"))
            continue
        idea_id = add_idea(
            note=cluster.note,
            topic=cluster.topic,
            priority=cluster.priority,
            source=SOURCE_NAME,
            source_metadata=cluster.source_metadata,
        )
        results.append(_result(cluster, "created", int(idea_id), "created"))
    return results


def build_reply_question_clusters(
    db,
    *,
    days: int = DEFAULT_DAYS,
    min_cluster_size: int = DEFAULT_MIN_CLUSTER_SIZE,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> list[ReplyQuestionCluster]:
    if days <= 0:
        raise ValueError("days must be positive")
    if min_cluster_size <= 0:
        raise ValueError("min_cluster_size must be positive")
    if limit is not None and limit < 0:
        raise ValueError("limit must be non-negative")

    rows = _fetch_question_rows(db, days=days, now=now)
    clusters = _cluster_rows(rows, min_cluster_size=min_cluster_size)
    candidates = [_cluster_to_candidate(cluster) for cluster in clusters]
    candidates.sort(
        key=lambda candidate: (
            -len(candidate.reply_ids),
            candidate.topic,
            candidate.reply_ids,
        )
    )
    return candidates[:limit] if limit is not None else candidates


def format_reply_question_ideas_json(results: list[ReplyQuestionSeedResult]) -> str:
    return json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True)


def format_reply_question_ideas_text(results: list[ReplyQuestionSeedResult]) -> str:
    created = sum(1 for result in results if result.status == "created")
    candidates = sum(1 for result in results if result.status == "candidate")
    skipped = sum(1 for result in results if result.status == "skipped")
    lines = [f"created={created} candidate={candidates} skipped={skipped}"]
    lines.append(f"{'Status':9s}  {'ID':>4s}  {'Replies':>7s}  {'Topic':18s}  Reason / idea")
    lines.append(f"{'-' * 9:9s}  {'-' * 4:>4s}  {'-' * 7:>7s}  {'-' * 18:18s}  {'-' * 40}")
    if not results:
        lines.append("none       -          0  ------------------  no qualifying reply question clusters")
        return "\n".join(lines)
    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        lines.append(
            f"{result.status:9s}  {idea_id:>4s}  {len(result.reply_ids):7d}  "
            f"{_shorten(result.topic, 18):18s}  {result.reason}: {_shorten(result.note, 86)}"
        )
    return "\n".join(lines)


def _fetch_question_rows(db, *, days: int, now: datetime | None) -> list[dict[str, Any]]:
    end = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    start = end - timedelta(days=days)
    cursor = db.conn.execute(
        """SELECT *
           FROM reply_queue
           WHERE intent = 'question'
             AND status IN ('pending', 'dismissed')
             AND posted_at IS NULL
             AND posted_tweet_id IS NULL
             AND posted_platform_id IS NULL
             AND datetime(detected_at) >= datetime(?)
           ORDER BY datetime(detected_at) ASC, id ASC""",
        (start.isoformat(),),
    )
    rows = [dict(row) for row in cursor.fetchall()]
    return [row for row in rows if not _is_spam_like(row) and _tokens(row.get("inbound_text"))]


def _cluster_rows(
    rows: list[dict[str, Any]],
    *,
    min_cluster_size: int,
) -> list[list[dict[str, Any]]]:
    token_sets = {int(row["id"]): set(_tokens(row.get("inbound_text"))) for row in rows}
    clusters: list[list[dict[str, Any]]] = []
    used: set[int] = set()
    for row in rows:
        row_id = int(row["id"])
        if row_id in used:
            continue
        cluster = [row]
        used.add(row_id)
        cluster_terms = set(token_sets[row_id])
        for other in rows:
            other_id = int(other["id"])
            if other_id in used:
                continue
            if _overlap_score(cluster_terms, token_sets[other_id]) >= 0.5:
                cluster.append(other)
                used.add(other_id)
                cluster_terms.update(token_sets[other_id])
        if len(cluster) >= min_cluster_size:
            clusters.append(cluster)
    return clusters


def _cluster_to_candidate(rows: list[dict[str, Any]]) -> ReplyQuestionCluster:
    reply_ids = [int(row["id"]) for row in rows]
    handles = sorted({str(row.get("inbound_author_handle") or "").lstrip("@") for row in rows if row.get("inbound_author_handle")})
    platforms = sorted({str(row.get("platform") or "x") for row in rows})
    terms = _cluster_terms(rows)
    topic = _topic_for_rows(rows)
    first_question = _shorten(rows[0].get("inbound_text"), 150)
    note = (
        f"Create a content idea answering a repeated audience question about {topic}: "
        f"{first_question} ({len(rows)} related replies)."
    )
    metadata = {
        "source": SOURCE_NAME,
        "reply_ids": reply_ids,
        "handles": handles,
        "platforms": platforms,
        "cluster_terms": terms,
        "question_count": len(rows),
        "sample_question": rows[0].get("inbound_text"),
        "detected_at_range": {
            "first": rows[0].get("detected_at"),
            "last": rows[-1].get("detected_at"),
        },
    }
    return ReplyQuestionCluster(
        topic=topic,
        note=note,
        priority="high" if len(rows) >= 3 else "normal",
        reply_ids=reply_ids,
        source_metadata=metadata,
    )


def _find_duplicate_for_reply_ids(db, reply_ids: list[int]) -> dict[str, Any] | None:
    wanted = {int(reply_id) for reply_id in reply_ids}
    getter = getattr(db, "get_content_ideas", None)
    if callable(getter):
        rows = getter(status=None, limit=1000, include_snoozed=True)
    else:
        rows = [
            dict(row)
            for row in db.conn.execute(
                """SELECT *
                   FROM content_ideas
                   WHERE status IN ('open', 'promoted')
                   ORDER BY created_at ASC, id ASC"""
            ).fetchall()
        ]
    for row in rows:
        if row.get("status") not in {"open", "promoted"}:
            continue
        metadata = _decode_metadata(row.get("source_metadata"))
        existing_ids = metadata.get("reply_ids")
        if not isinstance(existing_ids, list):
            continue
        try:
            existing = {int(reply_id) for reply_id in existing_ids}
        except (TypeError, ValueError):
            continue
        if wanted & existing:
            return row
    return None


def _result(
    cluster: ReplyQuestionCluster,
    status: str,
    idea_id: int | None,
    reason: str,
) -> ReplyQuestionSeedResult:
    return ReplyQuestionSeedResult(
        status=status,
        topic=cluster.topic,
        idea_id=idea_id,
        reason=reason,
        note=cluster.note,
        reply_ids=cluster.reply_ids,
        source_metadata=cluster.source_metadata,
    )


def _is_spam_like(row: dict[str, Any]) -> bool:
    if str(row.get("priority") or "").strip().lower() == "low":
        return True
    score = row.get("quality_score")
    if score is not None:
        try:
            if float(score) <= 2:
                return True
        except (TypeError, ValueError):
            pass
    return bool(_quality_flags(row) & LOW_VALUE_FLAGS)


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
    flags: set[str] = set()
    for item in parsed:
        value = str(item or "").strip().lower()
        if value:
            flags.add(value)
            flags.update(part for part in re.split(r"[:\s_-]+", value) if part)
    return flags


def _tokens(text: str | None) -> list[str]:
    tokens = re.findall(r"[a-z0-9+#']+", str(text or "").lower())
    return [_stem(token) for token in tokens if token not in STOPWORDS and len(token) > 1]


def _stem(token: str) -> str:
    for suffix in ("ing", "ers", "ies", "ed", "es", "s"):
        if len(token) > len(suffix) + 3 and token.endswith(suffix):
            if suffix == "ies":
                return token[: -len(suffix)] + "y"
            return token[: -len(suffix)]
    return token


def _overlap_score(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / min(len(left), len(right))


def _cluster_terms(rows: list[dict[str, Any]]) -> list[str]:
    counts = Counter(token for row in rows for token in _tokens(row.get("inbound_text")))
    return [term for term, _count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:8]]


def _topic_for_rows(rows: list[dict[str, Any]]) -> str:
    text = " ".join(str(row.get("inbound_text") or "") for row in rows)
    topics = classify_source_topics(text)
    return topics[0] if topics else "audience-questions"


def _decode_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _shorten(text: str | None, width: int = 100) -> str:
    value = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."
