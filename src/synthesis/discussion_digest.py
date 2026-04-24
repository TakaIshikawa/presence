"""Rank GitHub Discussions activity into seedable content ideas."""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


SOURCE_NAME = "github_discussion_digest"
BODY_EXCERPT_CHARS = 420
DEFAULT_MIN_SCORE = 35.0

ACTION_WORDS = {
    "how",
    "why",
    "what",
    "when",
    "where",
    "which",
    "should",
    "could",
    "can",
    "best",
    "recommend",
    "help",
    "error",
    "fails",
    "broken",
    "missing",
    "confusing",
}

STOPWORDS = {
    "about",
    "after",
    "again",
    "also",
    "from",
    "have",
    "into",
    "just",
    "like",
    "more",
    "need",
    "that",
    "their",
    "there",
    "this",
    "with",
    "would",
    "your",
}


@dataclass(frozen=True)
class DiscussionIdeaCandidate:
    repo_name: str
    number: int
    activity_id: str
    category: str
    title: str
    url: str
    state: str
    labels: list[str]
    topic: str
    angle: str
    note: str
    score: float
    score_reasons: list[str]
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DiscussionDigestResult:
    status: str
    repo_name: str
    number: int
    category: str
    topic: str
    score: float
    idea_id: int | None
    reason: str
    note: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _shorten(text: str | None, width: int = 70) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _body_excerpt(text: str | None, width: int = BODY_EXCERPT_CHARS) -> str:
    return _shorten(text, width) or "No discussion body provided."


def _category(metadata: dict[str, Any]) -> str:
    category = metadata.get("category") if isinstance(metadata, dict) else None
    if isinstance(category, dict):
        return str(category.get("name") or category.get("slug") or "uncategorized")
    if category:
        return str(category)
    return "uncategorized"


def _comments_count(metadata: dict[str, Any]) -> int:
    try:
        return int(metadata.get("comments_count") or 0)
    except (TypeError, ValueError):
        return 0


def _is_answered(row: dict) -> bool:
    metadata = row.get("metadata") or {}
    return str(row.get("state") or "").lower() == "answered" or bool(metadata.get("answer"))


def _tokens(*parts: str | None) -> list[str]:
    text = " ".join(part or "" for part in parts).lower()
    return [
        token
        for token in re.findall(r"[a-z][a-z0-9_-]{2,}", text)
        if token not in STOPWORDS
    ]


def _theme_terms(group_rows: list[dict]) -> list[str]:
    counter: Counter[str] = Counter()
    for row in group_rows:
        metadata = row.get("metadata") or {}
        labels = " ".join(str(label) for label in (row.get("labels") or []))
        counter.update(_tokens(row.get("title"), row.get("body"), labels, _category(metadata)))
    return [term for term, _count in counter.most_common(4)]


def _score(row: dict, *, group_size: int, group_terms: list[str], now: datetime) -> tuple[float, list[str]]:
    metadata = row.get("metadata") or {}
    title = str(row.get("title") or "")
    body = str(row.get("body") or "")
    labels = [str(label).lower() for label in (row.get("labels") or [])]
    text = f"{title} {body}".lower()
    comments = _comments_count(metadata)
    score = 20.0
    reasons = ["discussion-base+20"]

    if not _is_answered(row):
        score += 24
        reasons.append("unanswered+24")
    if "?" in title or any(word in _tokens(title) for word in ACTION_WORDS):
        score += 14
        reasons.append("question-title+14")
    if comments >= 5:
        score += 14
        reasons.append("comments>=5+14")
    elif comments >= 2:
        score += 8
        reasons.append("comments>=2+8")
    if any(label in {"question", "help", "docs", "documentation", "bug", "enhancement"} for label in labels):
        score += 10
        reasons.append("signal-label+10")
    if group_size >= 2:
        score += min(12, group_size * 4)
        reasons.append(f"group-size:{group_size}+{min(12, group_size * 4)}")
    if group_terms and any(term in text for term in group_terms[:3]):
        score += 6
        reasons.append("theme-match+6")

    updated_at = _parse_datetime(row.get("updated_at"))
    if updated_at is not None:
        age_days = max(0.0, (now - updated_at).total_seconds() / 86400)
        if age_days <= 2:
            score += 10
            reasons.append("updated<=2d+10")
        elif age_days <= 7:
            score += 6
            reasons.append("updated<=7d+6")
    return round(score, 2), reasons


def discussion_to_candidate(
    row: dict,
    *,
    group_size: int = 1,
    group_terms: list[str] | None = None,
    now: datetime | None = None,
) -> DiscussionIdeaCandidate:
    """Convert one stored GitHub Discussion row into a ranked content idea payload."""
    now = now or datetime.now(timezone.utc)
    metadata = row.get("metadata") or {}
    repo_name = str(row.get("repo_name") or "")
    number = int(row.get("number"))
    category = _category(metadata)
    labels = [str(label) for label in (row.get("labels") or []) if str(label).strip()]
    title = str(row.get("title") or f"Discussion #{number}")
    url = str(row.get("url") or "")
    body_excerpt = _body_excerpt(row.get("body"))
    group_terms = group_terms or []
    score, reasons = _score(row, group_size=group_size, group_terms=group_terms, now=now)
    theme = ", ".join(group_terms[:3]) if group_terms else category
    answered = _is_answered(row)
    status_word = "answered" if answered else "unanswered"
    topic = f"{repo_name} {category}: {title}".strip()
    angle = (
        f"Turn the {status_word} GitHub Discussion #{number} into a practical "
        f"post about {theme}: the user question, the underlying need, and the next step."
    )
    note = (
        f"GitHub Discussion #{number} in {repo_name} ({category}, {status_word}): {title}. "
        f"Labels: {', '.join(labels) if labels else 'none'}. "
        f"Comments: {_comments_count(metadata)}. "
        f"URL: {url or 'none'}. "
        f"Body excerpt: {body_excerpt} "
        f"Suggested angle: {angle}"
    )
    source_metadata = {
        "source": SOURCE_NAME,
        "activity_id": row.get("activity_id"),
        "repo_name": repo_name,
        "number": number,
        "category": category,
        "title": title,
        "url": url,
        "state": row.get("state"),
        "labels": labels,
        "updated_at": row.get("updated_at"),
        "comments_count": _comments_count(metadata),
        "answered": answered,
        "theme_terms": group_terms[:4],
        "score": score,
        "score_reasons": reasons,
        "body_excerpt": body_excerpt,
    }
    return DiscussionIdeaCandidate(
        repo_name=repo_name,
        number=number,
        activity_id=str(row.get("activity_id") or ""),
        category=category,
        title=title,
        url=url,
        state=str(row.get("state") or ""),
        labels=labels,
        topic=topic,
        angle=angle,
        note=note,
        score=score,
        score_reasons=reasons,
        source_metadata=source_metadata,
    )


def build_discussion_candidates(
    rows: list[dict],
    *,
    min_score: float = DEFAULT_MIN_SCORE,
    limit: int | None = None,
    now: datetime | None = None,
) -> list[DiscussionIdeaCandidate]:
    """Group discussions by repo/category and return ranked idea candidates."""
    if limit is not None and limit <= 0:
        return []
    now = now or datetime.now(timezone.utc)
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in rows:
        if row.get("activity_type") != "discussion":
            continue
        metadata = row.get("metadata") or {}
        groups[(str(row.get("repo_name") or ""), _category(metadata))].append(row)

    candidates: list[DiscussionIdeaCandidate] = []
    for group_rows in groups.values():
        terms = _theme_terms(group_rows)
        group_size = len(group_rows)
        for row in group_rows:
            candidate = discussion_to_candidate(
                row,
                group_size=group_size,
                group_terms=terms,
                now=now,
            )
            if candidate.score >= min_score:
                candidates.append(candidate)

    candidates.sort(key=lambda candidate: (-candidate.score, candidate.repo_name, candidate.category, candidate.number))
    return candidates[:limit] if limit is not None else candidates


def seed_discussion_ideas(
    db,
    *,
    days: int = 14,
    repo: str | None = None,
    limit: int | None = 10,
    min_score: float = DEFAULT_MIN_SCORE,
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[DiscussionDigestResult]:
    """Create content ideas from recent GitHub Discussion activity."""
    if days <= 0 or (limit is not None and limit <= 0):
        return []
    now = now or datetime.now(timezone.utc)
    rows = db.get_recent_github_discussions(days=days, repo_name=repo, limit=None, now=now)
    candidates = build_discussion_candidates(rows, min_score=min_score, limit=limit, now=now)

    results: list[DiscussionDigestResult] = []
    for candidate in candidates:
        existing = db.find_active_content_idea_for_source_metadata(
            note=candidate.note,
            topic=candidate.topic,
            source=SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        if existing:
            results.append(
                DiscussionDigestResult(
                    status="skipped",
                    repo_name=candidate.repo_name,
                    number=candidate.number,
                    category=candidate.category,
                    topic=candidate.topic,
                    score=candidate.score,
                    idea_id=existing["id"],
                    reason=f"{existing['status']} duplicate",
                    note=candidate.note,
                )
            )
            continue

        if dry_run:
            results.append(
                DiscussionDigestResult(
                    status="proposed",
                    repo_name=candidate.repo_name,
                    number=candidate.number,
                    category=candidate.category,
                    topic=candidate.topic,
                    score=candidate.score,
                    idea_id=None,
                    reason="dry run",
                    note=candidate.note,
                )
            )
            continue

        idea_id = db.add_content_idea(
            note=candidate.note,
            topic=candidate.topic,
            priority="normal",
            source=SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        results.append(
            DiscussionDigestResult(
                status="created",
                repo_name=candidate.repo_name,
                number=candidate.number,
                category=candidate.category,
                topic=candidate.topic,
                score=candidate.score,
                idea_id=idea_id,
                reason="created",
                note=candidate.note,
            )
        )

    return results
