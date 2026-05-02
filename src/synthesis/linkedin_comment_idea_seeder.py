"""Seed reviewable content ideas from imported LinkedIn comments."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


SOURCE_NAME = "linkedin_comment"
DEFAULT_DAYS = 14
DEFAULT_MIN_REACTIONS = 1


@dataclass(frozen=True)
class LinkedInCommentSignal:
    reply_id: int
    comment_id: str
    author: str
    author_profile_url: str | None
    body: str
    created_at: str
    detected_at: str
    reaction_count: int
    content_id: int | None
    post_id: str | None
    post_url: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class LinkedInCommentIdeaCandidate:
    comment_group_id: str
    content_id: int | None
    post_id: str | None
    post_url: str | None
    comments: tuple[LinkedInCommentSignal, ...]
    topic: str
    note: str
    priority: str
    reason: str
    source_metadata: dict[str, Any]

    @property
    def comment_ids(self) -> list[str]:
        return [comment.comment_id for comment in self.comments]

    @property
    def reaction_count(self) -> int:
        return sum(comment.reaction_count for comment in self.comments)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["comments"] = [comment.to_dict() for comment in self.comments]
        payload["comment_ids"] = self.comment_ids
        payload["reaction_count"] = self.reaction_count
        return payload


@dataclass(frozen=True)
class LinkedInCommentSeedResult:
    status: str
    comment_group_id: str
    comment_ids: list[str]
    authors: list[str]
    content_id: int | None
    post_id: str | None
    post_url: str | None
    reaction_count: int
    topic: str
    idea_id: int | None
    reason: str
    note: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _parse_datetime(value: object | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _decode_metadata(value: object | None) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        decoded = json.loads(str(value or "{}"))
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _clean(value: object | None) -> str:
    return str(value or "").strip()


def _optional_int(value: object | None) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _comment_group_id(*, content_id: int | None, post_id: str | None, post_url: str | None) -> str | None:
    if content_id is not None:
        return f"content:{content_id}"
    if post_id:
        return f"post:{post_id}"
    if post_url:
        return f"url:{post_url}"
    return None


def _linkedin_comment_rows(db: Any) -> list[dict[str, Any]]:
    if not getattr(db, "conn", None):
        return []
    rows = db.conn.execute(
        """SELECT id, inbound_tweet_id, inbound_author_handle, inbound_author_id,
                  inbound_text, inbound_url, our_tweet_id, our_platform_id,
                  our_content_id, platform_metadata, detected_at
             FROM reply_queue
            WHERE lower(COALESCE(platform, '')) = 'linkedin'
            ORDER BY detected_at ASC, id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _signal_from_row(row: dict[str, Any]) -> LinkedInCommentSignal:
    metadata = _decode_metadata(row.get("platform_metadata"))
    comment_id = _clean(metadata.get("comment_id") or row.get("inbound_tweet_id"))
    author = _clean(metadata.get("author") or row.get("inbound_author_handle") or "unknown")
    content_id = row.get("our_content_id")
    content_id = int(content_id) if content_id not in (None, "") else None
    post_id = _clean(metadata.get("post_id") or row.get("our_platform_id") or row.get("our_tweet_id")) or None
    post_url = _clean(metadata.get("post_url") or row.get("inbound_url")) or None
    return LinkedInCommentSignal(
        reply_id=int(row.get("id") or 0),
        comment_id=comment_id,
        author=author,
        author_profile_url=_clean(metadata.get("author_profile_url") or row.get("inbound_author_id")) or None,
        body=_clean(metadata.get("body") or row.get("inbound_text")),
        created_at=_clean(metadata.get("created_at") or row.get("detected_at")),
        detected_at=_clean(row.get("detected_at")),
        reaction_count=_optional_int(metadata.get("like_count") or metadata.get("reaction_count")),
        content_id=content_id,
        post_id=post_id,
        post_url=post_url,
    )


def _shorten(text: str, width: int) -> str:
    value = " ".join(text.split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _candidate_from_group(group_id: str, comments: list[LinkedInCommentSignal]) -> LinkedInCommentIdeaCandidate:
    comments = sorted(comments, key=lambda item: (item.created_at, item.comment_id, item.reply_id))
    first = comments[0]
    authors = sorted({comment.author for comment in comments if comment.author})
    reason = (
        f"{len(comments)} high-signal LinkedIn comment"
        f"{'' if len(comments) == 1 else 's'} with {sum(comment.reaction_count for comment in comments)} reactions"
    )
    excerpt = _shorten(first.body, 140)
    if len(comments) == 1:
        topic = f"LinkedIn comment idea: {excerpt[:80]}"
    else:
        topic = f"LinkedIn comment thread idea: {first.post_id or first.post_url or group_id}"
    note = (
        f"{reason}. Authors: {', '.join(authors) or 'unknown'}. "
        f"Post: {first.post_url or first.post_id or 'unknown'}. "
        f"Representative comment: {excerpt}. "
        "Review the comment context and turn the audience signal into a useful post."
    )
    metadata = {
        "source": SOURCE_NAME,
        "comment_group_id": group_id,
        "comment_id": first.comment_id,
        "comment_ids": [comment.comment_id for comment in comments],
        "reply_queue_ids": [comment.reply_id for comment in comments],
        "authors": authors,
        "post_id": first.post_id,
        "post_url": first.post_url,
        "content_id": first.content_id,
        "reaction_count": sum(comment.reaction_count for comment in comments),
        "reason": reason,
        "comments": [
            {
                "comment_id": comment.comment_id,
                "author": comment.author,
                "author_profile_url": comment.author_profile_url,
                "body": comment.body,
                "created_at": comment.created_at,
                "reaction_count": comment.reaction_count,
            }
            for comment in comments
        ],
    }
    return LinkedInCommentIdeaCandidate(
        comment_group_id=group_id,
        content_id=first.content_id,
        post_id=first.post_id,
        post_url=first.post_url,
        comments=tuple(comments),
        topic=topic,
        note=note,
        priority="high" if sum(comment.reaction_count for comment in comments) >= 10 else "normal",
        reason=reason,
        source_metadata={key: value for key, value in metadata.items() if value not in (None, "", [])},
    )


def build_linkedin_comment_idea_candidates(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_reactions: int = DEFAULT_MIN_REACTIONS,
    limit: int | None = None,
    now: datetime | None = None,
) -> list[LinkedInCommentIdeaCandidate]:
    """Return grouped high-signal LinkedIn comments from reply_queue."""
    if days <= 0 or min_reactions < 0 or (limit is not None and limit <= 0):
        return []
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)
    groups: dict[str, list[LinkedInCommentSignal]] = {}
    for row in _linkedin_comment_rows(db):
        signal = _signal_from_row(row)
        observed_at = _parse_datetime(signal.created_at) or _parse_datetime(signal.detected_at)
        if observed_at is None or observed_at < cutoff:
            continue
        if not signal.comment_id or not signal.body:
            continue
        if signal.reaction_count < min_reactions:
            continue
        group_id = _comment_group_id(
            content_id=signal.content_id,
            post_id=signal.post_id,
            post_url=signal.post_url,
        )
        if not group_id:
            continue
        groups.setdefault(group_id, []).append(signal)

    candidates = [_candidate_from_group(group_id, comments) for group_id, comments in groups.items()]
    candidates.sort(
        key=lambda candidate: (
            -candidate.reaction_count,
            candidate.comment_group_id,
            candidate.comment_ids[0] if candidate.comment_ids else "",
        )
    )
    return candidates[:limit] if limit is not None else candidates


def _existing_open_idea(db: Any, candidate: LinkedInCommentIdeaCandidate) -> dict[str, Any] | None:
    if not getattr(db, "conn", None):
        return None
    rows = db.conn.execute(
        """SELECT * FROM content_ideas
            WHERE status = 'open'
              AND source = ?
              AND source_metadata IS NOT NULL
            ORDER BY created_at ASC, id ASC""",
        (SOURCE_NAME,),
    ).fetchall()
    wanted_comment_ids = set(candidate.comment_ids)
    for row in rows:
        idea = dict(row)
        metadata = _decode_metadata(idea.get("source_metadata"))
        if metadata.get("comment_group_id") == candidate.comment_group_id:
            return idea
        existing_ids = {str(metadata.get("comment_id"))} if metadata.get("comment_id") else set()
        raw_ids = metadata.get("comment_ids")
        if isinstance(raw_ids, list):
            existing_ids.update(str(item) for item in raw_ids)
        if wanted_comment_ids & existing_ids:
            return idea
    return None


def _result(
    status: str,
    candidate: LinkedInCommentIdeaCandidate,
    *,
    idea_id: int | None,
    reason: str,
) -> LinkedInCommentSeedResult:
    return LinkedInCommentSeedResult(
        status=status,
        comment_group_id=candidate.comment_group_id,
        comment_ids=candidate.comment_ids,
        authors=sorted({comment.author for comment in candidate.comments if comment.author}),
        content_id=candidate.content_id,
        post_id=candidate.post_id,
        post_url=candidate.post_url,
        reaction_count=candidate.reaction_count,
        topic=candidate.topic,
        idea_id=idea_id,
        reason=reason,
        note=candidate.note,
        source_metadata=candidate.source_metadata,
    )


def seed_linkedin_comment_ideas(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_reactions: int = DEFAULT_MIN_REACTIONS,
    limit: int | None = 25,
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[LinkedInCommentSeedResult]:
    """Create or preview content ideas from high-signal LinkedIn comments."""
    candidates = build_linkedin_comment_idea_candidates(
        db,
        days=days,
        min_reactions=min_reactions,
        limit=limit,
        now=now,
    )
    add_idea = getattr(db, "add_content_idea", None) or getattr(db, "insert_content_idea", None)
    if not callable(add_idea):
        return []

    results: list[LinkedInCommentSeedResult] = []
    for candidate in candidates:
        existing = _existing_open_idea(db, candidate)
        if existing:
            results.append(
                _result(
                    "skipped",
                    candidate,
                    idea_id=int(existing["id"]),
                    reason="open duplicate",
                )
            )
            continue
        if dry_run:
            results.append(_result("proposed", candidate, idea_id=None, reason="dry run"))
            continue
        idea_id = add_idea(
            note=candidate.note,
            topic=candidate.topic,
            priority=candidate.priority,
            source=SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        results.append(_result("created", candidate, idea_id=idea_id, reason="created"))
    return results


def summarize_linkedin_comment_seed_results(
    results: list[LinkedInCommentSeedResult],
) -> dict[str, Any]:
    skip_reasons: dict[str, int] = {}
    for result in results:
        if result.status == "skipped":
            skip_reasons[result.reason] = skip_reasons.get(result.reason, 0) + 1
    return {
        "created": sum(1 for result in results if result.status == "created"),
        "proposed": sum(1 for result in results if result.status == "proposed"),
        "skipped": sum(1 for result in results if result.status == "skipped"),
        "skip_reasons": skip_reasons,
    }


def format_linkedin_comment_seed_json(results: list[LinkedInCommentSeedResult]) -> str:
    return json.dumps(
        {
            "summary": summarize_linkedin_comment_seed_results(results),
            "results": [result.to_dict() for result in results],
        },
        indent=2,
        sort_keys=True,
    )


def format_linkedin_comment_seed_text(results: list[LinkedInCommentSeedResult]) -> str:
    summary = summarize_linkedin_comment_seed_results(results)
    skip_reasons = ", ".join(
        f"{reason}={count}" for reason, count in sorted(summary["skip_reasons"].items())
    )
    lines = [
        f"created={summary['created']} proposed={summary['proposed']} skipped={summary['skipped']}",
        f"skip_reasons {skip_reasons or 'none'}",
        f"{'Status':8s}  {'ID':>4s}  {'React':>5s}  {'Group':24s}  Reason",
        f"{'-' * 8:8s}  {'-' * 4:>4s}  {'-' * 5:>5s}  {'-' * 24:24s}  {'-' * 32}",
    ]
    if not results:
        lines.append("none      ----  -----  ------------------------  no qualifying LinkedIn comments")
        return "\n".join(lines)
    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        group = _shorten(result.comment_group_id, 24)
        lines.append(
            f"{result.status:8s}  {idea_id:>4s}  {result.reaction_count:5d}  "
            f"{group:24s}  {result.reason}"
        )
    return "\n".join(lines)
