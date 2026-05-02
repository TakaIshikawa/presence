"""Seed reviewable content ideas from GitHub issue comments."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


SOURCE_NAME = "github_issue_comment_seed"
DEFAULT_DAYS = 14
DEFAULT_MIN_COMMENT_CHARS = 80
COMMENT_EXCERPT_CHARS = 320


@dataclass(frozen=True)
class GitHubIssueCommentIdeaCandidate:
    repo_name: str
    issue_number: str
    comment_id: str
    activity_id: str
    author: str
    url: str
    updated_at: str
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GitHubIssueCommentSeedResult:
    status: str
    repo_name: str
    issue_number: str
    comment_id: str
    author: str
    idea_id: int | None
    reason: str
    topic: str
    note: str
    source_metadata: dict[str, Any]
    priority: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _decode_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        decoded = json.loads(str(value or "{}"))
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _shorten(text: str | None, width: int) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _normalized_author_set(values: list[str] | None) -> set[str]:
    return {value.strip().lower() for value in (values or []) if value.strip()}


def _metadata_value(row: dict[str, Any], metadata: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
        value = metadata.get(key)
        if value not in (None, ""):
            return value
    return None


def _comment_id(row: dict[str, Any], metadata: dict[str, Any]) -> str:
    value = _metadata_value(row, metadata, "comment_id", "github_comment_id")
    for key in ("id", "node_id"):
        if metadata.get(key) not in (None, ""):
            value = metadata[key]
            break
    return _clean(value) or _clean(row.get("number"))


def _issue_number(row: dict[str, Any], metadata: dict[str, Any]) -> str:
    value = _metadata_value(
        row,
        metadata,
        "issue_number",
        "issue",
        "issue_id",
        "parent_number",
    )
    return _clean(value) or _clean(row.get("number"))


def _activity_id(row: dict[str, Any]) -> str:
    value = row.get("activity_id")
    if value:
        return str(value)
    return f"{row.get('repo_name', '')}#{row.get('number', '')}:issue_comment"


def _priority_from_text(text: str) -> str:
    normalized = text.lower()
    high_signal = (
        "blocked",
        "blocking",
        "cannot",
        "can't",
        "confusing",
        "regression",
        "security",
        "production",
        "urgent",
    )
    return "high" if any(term in normalized for term in high_signal) else "normal"


def _angle_from_text(text: str) -> str:
    normalized = text.lower()
    if any(term in normalized for term in ("confusing", "unclear", "docs", "document")):
        return "explain the user confusion and the missing mental model"
    if any(term in normalized for term in ("workaround", "tradeoff", "approach", "alternative")):
        return "unpack the implementation tradeoff and the practical decision path"
    if any(term in normalized for term in ("blocked", "cannot", "can't", "error", "fails", "bug")):
        return "turn the support thread into a diagnostic checklist"
    return "frame the support discussion as a useful lesson for similar users"


def _row_to_candidate(row: dict[str, Any]) -> GitHubIssueCommentIdeaCandidate | None:
    metadata = _decode_metadata(row.get("metadata"))
    repo_name = _clean(row.get("repo_name"))
    issue_number = _issue_number(row, metadata)
    comment_id = _comment_id(row, metadata)
    author = _clean(row.get("author") or metadata.get("author") or "unknown")
    url = _clean(row.get("url") or metadata.get("html_url") or metadata.get("url"))
    updated_at = _clean(row.get("updated_at") or metadata.get("updated_at"))
    body = _clean(row.get("body") or metadata.get("body") or metadata.get("comment_body"))
    if not repo_name or not issue_number or not comment_id or len(body) < DEFAULT_MIN_COMMENT_CHARS:
        return None

    title = _clean(row.get("title") or metadata.get("issue_title"))
    excerpt = _shorten(body, COMMENT_EXCERPT_CHARS)
    angle = _angle_from_text(body)
    topic_subject = title or f"issue #{issue_number}"
    topic = f"{repo_name}: issue comment on {topic_subject}"
    note = (
        f"GitHub issue comment by {author} on {repo_name}#{issue_number}. "
        f"URL: {url or 'none'}. "
        f"Comment excerpt: {excerpt}. "
        f"Suggested angle: {angle}."
    )
    activity_id = _activity_id(row)
    source_metadata = {
        "source": SOURCE_NAME,
        "repo_name": repo_name,
        "issue_number": issue_number,
        "comment_id": comment_id,
        "activity_id": activity_id,
        "author": author,
        "url": url,
        "updated_at": updated_at,
        "title": title,
        "body_excerpt": excerpt,
    }
    return GitHubIssueCommentIdeaCandidate(
        repo_name=repo_name,
        issue_number=issue_number,
        comment_id=comment_id,
        activity_id=activity_id,
        author=author,
        url=url,
        updated_at=updated_at,
        topic=topic,
        note=note,
        priority=_priority_from_text(body),
        source_metadata={
            key: value for key, value in source_metadata.items() if value not in (None, "", [])
        },
    )


def _github_issue_comment_rows(
    db: Any,
    *,
    days: int,
    repo: str | None,
    author: list[str] | None,
    exclude_author: list[str] | None,
    limit: int | None,
    now: datetime,
) -> list[dict[str, Any]]:
    if days <= 0 or (limit is not None and limit <= 0) or not getattr(db, "conn", None):
        return []
    cutoff = (now - timedelta(days=days)).isoformat()
    params: list[Any] = [cutoff]
    filters = ["activity_type = 'issue_comment'", "updated_at >= ?"]
    if repo:
        filters.append("repo_name = ?")
        params.append(repo)
    allowed_authors = _normalized_author_set(author)
    denied_authors = _normalized_author_set(exclude_author)
    if allowed_authors:
        filters.append(f"LOWER(COALESCE(author, '')) IN ({','.join('?' for _ in allowed_authors)})")
        params.extend(sorted(allowed_authors))
    if denied_authors:
        filters.append(f"LOWER(COALESCE(author, '')) NOT IN ({','.join('?' for _ in denied_authors)})")
        params.extend(sorted(denied_authors))

    limit_clause = ""
    if limit is not None:
        limit_clause = " LIMIT ?"
        params.append(limit)

    rows = db.conn.execute(
        f"""SELECT * FROM github_activity
            WHERE {' AND '.join(filters)}
            ORDER BY updated_at DESC, repo_name ASC, number ASC, id ASC{limit_clause}""",
        tuple(params),
    ).fetchall()
    mapper = getattr(db, "_github_activity_from_row", None)
    if callable(mapper):
        return [mapper(row) for row in rows]
    return [dict(row) for row in rows]


def build_github_issue_comment_idea_candidates(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    repo: str | None = None,
    author: list[str] | None = None,
    exclude_author: list[str] | None = None,
    limit: int | None = 25,
    now: datetime | None = None,
) -> list[GitHubIssueCommentIdeaCandidate]:
    """Return deterministic idea candidates from recent GitHub issue comments."""
    now = now or datetime.now(timezone.utc)
    candidates = [
        candidate
        for row in _github_issue_comment_rows(
            db,
            days=days,
            repo=repo,
            author=author,
            exclude_author=exclude_author,
            limit=limit,
            now=now,
        )
        if (candidate := _row_to_candidate(row)) is not None
    ]
    return sorted(
        candidates,
        key=lambda item: (
            item.updated_at,
            item.repo_name,
            item.issue_number,
            item.comment_id,
        ),
        reverse=True,
    )


def _result(
    status: str,
    candidate: GitHubIssueCommentIdeaCandidate,
    *,
    idea_id: int | None,
    reason: str,
) -> GitHubIssueCommentSeedResult:
    return GitHubIssueCommentSeedResult(
        status=status,
        repo_name=candidate.repo_name,
        issue_number=candidate.issue_number,
        comment_id=candidate.comment_id,
        author=candidate.author,
        idea_id=idea_id,
        reason=reason,
        topic=candidate.topic,
        note=candidate.note,
        source_metadata=candidate.source_metadata,
        priority=candidate.priority,
    )


def seed_github_issue_comment_ideas(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    repo: str | None = None,
    author: list[str] | None = None,
    exclude_author: list[str] | None = None,
    limit: int | None = 25,
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[GitHubIssueCommentSeedResult]:
    """Create or preview content ideas from recent GitHub issue comments."""
    candidates = build_github_issue_comment_idea_candidates(
        db,
        days=days,
        repo=repo,
        author=author,
        exclude_author=exclude_author,
        limit=limit,
        now=now,
    )
    results: list[GitHubIssueCommentSeedResult] = []
    for candidate in candidates:
        existing = db.find_active_content_idea_for_source_metadata(
            note=candidate.note,
            topic=candidate.topic,
            source=SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        if existing:
            results.append(
                _result(
                    "skipped",
                    candidate,
                    idea_id=int(existing["id"]),
                    reason=f"{existing['status']} duplicate",
                )
            )
            continue
        if dry_run:
            results.append(_result("proposed", candidate, idea_id=None, reason="dry run"))
            continue
        idea_id = db.add_content_idea(
            note=candidate.note,
            topic=candidate.topic,
            priority=candidate.priority,
            source=SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        results.append(_result("created", candidate, idea_id=idea_id, reason="created"))
    return results


def summarize_github_issue_comment_seed_results(
    results: list[GitHubIssueCommentSeedResult],
) -> dict[str, Any]:
    return {
        "created": sum(1 for result in results if result.status == "created"),
        "proposed": sum(1 for result in results if result.status == "proposed"),
        "skipped": sum(1 for result in results if result.status == "skipped"),
    }


def format_github_issue_comment_seed_json(
    results: list[GitHubIssueCommentSeedResult],
) -> str:
    return json.dumps(
        {
            "summary": summarize_github_issue_comment_seed_results(results),
            "results": [result.to_dict() for result in results],
        },
        indent=2,
        sort_keys=True,
    )


def format_github_issue_comment_seed_text(
    results: list[GitHubIssueCommentSeedResult],
) -> str:
    summary = summarize_github_issue_comment_seed_results(results)
    lines = [
        f"created={summary['created']} proposed={summary['proposed']} skipped={summary['skipped']}",
        f"{'Status':8s}  {'ID':>4s}  {'Priority':8s}  {'Comment':28s}  Reason / topic",
        f"{'-' * 8:8s}  {'-' * 4:>4s}  {'-' * 8:8s}  {'-' * 28:28s}  {'-' * 40}",
    ]
    if not results:
        lines.append("none      ----  --------  ----------------------------  no qualifying comments")
        return "\n".join(lines)
    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        ref = _shorten(f"{result.repo_name}#{result.issue_number}/{result.comment_id}", 28)
        topic = _shorten(re.sub(r"\s+", " ", result.topic), 48)
        lines.append(
            f"{result.status:8s}  {idea_id:>4s}  {result.priority:8s}  "
            f"{ref:28s}  {result.reason}: {topic}"
        )
    return "\n".join(lines)
