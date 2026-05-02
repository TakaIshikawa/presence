"""Seed content ideas from GitHub pull request review activity."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


SOURCE_NAME = "github_pr_review"
DEFAULT_DAYS = 14
DEFAULT_LIMIT = 10
DEFAULT_PRIORITY = "normal"
BODY_EXCERPT_CHARS = 360
PR_REVIEW_ACTIVITY_TYPES = (
    "pr_review_comment",
    "github_pr_review_comment",
    "pull_request_review_comment",
    "pull_request_review",
    "github_pr_review",
)

APPROVAL_ONLY_PATTERNS = (
    "approved",
    "approve",
    "lgtm",
    "looks good",
    "looks good to me",
    "ship it",
    "shipit",
    ":shipit:",
    "+1",
    ":+1:",
)

LESSON_MARKERS = (
    "because",
    "tradeoff",
    "trade-off",
    "instead",
    "avoid",
    "should",
    "prefer",
    "why",
    "risk",
    "edge case",
    "regression",
    "test",
    "coverage",
    "pattern",
    "design",
    "api",
    "migration",
    "performance",
    "reliability",
    "security",
)


@dataclass(frozen=True)
class PRReviewIdeaCandidate:
    repo_name: str
    number: int | str
    pr_number: int | str | None
    activity_type: str
    title: str
    body_excerpt: str
    author: str
    url: str
    updated_at: str
    activity_id: str
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PRReviewIdeaSeedResult:
    status: str
    repo_name: str
    number: int | str
    pr_number: int | str | None
    activity_type: str
    idea_id: int | None
    reason: str
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def seed_pr_review_ideas(
    db,
    *,
    days: int = DEFAULT_DAYS,
    repo: str | None = None,
    limit: int | None = DEFAULT_LIMIT,
    priority: str = DEFAULT_PRIORITY,
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[PRReviewIdeaSeedResult]:
    """Create content ideas from useful recent PR review comments."""

    if days <= 0 or (limit is not None and limit <= 0):
        return []
    if repo is not None and not repo.strip():
        raise ValueError("repo must not be blank")

    now = now or datetime.now(timezone.utc)
    rows = _recent_pr_review_rows(db, days=days, repo=repo, limit=limit, now=now)

    results: list[PRReviewIdeaSeedResult] = []
    for row in rows:
        candidate, skip_reason = pr_review_row_to_candidate(row, priority=priority)
        if candidate is None:
            if skip_reason:
                results.append(_skipped_row(row, skip_reason, priority))
            continue

        existing = db.find_active_content_idea_for_source_metadata(
            note=candidate.note,
            topic=candidate.topic,
            source=SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        if existing:
            results.append(_result(candidate, "skipped", existing["id"], f"{existing['status']} duplicate"))
            continue

        if dry_run:
            results.append(_result(candidate, "proposed", None, "dry run"))
            continue

        idea_id = db.add_content_idea(
            note=candidate.note,
            topic=candidate.topic,
            priority=candidate.priority,
            source=SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        results.append(_result(candidate, "created", idea_id, "created"))

    return results


def pr_review_row_to_candidate(
    row: dict[str, Any],
    *,
    priority: str = DEFAULT_PRIORITY,
) -> tuple[PRReviewIdeaCandidate | None, str | None]:
    """Convert one github_activity row into a candidate, or a skip reason."""

    body = _normalize_text(row.get("body"))
    if not body:
        return None, "empty body"
    if _is_bot_author(row.get("author")):
        return None, "bot author"
    if _is_approval_only(body, row):
        return None, "approval only"
    if not _has_lesson_signal(body):
        return None, "no lesson signal"

    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    repo_name = str(row.get("repo_name") or "")
    number = row.get("number") or metadata.get("comment_id") or metadata.get("review_id") or ""
    pr_number = (
        metadata.get("pr_number")
        or metadata.get("parent_pr_number")
        or metadata.get("parent_number")
        or _pr_number_from_url(row.get("url"))
        or _pr_number_from_url(metadata.get("pull_request_url"))
    )
    activity_type = str(row.get("activity_type") or "")
    title = str(row.get("title") or _default_title(pr_number, number))
    url = str(row.get("url") or metadata.get("html_url") or "")
    updated_at = str(row.get("updated_at") or row.get("created_at_github") or "")
    activity_id = str(
        row.get("activity_id")
        or metadata.get("activity_id")
        or f"{repo_name}#{number}:{activity_type}"
    )
    path = str(metadata.get("path") or "")
    diff_hunk = str(metadata.get("diff_hunk") or "")
    excerpt = _shorten(body, BODY_EXCERPT_CHARS)
    pr_label = f"PR #{pr_number}" if pr_number not in (None, "") else "a pull request"
    topic = f"{repo_name}: review lesson from {pr_label}"
    if path:
        topic = f"{topic} ({path})"
    note = (
        f"PR review activity in {repo_name} on {pr_label} raised an implementation lesson "
        f"or recurring reviewer concern. Comment: {url or 'stored GitHub activity only'}. "
        f"Reviewer: {row.get('author') or 'unknown'}. "
        f"Path: {path or 'not captured'}. "
        f"Review excerpt: {excerpt} "
        "Suggested angle: turn the reviewer concern into a practical engineering lesson, "
        "tradeoff explanation, or review checklist."
    )
    source_metadata = {
        "source": SOURCE_NAME,
        "activity_id": activity_id,
        "repo_name": repo_name,
        "activity_type": activity_type,
        "number": number,
        "pr_number": pr_number,
        "review_comment_url": url,
        "comment_url": url,
        "review_url": metadata.get("review_url") or metadata.get("pull_request_review_url"),
        "pull_request_url": metadata.get("pull_request_url"),
        "github_activity_id": row.get("id"),
        "author": row.get("author"),
        "path": path,
        "diff_hunk": diff_hunk,
        "title": title,
        "updated_at": updated_at,
        "body_excerpt": excerpt,
    }
    return (
        PRReviewIdeaCandidate(
            repo_name=repo_name,
            number=number,
            pr_number=pr_number,
            activity_type=activity_type,
            title=title,
            body_excerpt=excerpt,
            author=str(row.get("author") or ""),
            url=url,
            updated_at=updated_at,
            activity_id=activity_id,
            topic=topic,
            note=note,
            priority=priority,
            source_metadata={key: value for key, value in source_metadata.items() if value not in (None, "")},
        ),
        None,
    )


def format_pr_review_idea_results_json(results: list[PRReviewIdeaSeedResult]) -> str:
    return json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True)


def format_pr_review_idea_results_table(results: list[PRReviewIdeaSeedResult]) -> str:
    created = sum(1 for result in results if result.status == "created")
    proposed = sum(1 for result in results if result.status == "proposed")
    skipped = sum(1 for result in results if result.status == "skipped")
    lines = [f"created={created} proposed={proposed} skipped={skipped}"]
    lines.append(
        f"{'Status':9s}  {'ID':>4s}  {'Priority':8s}  {'Review':22s}  Topic / reason"
    )
    lines.append(
        f"{'-' * 9:9s}  {'-' * 4:>4s}  {'-' * 8:8s}  "
        f"{'-' * 22:22s}  {'-' * 44}"
    )
    if not results:
        lines.append("none       ----  --------  ----------------------  no eligible PR reviews")
        return "\n".join(lines)

    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        review_ref = f"{_shorten(result.repo_name, 12)}#{result.pr_number or result.number}"
        detail = f"{_shorten(result.topic, 44)} ({result.reason})"
        lines.append(
            f"{result.status:9s}  {idea_id:>4s}  {result.priority:8s}  "
            f"{review_ref:22s}  {detail}"
        )
    return "\n".join(lines)


def _recent_pr_review_rows(
    db,
    *,
    days: int,
    repo: str | None,
    limit: int | None,
    now: datetime,
) -> list[dict[str, Any]]:
    cutoff = (now - timedelta(days=days)).isoformat()
    params: list[Any] = [cutoff, *PR_REVIEW_ACTIVITY_TYPES]
    repo_filter = ""
    if repo:
        repo_filter = " AND repo_name = ?"
        params.append(repo)
    limit_clause = ""
    if limit is not None:
        limit_clause = " LIMIT ?"
        params.append(limit)
    placeholders = ", ".join("?" for _ in PR_REVIEW_ACTIVITY_TYPES)
    cursor = db.conn.execute(
        f"""SELECT * FROM github_activity
            WHERE updated_at >= ?
              AND activity_type IN ({placeholders}){repo_filter}
            ORDER BY updated_at DESC, id DESC{limit_clause}""",
        tuple(params),
    )
    return [db._github_activity_from_row(row) for row in cursor.fetchall()]


def _result(
    candidate: PRReviewIdeaCandidate,
    status: str,
    idea_id: int | None,
    reason: str,
) -> PRReviewIdeaSeedResult:
    return PRReviewIdeaSeedResult(
        status=status,
        repo_name=candidate.repo_name,
        number=candidate.number,
        pr_number=candidate.pr_number,
        activity_type=candidate.activity_type,
        idea_id=idea_id,
        reason=reason,
        topic=candidate.topic,
        note=candidate.note,
        priority=candidate.priority,
        source_metadata=candidate.source_metadata,
    )


def _skipped_row(row: dict[str, Any], reason: str, priority: str) -> PRReviewIdeaSeedResult:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    pr_number = metadata.get("pr_number") or metadata.get("parent_pr_number") or metadata.get("parent_number")
    number = row.get("number") or metadata.get("comment_id") or ""
    repo_name = str(row.get("repo_name") or "")
    activity_type = str(row.get("activity_type") or "")
    activity_id = str(row.get("activity_id") or metadata.get("activity_id") or f"{repo_name}#{number}:{activity_type}")
    source_metadata = {
        "source": SOURCE_NAME,
        "activity_id": activity_id,
        "repo_name": repo_name,
        "activity_type": activity_type,
        "number": number,
        "pr_number": pr_number,
        "review_comment_url": row.get("url"),
    }
    return PRReviewIdeaSeedResult(
        status="skipped",
        repo_name=repo_name,
        number=number,
        pr_number=pr_number,
        activity_type=activity_type,
        idea_id=None,
        reason=reason,
        topic=f"{repo_name}: skipped PR review activity",
        note="",
        priority=priority,
        source_metadata={key: value for key, value in source_metadata.items() if value not in (None, "")},
    )


def _normalize_text(value: object | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _shorten(text: str | None, width: int = 70) -> str:
    value = _normalize_text(text)
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _is_bot_author(author: object | None) -> bool:
    value = str(author or "").strip().lower()
    return bool(value) and (value.endswith("[bot]") or value.endswith("-bot") or value == "github-actions")


def _is_approval_only(body: str, row: dict[str, Any]) -> bool:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    state = str(metadata.get("state") or row.get("state") or "").strip().lower()
    normalized = re.sub(r"[\s.!✅👍🚀:;-]+", " ", body.strip().lower()).strip()
    compact = normalized.replace(" ", "")
    if state == "approved" and len(normalized) <= 40:
        return True
    return normalized in APPROVAL_ONLY_PATTERNS or compact in {"lgtm", "shipit", "+1"}


def _has_lesson_signal(body: str) -> bool:
    lowered = body.lower()
    if len(lowered) >= 120:
        return True
    return any(marker in lowered for marker in LESSON_MARKERS)


def _pr_number_from_url(value: object | None) -> int | None:
    if not value:
        return None
    parts = str(value).split("#", 1)[0].rstrip("/").split("/")
    try:
        marker = parts.index("pull")
        return int(parts[marker + 1])
    except (ValueError, IndexError):
        return None


def _default_title(pr_number: int | str | None, number: int | str) -> str:
    if pr_number not in (None, ""):
        return f"PR review comment on #{pr_number}"
    return f"PR review activity #{number}"
