"""Seed content ideas from high-signal GitHub PR review activity."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


SOURCE_NAME = "github_pr_review_seed"
DEFAULT_DAYS = 14
DEFAULT_LIMIT = 25
DEFAULT_MIN_SCORE = 60
BODY_EXCERPT_CHARS = 360
PR_REVIEW_ACTIVITY_TYPES = (
    "pull_request_review",
    "pull_request_review_comment",
    "github_pr_review",
    "github_pr_review_comment",
    "pr_review_comment",
)

APPROVAL_ONLY = {
    "approved",
    "approve",
    "lgtm",
    "looks good",
    "looks good to me",
    "ship it",
    "shipit",
    "+1",
}
TRADEOFF_TERMS = (
    "tradeoff",
    "trade-off",
    "instead",
    "prefer",
    "because",
    "alternative",
    "approach",
    "decision",
    "resolved",
)
CODE_REFERENCE_TERMS = ("line", "path", "diff", "hunk", "file", "function", "method")
SECURITY_PERFORMANCE_LABELS = ("security", "performance", "perf", "scalability")


@dataclass(frozen=True)
class GitHubPRReviewIdeaCandidate:
    repo_name: str
    pr_number: str
    review_id: str
    activity_id: str
    activity_type: str
    author: str
    state: str
    labels: list[str]
    score: int
    score_reasons: list[str]
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GitHubPRReviewIdeaSeedResult:
    status: str
    repo_name: str
    pr_number: str
    review_id: str
    activity_type: str
    author: str
    score: int
    idea_id: int | None
    reason: str
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def seed_github_pr_review_ideas(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_score: int = DEFAULT_MIN_SCORE,
    limit: int | None = DEFAULT_LIMIT,
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[GitHubPRReviewIdeaSeedResult]:
    """Create or preview content ideas from recent PR review activity."""
    if days <= 0:
        return []
    if min_score < 0:
        raise ValueError("min_score must be non-negative")
    if limit is not None and limit <= 0:
        return []

    candidates = build_github_pr_review_idea_candidates(
        db,
        days=days,
        limit=limit,
        now=now,
    )
    results: list[GitHubPRReviewIdeaSeedResult] = []
    for candidate in candidates:
        if candidate.score < min_score:
            results.append(
                _result(
                    "skipped",
                    candidate,
                    idea_id=None,
                    reason=f"score {candidate.score} below {min_score}",
                )
            )
            continue

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


def build_github_pr_review_idea_candidates(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> list[GitHubPRReviewIdeaCandidate]:
    """Return scored PR review candidates in deterministic order."""
    if days <= 0 or (limit is not None and limit <= 0):
        return []
    now = now or datetime.now(timezone.utc)
    rows = _recent_pr_review_rows(db, days=days, limit=limit, now=now)
    candidates = [
        candidate
        for row in rows
        if (candidate := github_pr_review_row_to_candidate(row)) is not None
    ]
    return sorted(
        candidates,
        key=lambda item: (
            item.score,
            item.source_metadata.get("updated_at", ""),
            item.repo_name,
            item.pr_number,
            item.review_id,
        ),
        reverse=True,
    )


def github_pr_review_row_to_candidate(row: dict[str, Any]) -> GitHubPRReviewIdeaCandidate | None:
    """Convert one github_activity row into a scored PR review idea candidate."""
    metadata = _metadata(row)
    body = _clean(row.get("body") or metadata.get("body") or metadata.get("comment_body"))
    if not body or _is_bot_author(row.get("author")) or _is_approval_only(body, row, metadata):
        return None

    repo_name = _clean(row.get("repo_name"))
    activity_type = _clean(row.get("activity_type"))
    pr_number = _clean(
        metadata.get("pr_number")
        or metadata.get("pull_request_number")
        or metadata.get("parent_pr_number")
        or metadata.get("parent_number")
        or _pr_number_from_url(row.get("url"))
        or row.get("number")
    )
    review_id = _clean(
        metadata.get("review_id")
        or metadata.get("comment_id")
        or metadata.get("id")
        or row.get("number")
    )
    if not repo_name or not pr_number or not review_id:
        return None

    labels = _labels(row, metadata)
    state = _clean(metadata.get("state") or row.get("state")).lower()
    author = _clean(row.get("author") or metadata.get("author") or "unknown")
    url = _clean(row.get("url") or metadata.get("html_url") or metadata.get("url"))
    review_url = _clean(metadata.get("review_url") or metadata.get("pull_request_review_url"))
    comment_url = url if "comment" in activity_type or "discussion" in url else _clean(metadata.get("comment_url"))
    score, score_reasons = score_github_pr_review_activity(row, body=body, metadata=metadata, labels=labels)
    priority = "high" if score >= 85 else "normal"
    path = _clean(metadata.get("path") or metadata.get("file_path"))
    line = metadata.get("line") or metadata.get("start_line") or metadata.get("original_line")
    excerpt = _shorten(body, BODY_EXCERPT_CHARS)
    title = _clean(row.get("title") or metadata.get("title") or f"PR review on #{pr_number}")
    topic = f"{repo_name}: PR #{pr_number} review lesson"
    if path:
        topic = f"{topic} in {path}"
    note = (
        f"Turn a GitHub PR review from {author} on {repo_name}#{pr_number} into a draft. "
        f"Review state: {state or 'unknown'}. Labels: {', '.join(labels) or 'none'}. "
        f"Code reference: {path or 'not captured'}{f':{line}' if line else ''}. "
        f"Score {score} ({'; '.join(score_reasons)}). "
        f"Review excerpt: {excerpt}. "
        "Suggested angle: explain the concrete engineering lesson behind the review, "
        "including the tradeoff and the code-review checklist it implies."
    )
    activity_id = _activity_id(row)
    source_metadata = {
        "source": SOURCE_NAME,
        "activity_id": activity_id,
        "github_activity_id": row.get("id"),
        "repo": repo_name,
        "repo_name": repo_name,
        "pr_number": pr_number,
        "review_id": review_id,
        "review_url": review_url or (url if activity_type == "pull_request_review" else ""),
        "comment_url": comment_url,
        "url": url,
        "author": author,
        "state": state,
        "labels": labels,
        "score": score,
        "score_reasons": score_reasons,
        "activity_type": activity_type,
        "path": path,
        "line": line,
        "diff_hunk": _clean(metadata.get("diff_hunk")),
        "title": title,
        "updated_at": _clean(row.get("updated_at")),
        "body_excerpt": excerpt,
    }
    return GitHubPRReviewIdeaCandidate(
        repo_name=repo_name,
        pr_number=pr_number,
        review_id=review_id,
        activity_id=activity_id,
        activity_type=activity_type,
        author=author,
        state=state,
        labels=labels,
        score=score,
        score_reasons=score_reasons,
        topic=topic,
        note=note,
        priority=priority,
        source_metadata={key: value for key, value in source_metadata.items() if value not in (None, "", [])},
    )


def score_github_pr_review_activity(
    row: dict[str, Any],
    *,
    body: str | None = None,
    metadata: dict[str, Any] | None = None,
    labels: list[str] | None = None,
) -> tuple[int, list[str]]:
    """Score PR review activity using deterministic, explainable signals."""
    metadata = metadata if metadata is not None else _metadata(row)
    labels = labels if labels is not None else _labels(row, metadata)
    body = _clean(body if body is not None else row.get("body"))
    lowered = body.lower()
    state = _clean(metadata.get("state") or row.get("state")).lower()
    activity_type = _clean(row.get("activity_type")).lower()
    score = 15
    reasons = ["review activity"]

    if activity_type == "pull_request_review":
        score += 8
        reasons.append("review summary")
    elif "review_comment" in activity_type or activity_type == "pull_request_review_comment":
        score += 12
        reasons.append("review thread comment")

    if state in {"changes_requested", "request_changes", "requested_changes"}:
        score += 28
        reasons.append("requested changes")

    if any(term in lowered for term in TRADEOFF_TERMS):
        score += 18
        reasons.append("technical tradeoff")

    if any(_label_matches(label, SECURITY_PERFORMANCE_LABELS) for label in labels):
        score += 18
        reasons.append("security/performance label")

    if _has_concrete_code_reference(metadata, lowered):
        score += 16
        reasons.append("concrete code reference")

    if any(term in lowered for term in ("security", "performance", "regression", "reliability")):
        score += 10
        reasons.append("risk-oriented review")

    if any(term in lowered for term in ("test", "coverage", "edge case")):
        score += 8
        reasons.append("test or edge-case guidance")

    if len(body) >= 180:
        score += 6
        reasons.append("substantive detail")

    return min(score, 100), reasons


def summarize_github_pr_review_seed_results(
    results: list[GitHubPRReviewIdeaSeedResult],
) -> dict[str, int]:
    return {
        "created": sum(1 for result in results if result.status == "created"),
        "proposed": sum(1 for result in results if result.status == "proposed"),
        "skipped": sum(1 for result in results if result.status == "skipped"),
    }


def format_github_pr_review_seed_json(results: list[GitHubPRReviewIdeaSeedResult]) -> str:
    return json.dumps(
        {
            "summary": summarize_github_pr_review_seed_results(results),
            "results": [result.to_dict() for result in results],
        },
        indent=2,
        sort_keys=True,
    )


def format_github_pr_review_seed_text(results: list[GitHubPRReviewIdeaSeedResult]) -> str:
    summary = summarize_github_pr_review_seed_results(results)
    lines = [
        f"created={summary['created']} proposed={summary['proposed']} skipped={summary['skipped']}",
        f"{'Status':8s}  {'ID':>4s}  {'Score':>5s}  {'Review':30s}  Reason / topic",
        f"{'-' * 8:8s}  {'-' * 4:>4s}  {'-' * 5:>5s}  {'-' * 30:30s}  {'-' * 40}",
    ]
    if not results:
        lines.append("none      ----  -----  ------------------------------  no qualifying PR reviews")
        return "\n".join(lines)
    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        ref = _shorten(f"{result.repo_name}#{result.pr_number}/{result.review_id}", 30)
        topic = _shorten(re.sub(r"\s+", " ", result.topic), 48)
        lines.append(
            f"{result.status:8s}  {idea_id:>4s}  {result.score:5d}  "
            f"{ref:30s}  {result.reason}: {topic}"
        )
    return "\n".join(lines)


def _recent_pr_review_rows(
    db: Any,
    *,
    days: int,
    limit: int | None,
    now: datetime,
) -> list[dict[str, Any]]:
    if not getattr(db, "conn", None):
        return []
    cutoff = (now - timedelta(days=days)).isoformat()
    placeholders = ",".join("?" for _ in PR_REVIEW_ACTIVITY_TYPES)
    params: list[Any] = [cutoff, *PR_REVIEW_ACTIVITY_TYPES]
    limit_clause = ""
    if limit is not None:
        limit_clause = " LIMIT ?"
        params.append(limit)
    rows = db.conn.execute(
        f"""SELECT * FROM github_activity
            WHERE updated_at >= ?
              AND activity_type IN ({placeholders})
            ORDER BY updated_at DESC, id DESC{limit_clause}""",
        tuple(params),
    ).fetchall()
    mapper = getattr(db, "_github_activity_from_row", None)
    if callable(mapper):
        return [mapper(row) for row in rows]
    return [dict(row) for row in rows]


def _result(
    status: str,
    candidate: GitHubPRReviewIdeaCandidate,
    *,
    idea_id: int | None,
    reason: str,
) -> GitHubPRReviewIdeaSeedResult:
    return GitHubPRReviewIdeaSeedResult(
        status=status,
        repo_name=candidate.repo_name,
        pr_number=candidate.pr_number,
        review_id=candidate.review_id,
        activity_type=candidate.activity_type,
        author=candidate.author,
        score=candidate.score,
        idea_id=idea_id,
        reason=reason,
        topic=candidate.topic,
        note=candidate.note,
        priority=candidate.priority,
        source_metadata=candidate.source_metadata,
    )


def _metadata(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("metadata")
    if isinstance(value, dict):
        return value
    try:
        decoded = json.loads(str(value or "{}"))
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _labels(row: dict[str, Any], metadata: dict[str, Any]) -> list[str]:
    raw = row.get("labels") or metadata.get("labels") or []
    if isinstance(raw, str):
        try:
            decoded = json.loads(raw)
            raw = decoded if isinstance(decoded, list) else [raw]
        except (TypeError, ValueError):
            raw = [part.strip() for part in raw.split(",")]
    return sorted({_clean(label) for label in raw if _clean(label)})


def _activity_id(row: dict[str, Any]) -> str:
    value = row.get("activity_id")
    if value:
        return str(value)
    return f"{row.get('repo_name', '')}#{row.get('number', '')}:{row.get('activity_type', '')}"


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _shorten(text: str | None, width: int) -> str:
    value = _clean(text)
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _is_bot_author(author: Any) -> bool:
    value = _clean(author).lower()
    return bool(value) and (value.endswith("[bot]") or value.endswith("-bot") or value == "github-actions")


def _is_approval_only(body: str, row: dict[str, Any], metadata: dict[str, Any]) -> bool:
    state = _clean(metadata.get("state") or row.get("state")).lower()
    normalized = re.sub(r"[\s.!:+✅👍🚀;-]+", " ", body.lower()).strip()
    compact = normalized.replace(" ", "")
    return (state == "approved" and len(normalized) <= 40) or normalized in APPROVAL_ONLY or compact in APPROVAL_ONLY


def _label_matches(label: str, terms: tuple[str, ...]) -> bool:
    normalized = label.lower()
    return any(term in normalized for term in terms)


def _has_concrete_code_reference(metadata: dict[str, Any], lowered_body: str) -> bool:
    if any(metadata.get(key) not in (None, "") for key in ("path", "file_path", "line", "start_line", "diff_hunk")):
        return True
    return any(term in lowered_body for term in CODE_REFERENCE_TERMS)


def _pr_number_from_url(value: Any) -> str:
    if not value:
        return ""
    parts = str(value).split("#", 1)[0].rstrip("/").split("/")
    try:
        marker = parts.index("pull")
        return parts[marker + 1]
    except (ValueError, IndexError):
        return ""
