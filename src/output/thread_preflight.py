"""Preflight validation for platform thread payloads before publishing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from synthesis.thread_validator import THREAD_MARKER_RE, parse_thread_posts

from .platform_adapter import BLUESKY_GRAPHEME_LIMIT, count_graphemes


X_CHARACTER_LIMIT = 280
_VALID_PLATFORMS = {"x", "bluesky"}


@dataclass(frozen=True)
class ThreadPreflightIssue:
    """A structured thread payload validation issue."""

    code: str
    message: str
    platform: str
    post_index: int | None = None

    def as_dict(self) -> dict:
        return {
            "code": self.code,
            "message": self.message,
            "platform": self.platform,
            "post_index": self.post_index,
        }


@dataclass(frozen=True)
class ThreadPreflightResult:
    """Serializable thread preflight result."""

    platform: str
    checked: bool
    passed: bool
    status: str
    post_count: int
    issues: list[ThreadPreflightIssue]

    @property
    def valid(self) -> bool:
        return self.passed

    def as_dict(self) -> dict:
        return {
            "platform": self.platform,
            "checked": self.checked,
            "passed": self.passed,
            "status": self.status,
            "post_count": self.post_count,
            "issues": [issue.as_dict() for issue in self.issues],
        }

    def error_summary(self) -> str:
        return "; ".join(
            f"{issue.platform} post {issue.post_index}: {issue.code}: {issue.message}"
            if issue.post_index is not None
            else f"{issue.platform}: {issue.code}: {issue.message}"
            for issue in self.issues
        )


def split_thread_content_for_preflight(content: str) -> list[str]:
    """Split generated X thread content while preserving empty marked posts.

    Older content in this project may be stored as unmarked text and parsed by
    ``parse_thread_content`` at publish time. For marked ``TWEET N:`` threads we
    use the synthesis parser so empty or skipped posts remain visible to
    preflight instead of being silently dropped.
    """
    if _contains_thread_markers(content):
        posts, _ = parse_thread_posts(content)
        return [post.text for post in posts]

    from .x_client import parse_thread_content

    return parse_thread_content(content)


def validate_thread_preflight(
    platform: str,
    posts: list[Any],
    *,
    content_type: str = "x_thread",
) -> ThreadPreflightResult:
    """Validate the platform-ready thread payload without mutating it."""
    if platform not in _VALID_PLATFORMS:
        raise ValueError("platform must be one of: x, bluesky")

    if content_type != "x_thread":
        return ThreadPreflightResult(
            platform=platform,
            checked=False,
            passed=True,
            status="not_applicable",
            post_count=len(posts or []),
            issues=[],
        )

    issues: list[ThreadPreflightIssue] = []
    if not posts:
        issues.append(
            ThreadPreflightIssue(
                "empty_thread",
                "Thread payload has no posts",
                platform,
            )
        )

    seen_order: list[int] = []
    for position, raw_post in enumerate(posts or [], start=1):
        text = _post_text(raw_post)
        index = _post_index(raw_post, position)
        if index is not None:
            seen_order.append(index)

        if index != position:
            issues.append(
                ThreadPreflightIssue(
                    "out_of_order_post",
                    f"Post index must be {position}; got {index}",
                    platform,
                    position,
                )
            )

        if not text.strip():
            issues.append(
                ThreadPreflightIssue(
                    "empty_post",
                    "Thread post text is empty",
                    platform,
                    position,
                )
            )
            continue

        limit = _platform_limit(platform)
        length = _platform_length(platform, text)
        if length > limit:
            issues.append(
                ThreadPreflightIssue(
                    "over_limit_post",
                    f"Thread post is {length} {_limit_unit(platform)}; max is {limit}",
                    platform,
                    position,
                )
            )

        if isinstance(raw_post, dict) and position > 1:
            _validate_reply_metadata(platform, raw_post, position, issues)

    expected_order = list(range(1, len(posts or []) + 1))
    if seen_order and seen_order != expected_order:
        issues.append(
            ThreadPreflightIssue(
                "malformed_order",
                "Thread post indexes must be sequential starting at 1",
                platform,
            )
        )

    return ThreadPreflightResult(
        platform=platform,
        checked=True,
        passed=not issues,
        status="passed" if not issues else "failed",
        post_count=len(posts or []),
        issues=issues,
    )


def validate_platform_threads(
    payloads: dict[str, list[Any]],
    *,
    content_type: str = "x_thread",
) -> dict[str, ThreadPreflightResult]:
    """Validate multiple platform thread payloads."""
    return {
        platform: validate_thread_preflight(
            platform,
            posts,
            content_type=content_type,
        )
        for platform, posts in payloads.items()
    }


def summarize_thread_preflight_failures(
    results: dict[str, ThreadPreflightResult],
) -> str:
    """Return a compact validation error summary for persistence/logging."""
    summaries = [
        result.error_summary()
        for result in results.values()
        if result.checked and not result.passed
    ]
    return "; ".join(summary for summary in summaries if summary)


def _contains_thread_markers(content: str) -> bool:
    return any(THREAD_MARKER_RE.match(line) for line in content.splitlines())


def _post_text(post: Any) -> str:
    if isinstance(post, dict):
        return str(post.get("text") or "")
    return str(post or "")


def _post_index(post: Any, fallback: int) -> int | None:
    if not isinstance(post, dict):
        return fallback
    value = post.get("index")
    if value is None:
        value = fallback
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _platform_limit(platform: str) -> int:
    return BLUESKY_GRAPHEME_LIMIT if platform == "bluesky" else X_CHARACTER_LIMIT


def _platform_length(platform: str, text: str) -> int:
    return count_graphemes(text) if platform == "bluesky" else len(text)


def _limit_unit(platform: str) -> str:
    return "graphemes" if platform == "bluesky" else "characters"


def _validate_reply_metadata(
    platform: str,
    post: dict,
    position: int,
    issues: list[ThreadPreflightIssue],
) -> None:
    if platform == "x":
        reply_to = post.get("in_reply_to_tweet_id") or post.get("reply_to")
        if not reply_to:
            issues.append(
                ThreadPreflightIssue(
                    "missing_parent_metadata",
                    "Reply post is missing in_reply_to_tweet_id metadata",
                    platform,
                    position,
                )
            )
        return

    reply_to = post.get("reply_to") or {}
    root = post.get("root") or reply_to.get("root") or {}
    parent = post.get("parent") or reply_to.get("parent") or {}
    if not _has_bluesky_ref(root) or not _has_bluesky_ref(parent):
        issues.append(
            ThreadPreflightIssue(
                "missing_reply_metadata",
                "Reply post is missing Bluesky root and parent uri/cid metadata",
                platform,
                position,
            )
        )


def _has_bluesky_ref(ref: object) -> bool:
    return isinstance(ref, dict) and bool(ref.get("uri")) and bool(ref.get("cid"))
