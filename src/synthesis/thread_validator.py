"""Deterministic validation for generated X threads."""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse


THREAD_MARKER_RE = re.compile(r"^\s*TWEET\s+(\d+)\s*:\s*(.*)$", re.IGNORECASE)
METADATA_RE = re.compile(r"^\s*(ATTRIBUTIONS_USED|ATTRIBUTIONS)\s*:", re.IGNORECASE)
CONTINUATION_RE = re.compile(
    r"^\s*\(?(\d+)\s*(?:/|of)\s*(\d+)?\)?\s*[:.-]?\s*"
    r"|\s+\(?(\d+)\s*(?:/|of)\s*(\d+)?\)?\s*$",
    re.IGNORECASE,
)
URLISH_RE = re.compile(r"^[a-z][a-z0-9+.-]*://\S+$", re.IGNORECASE)


@dataclass(frozen=True)
class ThreadPost:
    """A parsed tweet block from a generated thread."""

    number: int
    text: str


@dataclass(frozen=True)
class ThreadValidationIssue:
    """A single thread validation failure."""

    code: str
    message: str
    tweet_number: int | None = None


@dataclass(frozen=True)
class ThreadValidationResult:
    """Structured validation output."""

    posts: list[ThreadPost]
    issues: list[ThreadValidationIssue]

    @property
    def valid(self) -> bool:
        return not self.issues

    @property
    def is_valid(self) -> bool:
        return self.valid

    @property
    def failures(self) -> list[ThreadValidationIssue]:
        return self.issues

    @property
    def failure_reasons(self) -> list[str]:
        return [issue.message for issue in self.issues]


def parse_thread_posts(content: str) -> tuple[list[ThreadPost], list[str]]:
    """Parse TWEET N blocks while preserving empty posts and numbering errors."""
    posts: list[ThreadPost] = []
    unnumbered_blocks: list[str] = []
    current_number: int | None = None
    current_lines: list[str] = []

    def flush_current() -> None:
        nonlocal current_number, current_lines
        if current_number is not None:
            posts.append(ThreadPost(current_number, "\n".join(current_lines).strip()))
        current_number = None
        current_lines = []

    for line in content.splitlines():
        if METADATA_RE.match(line):
            flush_current()
            break

        marker = THREAD_MARKER_RE.match(line)
        if marker:
            flush_current()
            current_number = int(marker.group(1))
            current_lines = []
            inline_text = marker.group(2).strip()
            if inline_text:
                current_lines.append(inline_text)
            continue

        if current_number is None:
            if line.strip():
                unnumbered_blocks.append(line.strip())
        else:
            current_lines.append(line)

    flush_current()
    return posts, unnumbered_blocks


def validate_thread(content: str, max_chars: int = 280) -> ThreadValidationResult:
    """Validate generated X thread text.

    Checks are intentionally deterministic and do not try to repair content.
    """
    posts, unnumbered_blocks = parse_thread_posts(content)
    issues: list[ThreadValidationIssue] = []

    if not content.strip():
        issues.append(ThreadValidationIssue("empty_thread", "Thread is empty"))
        return ThreadValidationResult(posts, issues)

    if unnumbered_blocks:
        issues.append(
            ThreadValidationIssue(
                "invalid_numbering",
                "Thread contains content before the first TWEET marker",
            )
        )

    if not posts:
        issues.append(
            ThreadValidationIssue(
                "invalid_numbering",
                "Thread must use sequential TWEET N markers starting at 1",
            )
        )
        return ThreadValidationResult(posts, issues)

    numbers = [post.number for post in posts]
    expected_numbers = list(range(1, len(posts) + 1))
    if numbers != expected_numbers:
        issues.append(
            ThreadValidationIssue(
                "invalid_numbering",
                "Thread numbering must be sequential starting at 1",
            )
        )

    seen_texts: dict[str, int] = {}
    continuation_markers = [
        _extract_continuation_marker(post.text) for post in posts
    ]
    continuation_required = any(marker is not None for marker in continuation_markers)

    for index, post in enumerate(posts, start=1):
        tweet_number = post.number
        text = post.text.strip()
        if not text:
            issues.append(
                ThreadValidationIssue(
                    "empty_post",
                    f"Tweet {tweet_number} is empty",
                    tweet_number=tweet_number,
                )
            )
            continue

        if len(text) > max_chars:
            issues.append(
                ThreadValidationIssue(
                    "overlong_tweet",
                    f"Tweet {tweet_number} is {len(text)} characters; max is {max_chars}",
                    tweet_number=tweet_number,
                )
            )

        normalized = _normalize_tweet_text(text)
        if normalized in seen_texts:
            issues.append(
                ThreadValidationIssue(
                    "duplicate_tweet",
                    f"Tweet {tweet_number} duplicates tweet {seen_texts[normalized]}",
                    tweet_number=tweet_number,
                )
            )
        else:
            seen_texts[normalized] = tweet_number

        if _looks_like_url_only(text) and not _is_valid_url(text):
            issues.append(
                ThreadValidationIssue(
                    "broken_url_only_tweet",
                    f"Tweet {tweet_number} is a broken URL-only tweet",
                    tweet_number=tweet_number,
                )
            )

        if continuation_required:
            marker = continuation_markers[index - 1]
            if marker is None and index < len(posts):
                issues.append(
                    ThreadValidationIssue(
                        "missing_continuation_marker",
                        f"Tweet {tweet_number} is missing a continuation marker",
                        tweet_number=tweet_number,
                    )
                )
            elif marker is not None:
                marker_number, marker_total = marker
                if marker_number != index or (
                    marker_total is not None and marker_total != len(posts)
                ):
                    issues.append(
                        ThreadValidationIssue(
                            "invalid_continuation_marker",
                            f"Tweet {tweet_number} has an invalid continuation marker",
                            tweet_number=tweet_number,
                        )
                    )

    return ThreadValidationResult(posts, issues)


def _normalize_tweet_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text.strip()).lower()
    marker = _extract_continuation_marker(text)
    if marker is not None:
        text = CONTINUATION_RE.sub(" ", text).strip()
    return text


def _extract_continuation_marker(text: str) -> tuple[int, int | None] | None:
    match = CONTINUATION_RE.search(text)
    if not match:
        return None

    number_text = match.group(1) or match.group(3)
    total_text = match.group(2) or match.group(4)
    return int(number_text), int(total_text) if total_text else None


def _looks_like_url_only(text: str) -> bool:
    stripped = text.strip().strip("<>()[]{}")
    if any(char.isspace() for char in stripped):
        return False
    return "://" in stripped


def _is_valid_url(text: str) -> bool:
    stripped = text.strip().strip("<>()[]{}")
    if not URLISH_RE.match(stripped):
        return False
    parsed = urlparse(stripped)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


validate_x_thread = validate_thread
