"""Evaluate continuity between adjacent posts in generated X threads."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
from pathlib import Path
from typing import Any

from synthesis.thread_validator import THREAD_MARKER_RE, parse_thread_posts


DEFAULT_LIMIT = 50
DEFAULT_MIN_OVERLAP = 0.18
DEFAULT_MAX_OPENING_TOKENS = 4

_TOKEN_RE = re.compile(r"[a-z0-9]+(?:'[a-z0-9]+)?")
_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")
_NUMBER_PREFIX_RE = re.compile(r"^\s*(?:\(?\d+\)?\s*(?:/|of|[.)-])\s*)+")
_BRIDGE_RE = re.compile(
    r"(?i)^\s*(also|that|those|this|these|then|because|so|but|however|instead|meanwhile|"
    r"next|finally|the takeaway|the result|the fix|the problem|the reason|in practice|from there|"
    r"on top of that|which means|as a result|here'?s why)\b"
)
_OPENING_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "for",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "the",
    "to",
}
_CONTENT_STOPWORDS = _OPENING_STOPWORDS | {
    "about",
    "after",
    "again",
    "all",
    "also",
    "be",
    "before",
    "by",
    "can",
    "did",
    "does",
    "from",
    "had",
    "has",
    "have",
    "how",
    "if",
    "into",
    "its",
    "just",
    "more",
    "not",
    "only",
    "our",
    "should",
    "so",
    "that",
    "their",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "was",
    "we",
    "what",
    "when",
    "where",
    "which",
    "why",
    "with",
    "you",
}
_ORPHANED_ENDING_RE = re.compile(
    r"(?i)^\s*(also|another|next|meanwhile|for example|one more thing|the next step)\b"
)
_CONCLUSION_RE = re.compile(
    r"(?i)\b(finally|in short|the takeaway|takeaway|the result|that is why|that means|"
    r"ship it|use this|start here|end state|bottom line)\b"
)


@dataclass(frozen=True)
class ThreadContinuityIssue:
    """One deterministic continuity issue in a thread."""

    issue_type: str
    post_index: int
    previous_post_index: int | None
    severity: str
    detail: str
    recommendation: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ThreadContinuityRecord:
    """Continuity result for one generated X thread."""

    thread_id: int | str
    post_count: int
    issue_count: int
    continuity_score: float
    issues_by_type: dict[str, int]
    issues: tuple[ThreadContinuityIssue, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "continuity_score": self.continuity_score,
            "issue_count": self.issue_count,
            "issues": [issue.to_dict() for issue in self.issues],
            "issues_by_type": dict(sorted(self.issues_by_type.items())),
            "post_count": self.post_count,
            "thread_id": self.thread_id,
        }


@dataclass(frozen=True)
class ThreadContinuityReport:
    """Read-only continuity report for generated X threads."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    threads: tuple[ThreadContinuityRecord, ...]

    @property
    def blocking_issue_count(self) -> int:
        return sum(thread.issue_count for thread in self.threads)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "thread_continuity",
            "blocking_issue_count": self.blocking_issue_count,
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "threads": [thread.to_dict() for thread in self.threads],
            "totals": dict(sorted(self.totals.items())),
        }


def build_thread_continuity_report(
    source: Any,
    *,
    min_overlap: float = DEFAULT_MIN_OVERLAP,
    max_opening_tokens: int = DEFAULT_MAX_OPENING_TOKENS,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ThreadContinuityReport:
    """Build a continuity report from thread records or post text lists."""

    if min_overlap < 0:
        raise ValueError("min_overlap must be non-negative")
    if max_opening_tokens <= 0:
        raise ValueError("max_opening_tokens must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive when provided")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    thread_inputs = _thread_inputs(source)
    if limit is not None:
        thread_inputs = thread_inputs[:limit]

    threads = tuple(
        _record_for_thread(
            thread_id=thread_id,
            posts=posts,
            min_overlap=min_overlap,
            max_opening_tokens=max_opening_tokens,
        )
        for thread_id, posts in thread_inputs
    )
    score_sum = sum(thread.continuity_score for thread in threads)
    issue_counts: dict[str, int] = {}
    for thread in threads:
        for issue_type, count in thread.issues_by_type.items():
            issue_counts[issue_type] = issue_counts.get(issue_type, 0) + count

    thread_count = len(threads)
    return ThreadContinuityReport(
        generated_at=generated_at.isoformat(),
        filters={
            "limit": limit,
            "max_opening_tokens": max_opening_tokens,
            "min_overlap": min_overlap,
        },
        totals={
            "aggregate_continuity_score": round(score_sum / thread_count, 2)
            if thread_count
            else 100.0,
            "issue_count": sum(issue_counts.values()),
            "issues_by_type": dict(sorted(issue_counts.items())),
            "post_count": sum(thread.post_count for thread in threads),
            "thread_count": thread_count,
            "threads_with_issues": sum(1 for thread in threads if thread.issue_count),
        },
        threads=threads,
    )


def read_thread_continuity_input(input_path: str | Any) -> list[Any]:
    """Read candidate thread records from a JSON or JSONL file."""

    path = str(input_path)
    raw = Path(path).read_text(encoding="utf-8")
    stripped = raw.strip()
    if not stripped:
        return []
    if stripped[0] in "[{":
        parsed = json.loads(stripped)
        return _records_from_payload(parsed)

    records: list[Any] = []
    for line_number, line in enumerate(raw.splitlines(), start=1):
        text = line.strip()
        if not text:
            continue
        try:
            records.append(json.loads(text))
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSONL on line {line_number}: {exc.msg}") from exc
    return records


def format_thread_continuity_json(report: ThreadContinuityReport) -> str:
    """Serialize the continuity report as deterministic JSON."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _record_for_thread(
    *,
    thread_id: int | str,
    posts: Sequence[str],
    min_overlap: float,
    max_opening_tokens: int,
) -> ThreadContinuityRecord:
    cleaned_posts = tuple(_clean_post(post) for post in posts if _clean_post(post))
    issues = tuple(
        _continuity_issues(
            cleaned_posts,
            min_overlap=min_overlap,
            max_opening_tokens=max_opening_tokens,
        )
    )
    issues_by_type: dict[str, int] = {}
    for issue in issues:
        issues_by_type[issue.issue_type] = issues_by_type.get(issue.issue_type, 0) + 1
    return ThreadContinuityRecord(
        thread_id=thread_id,
        post_count=len(cleaned_posts),
        issue_count=len(issues),
        continuity_score=_score(issues),
        issues_by_type=issues_by_type,
        issues=issues,
    )


def _continuity_issues(
    posts: Sequence[str],
    *,
    min_overlap: float,
    max_opening_tokens: int,
) -> list[ThreadContinuityIssue]:
    if len(posts) <= 1:
        return []

    issues: list[ThreadContinuityIssue] = []
    openings: dict[str, int] = {}
    for index, post in enumerate(posts, start=1):
        opening = _opening_signature(post, max_tokens=max_opening_tokens)
        if opening and opening in openings:
            previous = openings[opening]
            issues.append(
                ThreadContinuityIssue(
                    issue_type="repeated_opening",
                    post_index=index,
                    previous_post_index=previous,
                    severity="warning",
                    detail=f"Post {index} repeats the opening phrase from post {previous}.",
                    recommendation=(
                        f"Rewrite post {index} so it advances the prior post instead of "
                        "restarting with the same phrase."
                    ),
                )
            )
        openings.setdefault(opening, index)

    for index in range(1, len(posts)):
        previous = posts[index - 1]
        current = posts[index]
        overlap = _lexical_overlap(previous, current)
        has_bridge = _has_transition_cue(current)
        if overlap < min_overlap and not has_bridge:
            issues.append(
                ThreadContinuityIssue(
                    issue_type="abrupt_topic_shift",
                    post_index=index + 1,
                    previous_post_index=index,
                    severity="error",
                    detail=(
                        f"Post {index + 1} shares {overlap:.0%} key-term overlap "
                        f"with post {index}."
                    ),
                    recommendation=(
                        f"Add a bridge from post {index} to post {index + 1}, or move "
                        "the new topic into a separate thread."
                    ),
                )
            )
        elif not has_bridge and overlap < (min_overlap + 0.12):
            issues.append(
                ThreadContinuityIssue(
                    issue_type="missing_transition_cue",
                    post_index=index + 1,
                    previous_post_index=index,
                    severity="warning",
                    detail=(
                        f"Post {index + 1} has weak lexical carryover and no explicit "
                        "transition cue."
                    ),
                    recommendation=(
                        "Start the post with a bridge word or repeat a concrete noun "
                        "from the previous post."
                    ),
                )
            )

    final = posts[-1]
    if _is_orphaned_ending(final):
        issues.append(
            ThreadContinuityIssue(
                issue_type="orphaned_ending",
                post_index=len(posts),
                previous_post_index=len(posts) - 1,
                severity="warning",
                detail="The final post reads like another middle post instead of a conclusion.",
                recommendation="Close the thread with a takeaway, result, or explicit final action.",
            )
        )
    return sorted(issues, key=lambda issue: (issue.post_index, issue.issue_type))


def _thread_inputs(source: Any) -> list[tuple[int | str, tuple[str, ...]]]:
    records = _records_from_payload(source)
    if _looks_like_post_list(records):
        return [(1, tuple(str(post) for post in records))]

    inputs: list[tuple[int | str, tuple[str, ...]]] = []
    for index, record in enumerate(records, start=1):
        thread_id, posts = _posts_from_record(record, fallback_id=index)
        inputs.append((thread_id, tuple(posts)))
    return inputs


def _records_from_payload(payload: Any) -> list[Any]:
    if isinstance(payload, Mapping):
        for key in ("threads", "records", "candidates", "items", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
        return [payload]
    if isinstance(payload, list):
        return payload
    if isinstance(payload, tuple):
        return list(payload)
    if isinstance(payload, str):
        return [payload]
    if isinstance(payload, Iterable):
        return list(payload)
    raise TypeError("source must be a thread record, post list, or iterable of records")


def _looks_like_post_list(records: list[Any]) -> bool:
    return bool(records) and all(isinstance(item, str) for item in records)


def _posts_from_record(record: Any, *, fallback_id: int) -> tuple[int | str, list[str]]:
    if isinstance(record, str):
        return fallback_id, parse_thread_text(record)
    if isinstance(record, Sequence) and not isinstance(record, (bytes, bytearray, str)):
        return fallback_id, [str(item) for item in record]
    if not isinstance(record, Mapping):
        return fallback_id, [str(record)]

    thread_id = (
        record.get("thread_id")
        or record.get("id")
        or record.get("content_id")
        or record.get("candidate_id")
        or fallback_id
    )
    for key in ("posts", "tweets", "thread", "items", "parts"):
        value = record.get(key)
        posts = _posts_from_value(value)
        if posts:
            return thread_id, posts
    for key in ("content", "text", "body"):
        value = record.get(key)
        if value:
            return thread_id, parse_thread_text(str(value))
    return thread_id, []


def _posts_from_value(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return parse_thread_text(value)
    if isinstance(value, Mapping):
        for key in ("posts", "tweets", "thread", "items", "parts"):
            posts = _posts_from_value(value.get(key))
            if posts:
                return posts
        for key in ("text", "content", "body"):
            text = value.get(key)
            if text:
                return [str(text)]
        return []
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        posts: list[str] = []
        for item in value:
            if isinstance(item, Mapping):
                text = item.get("text") or item.get("content") or item.get("body")
                if text:
                    posts.append(str(text))
            else:
                posts.append(str(item))
        return posts
    return []


def parse_thread_text(content: str) -> list[str]:
    """Parse common stored X thread text shapes into ordered post text."""

    text = str(content or "").strip()
    if not text:
        return []
    decoded = _decode_json(text)
    if decoded is not None:
        posts = _posts_from_value(decoded)
        if posts:
            return posts
    if any(THREAD_MARKER_RE.match(line) for line in text.splitlines()):
        posts, _ = parse_thread_posts(text)
        return [post.text for post in posts]
    parts = [part.strip() for part in re.split(r"\n\s*\n+", text) if part.strip()]
    return parts or [text]


def _decode_json(text: str) -> Any | None:
    stripped = text.strip()
    if not stripped or stripped[0] not in "[{":
        return None
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return None


def _clean_post(value: Any) -> str:
    text = _URL_RE.sub(" ", str(value or ""))
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return _NUMBER_PREFIX_RE.sub("", text).strip()


def _opening_signature(post: str, *, max_tokens: int) -> str:
    tokens = [
        token
        for token in _TOKEN_RE.findall(_clean_post(post).lower().replace("\u2019", "'"))
        if token not in _OPENING_STOPWORDS
    ]
    return " ".join(tokens[: min(max_tokens, 3)])


def _content_terms(post: str) -> set[str]:
    return {
        token
        for token in _TOKEN_RE.findall(_clean_post(post).lower().replace("\u2019", "'"))
        if token not in _CONTENT_STOPWORDS and len(token) > 2
    }


def _lexical_overlap(previous: str, current: str) -> float:
    previous_terms = _content_terms(previous)
    current_terms = _content_terms(current)
    if not previous_terms or not current_terms:
        return 0.0
    return len(previous_terms & current_terms) / len(previous_terms | current_terms)


def _has_transition_cue(post: str) -> bool:
    return bool(_BRIDGE_RE.search(_clean_post(post)))


def _is_orphaned_ending(post: str) -> bool:
    text = _clean_post(post)
    if not text:
        return False
    if _CONCLUSION_RE.search(text):
        return False
    if _ORPHANED_ENDING_RE.search(text):
        return True
    return text.endswith((",", ":", ";")) or text.endswith(("...", "\u2026"))


def _score(issues: Sequence[ThreadContinuityIssue]) -> float:
    penalties = {
        "abrupt_topic_shift": 22,
        "missing_transition_cue": 10,
        "orphaned_ending": 12,
        "repeated_opening": 14,
    }
    score = 100 - sum(penalties.get(issue.issue_type, 10) for issue in issues)
    return float(max(0, score))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
