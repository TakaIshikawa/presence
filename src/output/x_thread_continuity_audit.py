"""Audit generated X threads for sequence continuity before publishing."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any

from synthesis.thread_validator import THREAD_MARKER_RE, parse_thread_posts


DEFAULT_LIMIT = 50
DEFAULT_MAX_CHARS = 280
DEFAULT_MIN_OVERLAP = 0.12
FINDING_TYPES = (
    "overlong_post",
    "missing_order_metadata",
    "duplicate_opening",
    "low_continuity_transition",
    "broken_reply_chain_metadata",
)
_POST_KEYS = ("thread", "tweets", "posts", "items", "parts")
_TEXT_KEYS = ("text", "content", "body")
_INDEX_KEYS = ("index", "order", "post_index", "tweet_number", "number")
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "for",
    "from",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "of",
    "on",
    "or",
    "so",
    "that",
    "the",
    "their",
    "then",
    "this",
    "to",
    "was",
    "we",
    "when",
    "with",
    "you",
    "your",
}


@dataclass(frozen=True)
class ThreadPost:
    """One parsed post in a generated X thread."""

    index: int
    text: str
    declared_index: int | None = None
    has_order_metadata: bool = False
    raw: Any = None


@dataclass(frozen=True)
class XThreadContinuityFinding:
    """One thread continuity finding."""

    thread_id: int
    post_index: int
    finding_type: str
    severity: str
    suggested_fix: str
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class XThreadContinuityAuditReport:
    """Continuity audit report for generated X thread drafts."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    findings: tuple[XThreadContinuityFinding, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "x_thread_continuity_audit",
            "findings": [finding.to_dict() for finding in self.findings],
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_x_thread_continuity_audit_report(
    db_or_conn: Any,
    *,
    limit: int = DEFAULT_LIMIT,
    max_chars: int = DEFAULT_MAX_CHARS,
    min_overlap: float = DEFAULT_MIN_OVERLAP,
    include_published: bool = False,
    now: datetime | None = None,
) -> XThreadContinuityAuditReport:
    """Return continuity findings for generated X thread drafts."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    if max_chars <= 0:
        raise ValueError("max_chars must be positive")
    if min_overlap < 0:
        raise ValueError("min_overlap must be non-negative")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    filters = {
        "content_type": "x_thread",
        "include_published": include_published,
        "limit": limit,
        "max_chars": max_chars,
        "min_overlap": min_overlap,
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_thread_drafts(conn, schema, include_published=include_published)
    findings: list[XThreadContinuityFinding] = []
    for row in rows:
        findings.extend(
            audit_x_thread_content(
                row.get("content") or "",
                thread_id=int(row["id"]),
                max_chars=max_chars,
                min_overlap=min_overlap,
            )
        )

    findings.sort(key=lambda item: (item.thread_id, item.post_index, item.finding_type))
    findings = findings[:limit]
    counts = Counter(finding.finding_type for finding in findings)
    return XThreadContinuityAuditReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "threads_scanned": len(rows),
            "finding_count": len(findings),
            "by_finding_type": {
                finding_type: counts.get(finding_type, 0)
                for finding_type in FINDING_TYPES
            },
        },
        findings=tuple(findings),
        missing_tables=(),
        missing_columns={},
    )


def audit_x_thread_content(
    content: Any,
    *,
    thread_id: int,
    max_chars: int = DEFAULT_MAX_CHARS,
    min_overlap: float = DEFAULT_MIN_OVERLAP,
) -> list[XThreadContinuityFinding]:
    """Audit one generated thread payload."""
    posts, order_issue = parse_ordered_thread_posts(content)
    findings: list[XThreadContinuityFinding] = []
    if not posts:
        return findings

    if order_issue and len(posts) > 1:
        findings.append(
            XThreadContinuityFinding(
                thread_id=thread_id,
                post_index=posts[0].index,
                finding_type="missing_order_metadata",
                severity="error",
                suggested_fix="Add sequential TWEET N markers or explicit post indexes.",
                detail=order_issue,
            )
        )

    for post in posts:
        if len(post.text) > max_chars:
            findings.append(
                XThreadContinuityFinding(
                    thread_id=thread_id,
                    post_index=post.index,
                    finding_type="overlong_post",
                    severity="error",
                    suggested_fix=f"Trim post {post.index} to {max_chars} characters or split it.",
                    detail=f"Post {post.index} is {len(post.text)} characters; max is {max_chars}.",
                )
            )

    if len(posts) == 1:
        return findings

    findings.extend(_duplicate_opening_findings(thread_id, posts))
    findings.extend(_continuity_findings(thread_id, posts, min_overlap=min_overlap))
    findings.extend(_reply_metadata_findings(thread_id, posts))
    return findings


def parse_ordered_thread_posts(content: Any) -> tuple[list[ThreadPost], str | None]:
    """Parse supported stored thread shapes into ordered posts."""
    decoded = _decode_json(content) if isinstance(content, str) else content
    json_posts = _posts_from_json(decoded)
    if json_posts is not None:
        return json_posts

    text = str(content or "").strip()
    if not text:
        return [], None
    if any(THREAD_MARKER_RE.match(line) for line in text.splitlines()):
        parsed, unnumbered = parse_thread_posts(text)
        posts = [
            ThreadPost(
                index=position,
                declared_index=post.number,
                text=post.text,
                has_order_metadata=True,
            )
            for position, post in enumerate(parsed, start=1)
        ]
        expected = list(range(1, len(posts) + 1))
        actual = [post.declared_index for post in posts]
        if unnumbered:
            return posts, "Thread contains content before the first TWEET marker."
        if actual != expected:
            return posts, "Thread markers must be sequential starting at TWEET 1."
        return posts, None

    paragraphs = [part.strip() for part in re.split(r"\n\s*\n+", text) if part.strip()]
    if len(paragraphs) > 1:
        return [
            ThreadPost(index=index, text=paragraph, has_order_metadata=False)
            for index, paragraph in enumerate(paragraphs, start=1)
        ], "Multi-post thread content is missing explicit ordering metadata."
    return [ThreadPost(index=1, text=text, has_order_metadata=False)], None


def format_x_thread_continuity_audit_json(report: XThreadContinuityAuditReport) -> str:
    """Serialize the continuity audit as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_x_thread_continuity_audit_text(report: XThreadContinuityAuditReport) -> str:
    """Render the continuity audit for command-line review."""
    totals = report.totals
    filters = report.filters
    lines = [
        "X Thread Continuity Audit",
        f"Generated: {report.generated_at}",
        (
            f"Mode: content_type={filters['content_type']} "
            f"include_published={int(filters['include_published'])} "
            f"max_chars={filters['max_chars']} min_overlap={filters['min_overlap']} "
            f"limit={filters['limit']}"
        ),
        (
            f"Threads: scanned={totals['threads_scanned']} "
            f"findings={totals['finding_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + missing)
    if not report.findings:
        lines.extend(["", "No X thread continuity findings found."])
        return "\n".join(lines)

    lines.extend(["", "Findings:"])
    for finding in report.findings:
        lines.append(
            f"- thread_id={finding.thread_id} post_index={finding.post_index} "
            f"severity={finding.severity} type={finding.finding_type}"
        )
        lines.append(f"  detail={finding.detail}")
        lines.append(f"  suggested_fix={finding.suggested_fix}")
    return "\n".join(lines)


def _duplicate_opening_findings(
    thread_id: int,
    posts: list[ThreadPost],
) -> list[XThreadContinuityFinding]:
    seen: dict[str, int] = {}
    findings: list[XThreadContinuityFinding] = []
    for post in posts:
        opening = _opening_key(post.text)
        if not opening:
            continue
        previous = seen.get(opening)
        if previous is not None:
            findings.append(
                XThreadContinuityFinding(
                    thread_id=thread_id,
                    post_index=post.index,
                    finding_type="duplicate_opening",
                    severity="warning",
                    suggested_fix="Rewrite the repeated opening so this post advances the thread.",
                    detail=f"Post {post.index} repeats the opening from post {previous}.",
                )
            )
        else:
            seen[opening] = post.index
    return findings


def _continuity_findings(
    thread_id: int,
    posts: list[ThreadPost],
    *,
    min_overlap: float,
) -> list[XThreadContinuityFinding]:
    findings: list[XThreadContinuityFinding] = []
    previous_tokens = _lexical_tokens(posts[0].text)
    for previous, current in zip(posts, posts[1:]):
        current_tokens = _lexical_tokens(current.text)
        if len(previous_tokens) >= 3 and len(current_tokens) >= 3:
            overlap = len(previous_tokens & current_tokens) / len(
                previous_tokens | current_tokens
            )
            if overlap < min_overlap:
                findings.append(
                    XThreadContinuityFinding(
                        thread_id=thread_id,
                        post_index=current.index,
                        finding_type="low_continuity_transition",
                        severity="warning",
                        suggested_fix=(
                            "Add a bridge phrase or repeat a concrete noun from the "
                            "previous post."
                        ),
                        detail=(
                            f"Posts {previous.index}->{current.index} share "
                            f"{overlap:.2f} lexical overlap; minimum is {min_overlap:.2f}."
                        ),
                    )
                )
        previous_tokens = current_tokens
    return findings


def _reply_metadata_findings(
    thread_id: int,
    posts: list[ThreadPost],
) -> list[XThreadContinuityFinding]:
    if not any(isinstance(post.raw, dict) for post in posts):
        return []
    findings: list[XThreadContinuityFinding] = []
    for post in posts[1:]:
        if not isinstance(post.raw, dict):
            continue
        if _has_reply_metadata(post.raw):
            continue
        findings.append(
            XThreadContinuityFinding(
                thread_id=thread_id,
                post_index=post.index,
                finding_type="broken_reply_chain_metadata",
                severity="error",
                suggested_fix=(
                    "Attach reply metadata that points this post at the previous "
                    "thread post before publishing."
                ),
                detail=f"Post {post.index} is missing reply-chain metadata.",
            )
        )
    return findings


def _posts_from_json(value: Any) -> tuple[list[ThreadPost], str | None] | None:
    sequence = _json_post_sequence(value)
    if sequence is None:
        return None
    posts: list[ThreadPost] = []
    missing_order = False
    invalid_order = False
    for position, item in enumerate(sequence, start=1):
        text = _json_post_text(item)
        declared = _json_post_index(item)
        has_order = declared is not None
        missing_order = missing_order or not has_order
        invalid_order = invalid_order or (declared is not None and declared != position)
        posts.append(
            ThreadPost(
                index=position,
                declared_index=declared,
                text=text,
                has_order_metadata=has_order,
                raw=item,
            )
        )
    if not posts:
        return [], None
    if len(posts) == 1:
        return posts, None
    if missing_order:
        return posts, "One or more structured posts are missing index/order metadata."
    if invalid_order:
        return posts, "Structured post indexes must be sequential starting at 1."
    return posts, None


def _json_post_sequence(value: Any) -> list[Any] | None:
    if isinstance(value, list):
        return value
    if not isinstance(value, dict):
        return None
    for key in _POST_KEYS:
        candidate = value.get(key)
        if isinstance(candidate, list):
            return candidate
    return None


def _json_post_text(item: Any) -> str:
    if isinstance(item, str):
        return item.strip()
    if isinstance(item, dict):
        for key in _TEXT_KEYS:
            if key in item:
                return str(item.get(key) or "").strip()
    return str(item or "").strip()


def _json_post_index(item: Any) -> int | None:
    if not isinstance(item, dict):
        return None
    for key in _INDEX_KEYS:
        value = item.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return None


def _has_reply_metadata(item: dict[str, Any]) -> bool:
    for key in ("in_reply_to_tweet_id", "reply_to_tweet_id", "previous_tweet_id"):
        if item.get(key):
            return True
    reply_to = item.get("reply_to")
    if isinstance(reply_to, str):
        return bool(reply_to.strip())
    if isinstance(reply_to, dict):
        return any(bool(reply_to.get(key)) for key in ("id", "tweet_id", "parent"))
    parent = item.get("parent")
    return isinstance(parent, dict) and any(parent.values())


def _opening_key(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text.strip().lower())
    normalized = re.sub(r"^\(?\d+\s*(?:/|of)\s*\d*\)?\s*[:.-]?\s*", "", normalized)
    words = re.findall(r"[a-z0-9']+", normalized)
    words = [word for word in words if word not in _STOPWORDS]
    return " ".join(words[:3])


def _lexical_tokens(text: str) -> set[str]:
    words = re.findall(r"[a-z0-9']+", text.lower())
    return {
        word
        for word in words
        if len(word) > 2 and word not in _STOPWORDS and not word.isdigit()
    }


def _load_thread_drafts(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    include_published: bool,
) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    filters = ["content_type = 'x_thread'"]
    if not include_published and "published" in columns:
        filters.append("COALESCE(published, 0) = 0")
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT id, content
                FROM generated_content
                WHERE {' AND '.join(filters)}
                ORDER BY created_at DESC, id DESC"""
        ).fetchall()
    ]


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: list[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> XThreadContinuityAuditReport:
    return XThreadContinuityAuditReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "threads_scanned": 0,
            "finding_count": 0,
            "by_finding_type": {finding_type: 0 for finding_type in FINDING_TYPES},
        },
        findings=(),
        missing_tables=tuple(missing_tables),
        missing_columns=missing_columns,
    )


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[list[str], dict[str, tuple[str, ...]]]:
    if "generated_content" not in schema:
        return ["generated_content"], {}
    required = {"id", "content_type", "content"}
    missing = tuple(sorted(required - schema["generated_content"]))
    return [], {"generated_content": missing} if missing else {}


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    return {
        row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _decode_json(value: str) -> Any | None:
    stripped = value.strip()
    if not stripped or stripped[0] not in "[{":
        return None
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return None
