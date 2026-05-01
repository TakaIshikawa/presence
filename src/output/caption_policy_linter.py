"""Platform-aware linting for generated captions and selected variants."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

SUPPORTED_PLATFORMS = ("x", "bluesky")
PLATFORM_ALIASES = {"twitter": "x", "bsky": "bluesky"}

RULE_TOO_MANY_HASHTAGS = "CAP_HASHTAG_LIMIT"
RULE_TOO_MANY_LINKS = "CAP_LINK_LIMIT"
RULE_DUPLICATE_MENTION = "CAP_DUPLICATE_MENTION"
RULE_EMPTY_THREAD_PART = "CAP_EMPTY_THREAD_PART"
RULE_CTA_DENSITY = "CAP_CTA_DENSITY"
RULE_MISSING_SELECTED_VARIANT = "CAP_MISSING_SELECTED_VARIANT"

STRICT_PROMOTABLE_RULES = {
    RULE_TOO_MANY_HASHTAGS,
    RULE_CTA_DENSITY,
    RULE_MISSING_SELECTED_VARIANT,
}

HASHTAG_LIMITS = {"x": 4, "bluesky": 2}
LINK_LIMITS = {"x": 1, "bluesky": 1}
BLOCKING_RULES = {
    RULE_TOO_MANY_LINKS,
    RULE_DUPLICATE_MENTION,
    RULE_EMPTY_THREAD_PART,
}

HASHTAG_RE = re.compile(r"(?<!\w)#[A-Za-z][A-Za-z0-9_]*")
URL_RE = re.compile(r"https?://[^\s<>()]+")
MENTION_RE = re.compile(r"(?<![\w@])@([A-Za-z0-9_][A-Za-z0-9_.-]{0,63})")
THREAD_MARKER_RE = re.compile(r"^\s*TWEET\s+(\d+)\s*:\s*(.*)$", re.IGNORECASE)
CTA_RE = re.compile(
    r"\b("
    r"subscribe|follow|share|repost|retweet|like|comment|reply|"
    r"click|tap|read more|sign up|join|buy|download|check out|"
    r"learn more|try it|dm me"
    r")\b",
    re.IGNORECASE,
)


class CaptionPolicyRecordNotFound(LookupError):
    """Raised when the requested generated content or queue row is missing."""


@dataclass(frozen=True)
class CaptionSubject:
    content_id: int
    platform: str
    source: str
    text: str
    content_type: str | None = None
    variant_id: int | None = None
    variant_type: str | None = None

    def identity(self) -> dict[str, Any]:
        return {
            "content_id": self.content_id,
            "platform": self.platform,
            "source": self.source,
            "variant_id": self.variant_id,
            "variant_type": self.variant_type,
        }


def lint_caption_policy(
    db: Any,
    *,
    content_id: int | None = None,
    queue_id: int | None = None,
    platform: str = "all",
    strict: bool = False,
) -> dict[str, Any]:
    """Lint generated copy and selected platform variants before publication."""
    if (content_id is None) == (queue_id is None):
        raise ValueError("Pass exactly one of content_id or queue_id")

    if queue_id is not None:
        content, queue = _fetch_queue_content(db, queue_id)
    else:
        content = _fetch_generated_content(db, content_id)
        queue = _fetch_latest_queue(db, content_id)

    platforms = _requested_platforms(platform, queue)
    variants = _fetch_content_variants(db, int(content["id"]))
    issues: list[dict[str, Any]] = []
    subjects: list[dict[str, Any]] = []

    for platform_name in platforms:
        generated = CaptionSubject(
            content_id=int(content["id"]),
            platform=platform_name,
            source="generated",
            text=content.get("content") or "",
            content_type=content.get("content_type"),
        )
        subjects.append(generated.identity())
        issues.extend(_lint_subject(generated, strict=strict))

        platform_variants = [
            variant
            for variant in variants
            if normalize_platform(variant.get("platform") or "") == platform_name
        ]
        selected_variants = [
            variant for variant in platform_variants if bool(variant.get("selected"))
        ]
        if platform_variants and not selected_variants:
            issues.append(
                _issue(
                    RULE_MISSING_SELECTED_VARIANT,
                    platform_name,
                    "variant",
                    strict=strict,
                    content_id=int(content["id"]),
                    message=(
                        f"{len(platform_variants)} stored {platform_name} variant(s) "
                        "exist but none is selected."
                    ),
                    variant_id=None,
                    variant_type=None,
                    details={
                        "available_variant_ids": [
                            int(variant["id"]) for variant in platform_variants
                        ],
                        "available_variant_types": [
                            variant.get("variant_type") for variant in platform_variants
                        ],
                    },
                )
            )
        for variant in selected_variants:
            subject = CaptionSubject(
                content_id=int(content["id"]),
                platform=platform_name,
                source="variant",
                text=variant.get("content") or "",
                content_type=content.get("content_type"),
                variant_id=variant.get("id"),
                variant_type=variant.get("variant_type"),
            )
            subjects.append(subject.identity())
            issues.extend(_lint_subject(subject, strict=strict))

    issues = sorted(
        issues,
        key=lambda item: (
            item["platform"],
            item["source"],
            item.get("variant_id") is None,
            item.get("variant_id") or 0,
            item["code"],
            item.get("segment_index") or 0,
            item["message"],
        ),
    )
    blocking = sum(1 for issue in issues if issue["severity"] == "error")
    warnings = sum(1 for issue in issues if issue["severity"] == "warning")

    return {
        "artifact_type": "caption_policy_lint",
        "status": "blocked" if blocking else "ok",
        "strict": bool(strict),
        "filters": {
            "content_id": content_id,
            "queue_id": queue_id,
            "platform": platform,
        },
        "content": {
            "id": int(content["id"]),
            "content_type": content.get("content_type"),
        },
        "queue": queue,
        "platforms": platforms,
        "counts": {
            "subjects": len(subjects),
            "issues": len(issues),
            "warnings": warnings,
            "blocking_errors": blocking,
        },
        "subjects": sorted(
            subjects,
            key=lambda item: (
                item["platform"],
                item["source"],
                item.get("variant_id") or 0,
            ),
        ),
        "issues": issues,
    }


def format_json_report(report: dict[str, Any]) -> str:
    """Serialize a caption policy report as stable JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_text_report(report: dict[str, Any]) -> str:
    """Render a deterministic terminal report."""
    content = report["content"]
    counts = report["counts"]
    lines = [
        "Caption Policy Lint",
        f"Content: {content['id']} ({content.get('content_type')})",
        "Filters: queue_id={queue_id} platform={platform} strict={strict}".format(
            queue_id=report["filters"].get("queue_id") or "-",
            platform=report["filters"].get("platform") or "all",
            strict="yes" if report.get("strict") else "no",
        ),
        "Counts: subjects={subjects} issues={issues} warnings={warnings} blocking_errors={blocking_errors}".format(
            **counts
        ),
    ]
    queue = report.get("queue")
    if queue:
        lines.append(
            "Queue: {queue_id} {queue_platform} {queue_status} {scheduled_at}".format(
                **queue
            )
        )
    lines.extend(["", "Issues"])
    if not report["issues"]:
        lines.append("  none")
        return "\n".join(lines)

    for issue in report["issues"]:
        target = (
            f"{issue['platform']} {issue['source']}"
            + (
                f" variant #{issue['variant_id']} {issue.get('variant_type')}"
                if issue.get("variant_id")
                else ""
            )
        )
        segment = (
            f" segment={issue['segment_index']}/{issue['segment_total']}"
            if issue.get("segment_index")
            else ""
        )
        lines.append(
            "  - {severity} {code} [{target}{segment}]: {message}".format(
                severity=issue["severity"],
                code=issue["code"],
                target=target,
                segment=segment,
                message=issue["message"],
            )
        )
    return "\n".join(lines)


def normalize_platform(platform: str) -> str:
    normalized = platform.strip().lower().replace("-", "_")
    return PLATFORM_ALIASES.get(normalized, normalized)


def _requested_platforms(platform: str, queue: dict[str, Any] | None) -> list[str]:
    normalized = normalize_platform(platform or "all")
    if normalized != "all":
        if normalized not in SUPPORTED_PLATFORMS:
            raise ValueError(f"Unsupported platform: {platform}")
        return [normalized]

    queue_platform = normalize_platform((queue or {}).get("queue_platform") or "all")
    if queue_platform == "all":
        return list(SUPPORTED_PLATFORMS)
    if queue_platform in SUPPORTED_PLATFORMS:
        return [queue_platform]
    return list(SUPPORTED_PLATFORMS)


def _lint_subject(subject: CaptionSubject, *, strict: bool) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    segments = _thread_segments(subject.text, subject.content_type, subject.variant_type)
    if not segments:
        segments = [{"index": 1, "total": 1, "text": subject.text or ""}]

    for segment in segments:
        text = segment["text"]
        common = {
            "content_id": subject.content_id,
            "variant_id": subject.variant_id,
            "variant_type": subject.variant_type,
            "segment_index": segment["index"],
            "segment_total": segment["total"],
        }
        if not text.strip():
            issues.append(
                _issue(
                    RULE_EMPTY_THREAD_PART,
                    subject.platform,
                    subject.source,
                    strict=strict,
                    message="Thread part is empty.",
                    details={},
                    **common,
                )
            )

        hashtag_count = len(HASHTAG_RE.findall(text))
        hashtag_limit = HASHTAG_LIMITS[subject.platform]
        if hashtag_count > hashtag_limit:
            issues.append(
                _issue(
                    RULE_TOO_MANY_HASHTAGS,
                    subject.platform,
                    subject.source,
                    strict=strict,
                    message=(
                        f"{hashtag_count} hashtags exceeds {subject.platform} "
                        f"limit of {hashtag_limit}."
                    ),
                    details={"count": hashtag_count, "limit": hashtag_limit},
                    **common,
                )
            )

        links = URL_RE.findall(text)
        link_limit = LINK_LIMITS[subject.platform]
        if len(links) > link_limit:
            issues.append(
                _issue(
                    RULE_TOO_MANY_LINKS,
                    subject.platform,
                    subject.source,
                    strict=strict,
                    message=(
                        f"{len(links)} links exceeds {subject.platform} "
                        f"limit of {link_limit}."
                    ),
                    details={"count": len(links), "limit": link_limit},
                    **common,
                )
            )

        duplicates = _duplicate_mentions(text)
        if duplicates:
            issues.append(
                _issue(
                    RULE_DUPLICATE_MENTION,
                    subject.platform,
                    subject.source,
                    strict=strict,
                    message="Duplicate mention(s): " + ", ".join(duplicates),
                    details={"mentions": duplicates},
                    **common,
                )
            )

        cta_count = len(CTA_RE.findall(text))
        if cta_count > 1:
            issues.append(
                _issue(
                    RULE_CTA_DENSITY,
                    subject.platform,
                    subject.source,
                    strict=strict,
                    message=f"{cta_count} call-to-action phrases in one post.",
                    details={"count": cta_count, "limit": 1},
                    **common,
                )
            )

    return issues


def _issue(
    code: str,
    platform: str,
    source: str,
    *,
    strict: bool,
    content_id: int,
    message: str,
    details: dict[str, Any],
    variant_id: int | None = None,
    variant_type: str | None = None,
    segment_index: int | None = None,
    segment_total: int | None = None,
) -> dict[str, Any]:
    severity = "error" if code in BLOCKING_RULES else "warning"
    if strict and code in STRICT_PROMOTABLE_RULES:
        severity = "error"
    return {
        "code": code,
        "severity": severity,
        "platform": platform,
        "source": source,
        "content_id": content_id,
        "variant_id": variant_id,
        "variant_type": variant_type,
        "segment_index": segment_index,
        "segment_total": segment_total,
        "message": message,
        "details": details,
    }


def _duplicate_mentions(text: str) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for match in MENTION_RE.finditer(text):
        handle = "@" + match.group(1).lower().rstrip(".")
        if handle in seen:
            duplicates.add(handle)
        seen.add(handle)
    return sorted(duplicates)


def _thread_segments(
    text: str,
    content_type: str | None,
    variant_type: str | None,
) -> list[dict[str, Any]]:
    if content_type != "x_thread" and variant_type != "thread":
        return [{"index": 1, "total": 1, "text": text or ""}]

    posts: list[str] = []
    current: list[str] | None = None
    saw_marker = False
    for line in (text or "").splitlines():
        marker = THREAD_MARKER_RE.match(line)
        if marker:
            saw_marker = True
            if current is not None:
                posts.append("\n".join(current).strip())
            current = []
            inline = marker.group(2).strip()
            if inline:
                current.append(inline)
            continue
        if current is None:
            if line.strip():
                current = [line]
        else:
            current.append(line)
    if current is not None:
        posts.append("\n".join(current).strip())
    if not saw_marker:
        posts = [text or ""]

    total = max(len(posts), 1)
    return [
        {"index": index, "total": total, "text": post}
        for index, post in enumerate(posts or [""], start=1)
    ]


def _fetch_generated_content(db: Any, content_id: int | None) -> dict[str, Any]:
    getter = getattr(db, "get_generated_content", None)
    if callable(getter):
        row = getter(content_id)
        if row:
            return dict(row)
    row = db.conn.execute(
        "SELECT * FROM generated_content WHERE id = ?",
        (content_id,),
    ).fetchone()
    if not row:
        raise CaptionPolicyRecordNotFound(f"generated_content id {content_id} not found")
    return dict(row)


def _fetch_queue_content(
    db: Any,
    queue_id: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    row = db.conn.execute(
        """SELECT pq.id AS queue_id,
                  pq.content_id,
                  pq.scheduled_at,
                  pq.platform AS queue_platform,
                  pq.status AS queue_status,
                  gc.*
           FROM publish_queue pq
           INNER JOIN generated_content gc ON gc.id = pq.content_id
           WHERE pq.id = ?""",
        (queue_id,),
    ).fetchone()
    if not row:
        raise CaptionPolicyRecordNotFound(f"publish_queue id {queue_id} not found")
    record = dict(row)
    queue = {
        key: record.get(key)
        for key in (
            "queue_id",
            "content_id",
            "scheduled_at",
            "queue_platform",
            "queue_status",
        )
    }
    return record, queue


def _fetch_latest_queue(db: Any, content_id: int | None) -> dict[str, Any] | None:
    if not _table_exists(db, "publish_queue"):
        return None
    row = db.conn.execute(
        """SELECT id AS queue_id,
                  content_id,
                  scheduled_at,
                  platform AS queue_platform,
                  status AS queue_status
           FROM publish_queue
           WHERE content_id = ?
           ORDER BY id DESC
           LIMIT 1""",
        (content_id,),
    ).fetchone()
    return dict(row) if row else None


def _fetch_content_variants(db: Any, content_id: int) -> list[dict[str, Any]]:
    if not _table_exists(db, "content_variants"):
        return []
    lister = getattr(db, "list_content_variants", None)
    if callable(lister):
        return [dict(variant) for variant in lister(content_id)]

    rows = db.conn.execute(
        """SELECT * FROM content_variants
           WHERE content_id = ?
           ORDER BY platform, selected DESC, created_at, id""",
        (content_id,),
    ).fetchall()
    variants = []
    for row in rows:
        variant = dict(row)
        if isinstance(variant.get("metadata"), str):
            try:
                variant["metadata"] = json.loads(variant["metadata"] or "{}")
            except json.JSONDecodeError:
                variant["metadata"] = {}
        variants.append(variant)
    return variants


def _table_exists(db: Any, table: str) -> bool:
    row = db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None
