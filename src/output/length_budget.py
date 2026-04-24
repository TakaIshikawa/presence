"""Deterministic copy length budget reports for publishing targets."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .platform_adapter import (
    BLUESKY_GRAPHEME_LIMIT,
    LINKEDIN_GRAPHEME_LIMIT,
    count_graphemes,
)

try:
    from .x_client import parse_thread_content
except ModuleNotFoundError as exc:  # pragma: no cover - exercised by script envs
    if exc.name != "tweepy":
        raise

    def parse_thread_content(content: str) -> list[str]:
        return [content] if content else []


X_CHARACTER_LIMIT = 280
NEWSLETTER_SUBJECT_LIMIT = 90
BLOG_TITLE_LIMIT = 70
RECOMMENDED_TARGET_RATIO = 0.9


@dataclass(frozen=True)
class PlatformBudget:
    """One platform's hard limit and recommended target."""

    platform: str
    limit: int
    label: str

    @property
    def recommended_target(self) -> int:
        return int(self.limit * RECOMMENDED_TARGET_RATIO)


PLATFORM_BUDGETS: dict[str, PlatformBudget] = {
    "x": PlatformBudget("x", X_CHARACTER_LIMIT, "X post"),
    "bluesky": PlatformBudget("bluesky", BLUESKY_GRAPHEME_LIMIT, "Bluesky post"),
    "linkedin": PlatformBudget("linkedin", LINKEDIN_GRAPHEME_LIMIT, "LinkedIn post"),
    "newsletter": PlatformBudget(
        "newsletter",
        NEWSLETTER_SUBJECT_LIMIT,
        "Newsletter subject",
    ),
    "blog": PlatformBudget("blog", BLOG_TITLE_LIMIT, "Blog title"),
}

PLATFORM_ALIASES = {
    "twitter": "x",
    "bsky": "bluesky",
    "newsletter_subject": "newsletter",
    "subject": "newsletter",
    "blog_title": "blog",
    "title": "blog",
}


class LengthBudgetRecordNotFound(LookupError):
    """Raised when requested generated content or queue row does not exist."""


def normalize_platform(platform: str) -> str:
    """Return the canonical budget platform name."""
    normalized = platform.strip().lower().replace("-", "_")
    return PLATFORM_ALIASES.get(normalized, normalized)


def requested_platforms(platform: str | None = None) -> list[str]:
    """Resolve a CLI/API platform filter into canonical platform names."""
    if platform is None or platform == "all":
        return list(PLATFORM_BUDGETS)
    normalized = normalize_platform(platform)
    if normalized not in PLATFORM_BUDGETS:
        raise ValueError(f"Unsupported platform: {platform}")
    return [normalized]


def evaluate_copy_budget(
    text: str,
    platform: str,
    *,
    content_type: str = "x_post",
    variant_type: str | None = None,
    source: str = "generated",
    variant_id: int | None = None,
) -> dict[str, Any]:
    """Evaluate copy against one platform's length budget."""
    platform = normalize_platform(platform)
    if platform not in PLATFORM_BUDGETS:
        raise ValueError(f"Unsupported platform: {platform}")

    budget = PLATFORM_BUDGETS[platform]
    segments = _segments_for_budget(text, content_type, variant_type)
    segment_reports = [
        _segment_report(segment, index, len(segments), budget)
        for index, segment in enumerate(segments, start=1)
    ]
    segment_count = max(len(segment_reports), 1)
    count = sum(segment["count"] for segment in segment_reports)
    limit = budget.limit * segment_count
    remaining = limit - count
    overflow = max(0, count - limit)
    segment_overflow = any(segment["overflow"] > 0 for segment in segment_reports)
    thread_segment_risk = any(
        segment["overflow"] > 0 or segment["count"] > segment["recommended_target"]
        for segment in segment_reports
    )

    return {
        "platform": platform,
        "label": budget.label,
        "source": source,
        "variant_id": variant_id,
        "variant_type": variant_type,
        "status": _status(overflow, thread_segment_risk, segment_overflow),
        "count": count,
        "limit": limit,
        "remaining": remaining,
        "overflow": overflow,
        "recommended_target": budget.recommended_target * segment_count,
        "thread_segment_risk": thread_segment_risk,
        "segments": segment_reports,
    }


def build_length_budget_report(
    db: Any,
    *,
    content_id: int | None = None,
    queue_id: int | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    """Build a copy length budget report from generated content or queue row."""
    if (content_id is None) == (queue_id is None):
        raise ValueError("Pass exactly one of content_id or queue_id")

    queue = None
    if queue_id is not None:
        content, queue = _fetch_queue_content(db, queue_id)
    else:
        content = _fetch_generated_content(db, content_id)
        queue = _fetch_latest_queue(db, content_id)

    platforms = requested_platforms(platform)
    variants = _fetch_content_variants(db, int(content["id"]))
    platforms_report: dict[str, dict[str, Any]] = {}

    for platform_name in platforms:
        generated = evaluate_copy_budget(
            content.get("content") or "",
            platform_name,
            content_type=content.get("content_type") or "x_post",
            source="generated",
        )
        platform_variants = [
            evaluate_copy_budget(
                variant.get("content") or "",
                platform_name,
                content_type=content.get("content_type") or "x_post",
                variant_type=variant.get("variant_type"),
                source="variant",
                variant_id=variant.get("id"),
            )
            | {
                "stored_platform": normalize_platform(variant.get("platform") or ""),
                "metadata": variant.get("metadata") or {},
            }
            for variant in variants
            if normalize_platform(variant.get("platform") or "") == platform_name
        ]
        platforms_report[platform_name] = generated | {"variants": platform_variants}

    return {
        "content": {
            "id": content["id"],
            "content_type": content.get("content_type"),
        },
        "queue": queue,
        "platforms": platforms_report,
    }


def budget_report_to_json(report: dict[str, Any]) -> str:
    """Serialize a length budget report as stable, readable JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_length_budget_report(report: dict[str, Any]) -> str:
    """Render a length budget report for terminal review."""
    content = report["content"]
    lines = [
        f"Content {content['id']} ({content['content_type']})",
    ]
    queue = report.get("queue")
    if queue:
        lines.append(
            "Queue {queue_id}: {queue_status} for {queue_platform} at {scheduled_at}".format(
                **queue
            )
        )

    for platform, budget in report["platforms"].items():
        lines.append("")
        lines.append(_format_budget_line(platform.upper(), budget))
        if budget["segments"]:
            lines.append("Segments:")
            for segment in budget["segments"]:
                lines.append(
                    "  {index}/{total}: {status} {count}/{limit} "
                    "(remaining {remaining}, overflow {overflow})".format(**segment)
                )
        for variant in budget.get("variants", []):
            variant_name = f"variant #{variant['variant_id']} {variant['variant_type']}"
            lines.append(_format_budget_line(f"  {variant_name}", variant))

    return "\n".join(lines).rstrip()


def _fetch_generated_content(db: Any, content_id: int | None) -> dict[str, Any]:
    row = db.conn.execute(
        "SELECT * FROM generated_content WHERE id = ?",
        (content_id,),
    ).fetchone()
    if not row:
        raise LengthBudgetRecordNotFound(f"generated_content id {content_id} not found")
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
                  pq.published_at AS queue_published_at,
                  pq.error AS queue_error,
                  gc.*
           FROM publish_queue pq
           INNER JOIN generated_content gc ON gc.id = pq.content_id
           WHERE pq.id = ?""",
        (queue_id,),
    ).fetchone()
    if not row:
        raise LengthBudgetRecordNotFound(f"publish_queue id {queue_id} not found")
    record = dict(row)
    return record, {
        key: record.get(key)
        for key in (
            "queue_id",
            "content_id",
            "scheduled_at",
            "queue_platform",
            "queue_status",
            "queue_published_at",
            "queue_error",
        )
    }


def _fetch_latest_queue(db: Any, content_id: int | None) -> dict[str, Any] | None:
    row = db.conn.execute(
        """SELECT id AS queue_id,
                  content_id,
                  scheduled_at,
                  platform AS queue_platform,
                  status AS queue_status,
                  published_at AS queue_published_at,
                  error AS queue_error
           FROM publish_queue
           WHERE content_id = ?
           ORDER BY id DESC
           LIMIT 1""",
        (content_id,),
    ).fetchone()
    return dict(row) if row else None


def _fetch_content_variants(db: Any, content_id: int) -> list[dict[str, Any]]:
    lister = getattr(db, "list_content_variants", None)
    if callable(lister):
        return [dict(variant) for variant in lister(content_id)]

    rows = db.conn.execute(
        """SELECT * FROM content_variants
           WHERE content_id = ?
           ORDER BY created_at, id""",
        (content_id,),
    ).fetchall()
    variants = []
    for row in rows:
        variant = dict(row)
        if isinstance(variant.get("metadata"), str):
            variant["metadata"] = json.loads(variant["metadata"] or "{}")
        variants.append(variant)
    return variants


def _segments_for_budget(
    text: str,
    content_type: str,
    variant_type: str | None,
) -> list[str]:
    if content_type == "x_thread" or variant_type == "thread":
        return parse_thread_content(text) or ([text] if text else [])
    return [text] if text else []


def _segment_report(
    text: str,
    index: int,
    total: int,
    budget: PlatformBudget,
) -> dict[str, Any]:
    count = count_graphemes(text)
    remaining = budget.limit - count
    overflow = max(0, count - budget.limit)
    risk = overflow > 0 or count > budget.recommended_target
    return {
        "index": index,
        "total": total,
        "status": _status(overflow, risk),
        "count": count,
        "limit": budget.limit,
        "remaining": remaining,
        "overflow": overflow,
        "recommended_target": budget.recommended_target,
        "risk": risk,
    }


def _status(overflow: int, risk: bool, segment_overflow: bool = False) -> str:
    if overflow > 0 or segment_overflow:
        return "overflow"
    if risk:
        return "risk"
    return "ok"


def _format_budget_line(prefix: str, budget: dict[str, Any]) -> str:
    return (
        f"{prefix}: {budget['status']} {budget['count']}/{budget['limit']} "
        f"(remaining {budget['remaining']}, overflow {budget['overflow']}, "
        f"target {budget['recommended_target']})"
    )
