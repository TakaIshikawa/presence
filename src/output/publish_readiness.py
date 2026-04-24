"""Read-only preflight checks for queued publication readiness."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from synthesis.alt_text_guard import validate_alt_text

from .attribution_guard import check_publication_attribution_guard
from .license_guard import (
    STRICT_RESTRICTED_BEHAVIOR,
    check_publication_license_guard,
)
from .platform_adapter import count_graphemes
from .x_client import parse_thread_content


READY = "ready"
BLOCKED = "blocked"
WARNING = "warning"
SUPPORTED_PLATFORMS = {"x", "bluesky", "all"}
MAX_X_GRAPHEMES = 280


@dataclass(frozen=True)
class ReadinessReason:
    """One operator-facing readiness reason."""

    code: str
    message: str
    severity: str = "block"

    def as_dict(self) -> dict:
        return {
            "code": self.code,
            "message": self.message,
            "severity": self.severity,
        }


@dataclass(frozen=True)
class PublishReadinessResult:
    """Readiness status for one publish queue row."""

    queue_id: int
    content_id: int
    platform: str
    status: str
    reasons: list[ReadinessReason] = field(default_factory=list)
    scheduled_at: str | None = None
    queue_status: str | None = None
    due: bool | None = None

    @property
    def blocked(self) -> bool:
        return self.status == BLOCKED

    def as_dict(self) -> dict:
        return {
            "queue_id": self.queue_id,
            "content_id": self.content_id,
            "platform": self.platform,
            "status": self.status,
            "reasons": [reason.as_dict() for reason in self.reasons],
            "scheduled_at": self.scheduled_at,
            "queue_status": self.queue_status,
            "due": self.due,
        }


def _requested_platforms(platform: str | None) -> list[str]:
    if platform == "all" or platform is None:
        return ["x", "bluesky"]
    if platform in {"x", "bluesky"}:
        return [platform]
    return []


def _variant_type_for_content_type(content_type: str | None) -> str:
    if content_type == "x_thread":
        return "thread"
    return "post"


def _copy_parts(content: str, content_type: str | None) -> list[str]:
    if content_type == "x_thread":
        return parse_thread_content(content)
    return [content] if content else []


def _content_copy_for_platform(
    db: Any,
    content_id: int,
    platform: str,
    variant_type: str,
    fallback_content: str,
) -> str:
    getter = getattr(db, "get_content_variant_or_original", None)
    if not callable(getter):
        return fallback_content
    copy = getter(content_id, platform, variant_type)
    return (copy or {}).get("content") or fallback_content


def _is_visual_post(item: dict) -> bool:
    return bool(item.get("image_path")) or item.get("content_type") in {
        "x_visual",
        "visual",
    }


def _has_bluesky_credentials(config: object | None) -> bool:
    bluesky = getattr(config, "bluesky", None)
    return bool(
        bluesky
        and getattr(bluesky, "enabled", False)
        and getattr(bluesky, "handle", None)
        and getattr(bluesky, "app_password", None)
    )


def _restricted_prompt_behavior(config: object | None) -> str:
    curated_sources = getattr(config, "curated_sources", None)
    behavior = getattr(
        curated_sources,
        "restricted_prompt_behavior",
        STRICT_RESTRICTED_BEHAVIOR,
    )
    if behavior in {"strict", "permissive"}:
        return behavior
    return STRICT_RESTRICTED_BEHAVIOR


def _fetch_queue_items(
    db: Any,
    *,
    platform: str | None = None,
    queue_id: int | None = None,
) -> list[dict]:
    filters = ["pq.status IN ('queued', 'failed')"]
    params: list[object] = []

    if platform is not None:
        filters.append("pq.platform = ?")
        params.append(platform)
    if queue_id is not None:
        filters.append("pq.id = ?")
        params.append(queue_id)

    where_clause = " AND ".join(filters)
    rows = db.conn.execute(
        f"""SELECT pq.id AS queue_id,
                  pq.content_id,
                  pq.scheduled_at,
                  pq.platform,
                  pq.status AS queue_status,
                  pq.error AS queue_error,
                  pq.error_category,
                  pq.hold_reason,
                  gc.content,
                  gc.content_type,
                  gc.image_path,
                  gc.image_prompt,
                  gc.image_alt_text
           FROM publish_queue pq
           INNER JOIN generated_content gc ON gc.id = pq.content_id
           WHERE {where_clause}
           ORDER BY pq.scheduled_at ASC, pq.id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _is_due(scheduled_at: str | None, now_iso: str | None) -> bool | None:
    if now_iso is None or scheduled_at is None:
        return None
    return scheduled_at <= now_iso


def _license_reasons(
    db: Any,
    item: dict,
    *,
    restricted_prompt_behavior: str,
    allow_restricted_knowledge: bool,
) -> list[ReadinessReason]:
    guard = check_publication_license_guard(
        db,
        item["content_id"],
        restricted_prompt_behavior=restricted_prompt_behavior,
        allow_restricted=allow_restricted_knowledge,
    )
    if not guard.restricted_sources:
        return []

    sources = ", ".join(
        "knowledge {knowledge_id}: {license} {source_url}".format(
            knowledge_id=source.knowledge_id,
            license=source.license,
            source_url=source.source_url or "no source URL",
        )
        for source in guard.restricted_sources
    )
    return [
        ReadinessReason(
            code="license_guard_blocked" if guard.blocked else "license_guard_warning",
            message=(
                "Restricted knowledge source blocks publication: "
                if guard.blocked
                else "Restricted knowledge source requires operator review: "
            )
            + sources,
            severity="block" if guard.blocked else "warning",
        )
    ]


def _attribution_reasons(
    db: Any,
    item: dict,
    requested_platforms: list[str],
) -> list[ReadinessReason]:
    content = item.get("content") or ""
    variant_type = _variant_type_for_content_type(item.get("content_type"))
    reasons: list[ReadinessReason] = []

    for platform in requested_platforms:
        platform_content = _content_copy_for_platform(
            db,
            item["content_id"],
            platform,
            variant_type,
            content,
        )
        guard = check_publication_attribution_guard(
            db,
            item["content_id"],
            platform_content,
        )
        if guard.blocked:
            sources = ", ".join(
                "knowledge {knowledge_id}: {license} {author} {source_url}".format(
                    knowledge_id=source.knowledge_id,
                    license=source.license,
                    author=source.author or "unknown author",
                    source_url=source.source_url or "no source URL",
                )
                for source in guard.missing_sources
            )
            reasons.append(
                ReadinessReason(
                    code="attribution_guard_blocked",
                    message=(
                        f"{platform} copy is missing visible attribution: {sources}"
                    ),
                )
            )
    return reasons


def _x_length_reasons(item: dict) -> list[ReadinessReason]:
    content = item.get("content") or ""
    parts = _copy_parts(content, item.get("content_type"))
    if not parts:
        return []

    reasons: list[ReadinessReason] = []
    for index, text in enumerate(parts, start=1):
        graphemes = count_graphemes(text)
        if graphemes > MAX_X_GRAPHEMES:
            reasons.append(
                ReadinessReason(
                    code="x_post_over_limit",
                    message=(
                        f"X post {index} is {graphemes} graphemes; "
                        f"limit is {MAX_X_GRAPHEMES}."
                    ),
                )
            )
    return reasons


def _visual_reasons(item: dict) -> list[ReadinessReason]:
    if not _is_visual_post(item):
        return []

    reasons: list[ReadinessReason] = []
    image_path = (item.get("image_path") or "").strip()
    if not image_path:
        reasons.append(
            ReadinessReason(
                code="missing_image_path",
                message="Visual posts require an image path before publishing.",
            )
        )
    elif not Path(image_path).exists():
        reasons.append(
            ReadinessReason(
                code="missing_image_file",
                message=f"Visual post image file does not exist: {image_path}",
            )
        )

    alt_text = validate_alt_text(
        item.get("image_alt_text"),
        image_prompt=item.get("image_prompt"),
        image_path=item.get("image_path"),
        content_type=item.get("content_type"),
    )
    if not alt_text.passed:
        for issue in alt_text.issues:
            reasons.append(
                ReadinessReason(
                    code=issue.code,
                    message=issue.message,
                    severity="block",
                )
            )
    return reasons


def check_queue_item_readiness(
    db: Any,
    item: dict,
    *,
    config: object | None = None,
    now_iso: str | None = None,
    allow_restricted_knowledge: bool = False,
) -> PublishReadinessResult:
    """Return read-only readiness for one queued publish item."""
    reasons: list[ReadinessReason] = []
    content = (item.get("content") or "").strip()
    requested_platforms = _requested_platforms(item.get("platform"))

    if not content:
        reasons.append(
            ReadinessReason(
                code="empty_content",
                message="Generated content is empty.",
            )
        )

    if not requested_platforms:
        reasons.append(
            ReadinessReason(
                code="unsupported_platform",
                message=f"Unsupported queue platform: {item.get('platform')}",
            )
        )

    if item.get("platform") == "bluesky" and not _has_bluesky_credentials(config):
        reasons.append(
            ReadinessReason(
                code="missing_bluesky_credentials",
                message=(
                    "Bluesky-only queue row cannot publish without enabled "
                    "Bluesky handle and app password."
                ),
            )
        )

    if "x" in requested_platforms:
        reasons.extend(_x_length_reasons(item))

    reasons.extend(_visual_reasons(item))
    reasons.extend(
        _license_reasons(
            db,
            item,
            restricted_prompt_behavior=_restricted_prompt_behavior(config),
            allow_restricted_knowledge=allow_restricted_knowledge,
        )
    )
    reasons.extend(_attribution_reasons(db, item, requested_platforms))

    if any(reason.severity == "block" for reason in reasons):
        status = BLOCKED
    elif any(reason.severity == "warning" for reason in reasons):
        status = WARNING
    else:
        status = READY

    return PublishReadinessResult(
        queue_id=item["queue_id"],
        content_id=item["content_id"],
        platform=item["platform"],
        status=status,
        reasons=reasons,
        scheduled_at=item.get("scheduled_at"),
        queue_status=item.get("queue_status"),
        due=_is_due(item.get("scheduled_at"), now_iso),
    )


def check_publish_readiness(
    db: Any,
    *,
    config: object | None = None,
    platform: str | None = None,
    queue_id: int | None = None,
    now_iso: str | None = None,
    allow_restricted_knowledge: bool = False,
) -> list[PublishReadinessResult]:
    """Return deterministic readiness results for queued publish rows."""
    if platform is not None and platform not in SUPPORTED_PLATFORMS:
        raise ValueError("platform must be one of: x, bluesky, all")

    items = _fetch_queue_items(db, platform=platform, queue_id=queue_id)
    return [
        check_queue_item_readiness(
            db,
            item,
            config=config,
            now_iso=now_iso,
            allow_restricted_knowledge=allow_restricted_knowledge,
        )
        for item in items
    ]
