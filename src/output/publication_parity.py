"""Read-only audit for cross-platform publication parity."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


ACTIVE_QUEUE_STATUSES = {"queued", "held"}
PUBLISHED_STATUS = "published"
DEFAULT_PLATFORMS = ("x", "bluesky")


@dataclass(frozen=True)
class PlatformState:
    """Effective publication state for one content/platform pair."""

    platform: str
    published: bool = False
    queued: bool = False
    has_variant: bool = False
    publication_status: str | None = None
    queue_status: str | None = None
    queue_platform: str | None = None
    variant_types: tuple[str, ...] = ()

    @property
    def has_durable_state(self) -> bool:
        return self.published or self.queued


@dataclass(frozen=True)
class PublicationParityIssue:
    """A content item with asymmetric cross-platform publication state."""

    content_id: int
    content_type: str
    content: str
    created_at: str
    present_platforms: tuple[str, ...]
    missing_platforms: tuple[str, ...]
    intended_platforms: tuple[str, ...]
    variant_platforms: tuple[str, ...]
    reasons: tuple[str, ...]
    platform_states: dict[str, PlatformState]


def _cutoff(days: int, now: datetime | None) -> str:
    if days <= 0:
        raise ValueError("days must be positive")
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return (now - timedelta(days=days)).isoformat()


def _normalize_platforms(platforms: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    selected = tuple(platforms or DEFAULT_PLATFORMS)
    if len(selected) < 2:
        raise ValueError("at least two platforms are required for parity")
    invalid = [platform for platform in selected if platform not in DEFAULT_PLATFORMS]
    if invalid:
        raise ValueError(f"unsupported platforms: {', '.join(invalid)}")
    return selected


def _rows_by_platform(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row["platform"], []).append(row)
    return grouped


def _publication_rows(conn, content_id: int, platforms: tuple[str, ...]) -> dict[str, list[dict]]:
    placeholders = ",".join("?" for _ in platforms)
    rows = conn.execute(
        f"""SELECT *
            FROM content_publications
            WHERE content_id = ?
              AND platform IN ({placeholders})
            ORDER BY updated_at DESC, id DESC""",
        (content_id, *platforms),
    ).fetchall()
    return _rows_by_platform([dict(row) for row in rows])


def _queue_rows(conn, content_id: int, platforms: tuple[str, ...]) -> dict[str, list[dict]]:
    selectors = []
    params: list[Any] = []
    for platform in platforms:
        selectors.append(
            """SELECT ?, id, content_id, platform AS queue_platform, status,
                      scheduled_at, published_at, error, hold_reason, created_at
               FROM publish_queue
               WHERE content_id = ?
                 AND platform IN (?, 'all')"""
        )
        params.extend([platform, content_id, platform])
    rows = conn.execute(
        " UNION ALL ".join(selectors) + " ORDER BY scheduled_at DESC, id DESC",
        params,
    ).fetchall()
    normalized = []
    for row in rows:
        normalized.append(
            {
                "platform": row[0],
                "id": row[1],
                "content_id": row[2],
                "queue_platform": row[3],
                "status": row[4],
                "scheduled_at": row[5],
                "published_at": row[6],
                "error": row[7],
                "hold_reason": row[8],
                "created_at": row[9],
            }
        )
    return _rows_by_platform(normalized)


def _variant_rows(conn, content_id: int, platforms: tuple[str, ...]) -> dict[str, list[dict]]:
    placeholders = ",".join("?" for _ in platforms)
    rows = conn.execute(
        f"""SELECT *
            FROM content_variants
            WHERE content_id = ?
              AND platform IN ({placeholders})
            ORDER BY created_at DESC, id DESC""",
        (content_id, *platforms),
    ).fetchall()
    return _rows_by_platform([dict(row) for row in rows])


def _legacy_published(content: dict[str, Any], platform: str) -> bool:
    if platform == "x":
        return bool(content.get("tweet_id")) or bool(content.get("published") == 1)
    if platform == "bluesky":
        return bool(content.get("bluesky_uri"))
    return False


def _state_for_platform(
    content: dict[str, Any],
    platform: str,
    publications: dict[str, list[dict]],
    queues: dict[str, list[dict]],
    variants: dict[str, list[dict]],
    include_queued: bool,
) -> PlatformState:
    publication = next(iter(publications.get(platform, [])), None)
    queue = next(iter(queues.get(platform, [])), None)
    variant_rows = variants.get(platform, [])
    publication_status = publication["status"] if publication else None
    queue_status = queue["status"] if queue else None
    published = publication_status == PUBLISHED_STATUS or _legacy_published(content, platform)
    queued = include_queued and (
        publication_status == "queued" or queue_status in ACTIVE_QUEUE_STATUSES
    )
    return PlatformState(
        platform=platform,
        published=published,
        queued=queued,
        has_variant=bool(variant_rows),
        publication_status=publication_status,
        queue_status=queue_status,
        queue_platform=queue["queue_platform"] if queue else None,
        variant_types=tuple(row["variant_type"] for row in variant_rows),
    )


def find_publication_parity_gaps(
    conn,
    days: int = 30,
    platforms: tuple[str, ...] | list[str] | None = None,
    include_queued: bool = False,
    now: datetime | None = None,
) -> list[PublicationParityIssue]:
    """Return recent content with asymmetric publication/queue/variant state.

    This function is intentionally read-only. It only executes SELECT queries.
    """
    selected_platforms = _normalize_platforms(platforms)
    cutoff = _cutoff(days, now)
    content_rows = conn.execute(
        """SELECT *
           FROM generated_content
           WHERE created_at >= ?
           ORDER BY created_at DESC, id DESC""",
        (cutoff,),
    ).fetchall()

    issues: list[PublicationParityIssue] = []
    for content_row in content_rows:
        content = dict(content_row)
        content_id = content["id"]
        publications = _publication_rows(conn, content_id, selected_platforms)
        queues = _queue_rows(conn, content_id, selected_platforms)
        variants = _variant_rows(conn, content_id, selected_platforms)
        states = {
            platform: _state_for_platform(
                content,
                platform,
                publications,
                queues,
                variants,
                include_queued,
            )
            for platform in selected_platforms
        }

        present = tuple(platform for platform, state in states.items() if state.has_durable_state)
        variant_platforms = tuple(platform for platform, state in states.items() if state.has_variant)
        if present:
            intended = selected_platforms
        elif len(variant_platforms) >= 2:
            intended = selected_platforms
        else:
            continue

        missing = tuple(
            platform
            for platform in intended
            if not states[platform].has_durable_state
        )
        if not missing:
            continue

        reasons = []
        if present and missing:
            reasons.append("missing_counterpart")
        if any(states[platform].has_variant and not states[platform].has_durable_state for platform in missing):
            reasons.append("variant_without_state")

        issues.append(
            PublicationParityIssue(
                content_id=content_id,
                content_type=content["content_type"],
                content=content["content"],
                created_at=content["created_at"],
                present_platforms=present,
                missing_platforms=missing,
                intended_platforms=intended,
                variant_platforms=variant_platforms,
                reasons=tuple(reasons),
                platform_states=states,
            )
        )
    return issues


def issues_for_json(issues: list[PublicationParityIssue]) -> list[dict[str, Any]]:
    """Normalize parity issues for JSON output."""
    return [
        {
            "content_id": issue.content_id,
            "content_type": issue.content_type,
            "content": issue.content,
            "created_at": issue.created_at,
            "present_platforms": list(issue.present_platforms),
            "missing_platforms": list(issue.missing_platforms),
            "intended_platforms": list(issue.intended_platforms),
            "variant_platforms": list(issue.variant_platforms),
            "reasons": list(issue.reasons),
            "platform_states": {
                platform: {
                    "published": state.published,
                    "queued": state.queued,
                    "has_variant": state.has_variant,
                    "publication_status": state.publication_status,
                    "queue_status": state.queue_status,
                    "queue_platform": state.queue_platform,
                    "variant_types": list(state.variant_types),
                }
                for platform, state in issue.platform_states.items()
            },
        }
        for issue in issues
    ]


def format_json_report(issues: list[PublicationParityIssue]) -> str:
    """Format parity issues as machine-readable JSON."""
    return json.dumps(issues_for_json(issues), indent=2)


def _shorten(value: Any, width: int) -> str:
    if value is None:
        return "-"
    text = str(value).replace("\n", " ")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def format_text_report(issues: list[PublicationParityIssue]) -> str:
    """Format parity issues as a compact operator-friendly table."""
    if not issues:
        return "No publication parity gaps found."

    columns = [
        ("content_id", "CID", 5),
        ("content_type", "TYPE", 10),
        ("present", "PRESENT", 16),
        ("missing", "MISSING", 16),
        ("variants", "VARIANTS", 16),
        ("reasons", "REASONS", 28),
        ("content", "CONTENT", 42),
    ]
    rows = []
    for issue in issues:
        rows.append(
            {
                "content_id": issue.content_id,
                "content_type": issue.content_type,
                "present": ",".join(issue.present_platforms) or "-",
                "missing": ",".join(issue.missing_platforms) or "-",
                "variants": ",".join(issue.variant_platforms) or "-",
                "reasons": ",".join(issue.reasons) or "-",
                "content": issue.content,
            }
        )

    lines = [
        "  ".join(label.ljust(width) for _, label, width in columns),
        "  ".join("-" * width for _, _, width in columns),
    ]
    for row in rows:
        lines.append(
            "  ".join(
                _shorten(row[key], width).ljust(width)
                for key, _, width in columns
            )
        )
    return "\n".join(lines)
