"""Preview generated content exactly as it would be sent to platforms."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from synthesis.alt_text_guard import validate_alt_text
from synthesis.hashtag_suggester import HashtagSuggestions, suggest_hashtags

from .attribution_guard import check_publication_attribution_guard
from .license_guard import (
    STRICT_RESTRICTED_BEHAVIOR,
    check_publication_license_guard,
)
from .platform_adapter import BlueskyPlatformAdapter, count_graphemes
from .x_client import parse_thread_content


class PreviewRecordNotFound(LookupError):
    """Raised when the requested content or queue row does not exist."""


def _requested_platforms(platform: str | None) -> list[str]:
    if platform == "all" or platform is None:
        return ["x", "bluesky"]
    if platform in {"x", "bluesky"}:
        return [platform]
    return []


def _fetch_queue_record(db: Any, queue_id: int) -> dict | None:
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
    return dict(row) if row else None


def _fetch_content_record(db: Any, content_id: int) -> dict | None:
    row = db.conn.execute(
        "SELECT * FROM generated_content WHERE id = ?",
        (content_id,),
    ).fetchone()
    return dict(row) if row else None


def _fetch_latest_queue_for_content(db: Any, content_id: int) -> dict | None:
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


def _fetch_publication_state(db: Any, content_id: int, platform: str) -> dict | None:
    getter = getattr(db, "get_publication_state", None)
    if callable(getter):
        state = getter(content_id, platform)
        return dict(state) if state else None

    row = db.conn.execute(
        """SELECT * FROM content_publications
           WHERE content_id = ? AND platform = ?""",
        (content_id, platform),
    ).fetchone()
    return dict(row) if row else None


def _fetch_claim_check_summary(db: Any, content_id: int) -> dict | None:
    getter = getattr(db, "get_claim_check_summary", None)
    if callable(getter):
        summary = getter(content_id)
        return dict(summary) if summary else None

    row = db.conn.execute(
        "SELECT * FROM content_claim_checks WHERE content_id = ?",
        (content_id,),
    ).fetchone()
    return dict(row) if row else None


def _fetch_persona_guard_summary(db: Any, content_id: int) -> dict | None:
    getter = getattr(db, "get_persona_guard_summary", None)
    if callable(getter):
        summary = getter(content_id)
        return dict(summary) if summary else None

    row = db.conn.execute(
        "SELECT * FROM content_persona_guard WHERE content_id = ?",
        (content_id,),
    ).fetchone()
    if not row:
        return None
    summary = dict(row)
    summary["checked"] = bool(summary.get("checked"))
    summary["passed"] = bool(summary.get("passed"))
    summary["reasons"] = json.loads(summary.get("reasons") or "[]")
    summary["metrics"] = json.loads(summary.get("metrics") or "{}")
    return summary


def _fetch_content_topics(db: Any, content_id: int) -> list[dict]:
    row_factory = getattr(getattr(db, "conn", None), "row_factory", None)
    try:
        rows = db.conn.execute(
            """SELECT topic, subtopic, confidence
               FROM content_topics
               WHERE content_id = ?
               ORDER BY confidence DESC, id ASC""",
            (content_id,),
        ).fetchall()
    except Exception:
        return []

    if row_factory:
        return [dict(row) for row in rows]
    return [
        {
            "topic": row[0],
            "subtopic": row[1],
            "confidence": row[2],
        }
        for row in rows
    ]


def _claim_check_status(summary: dict | None) -> dict:
    if not summary:
        return {
            "checked": False,
            "status": "not_checked",
            "supported_count": 0,
            "unsupported_count": 0,
            "annotation_text": None,
        }

    unsupported_count = summary.get("unsupported_count") or 0
    return {
        "checked": True,
        "status": "unsupported_claims" if unsupported_count else "supported",
        "supported_count": summary.get("supported_count") or 0,
        "unsupported_count": unsupported_count,
        "annotation_text": summary.get("annotation_text"),
        "created_at": summary.get("created_at"),
        "updated_at": summary.get("updated_at"),
    }


def _persona_guard_status(summary: dict | None) -> dict:
    if not summary:
        return {
            "checked": False,
            "passed": None,
            "status": "not_checked",
            "score": None,
            "reasons": [],
            "metrics": {},
        }

    return {
        "checked": bool(summary.get("checked")),
        "passed": bool(summary.get("passed")),
        "status": summary.get("status") or "unknown",
        "score": summary.get("score"),
        "reasons": summary.get("reasons") or [],
        "metrics": summary.get("metrics") or {},
        "created_at": summary.get("created_at"),
        "updated_at": summary.get("updated_at"),
    }


def _split_x_posts(content: str, content_type: str) -> list[str]:
    if content_type == "x_thread":
        return parse_thread_content(content)
    return [content] if content else []


def _post_counts(text: str) -> dict:
    return {
        "characters": len(text),
        "graphemes": count_graphemes(text),
    }


def _platform_status(
    content: dict,
    platform: str,
    state: dict | None,
    queue: dict | None,
    requested: bool,
) -> dict:
    status = state["status"] if state else None
    platform_post_id = state.get("platform_post_id") if state else None
    platform_url = state.get("platform_url") if state else None
    published_at = state.get("published_at") if state else None
    error = state.get("error") if state else None

    if platform == "x" and content.get("published"):
        status = status or ("published" if content.get("published") == 1 else "abandoned")
        platform_post_id = platform_post_id or content.get("tweet_id")
        platform_url = platform_url or content.get("published_url")
        published_at = published_at or content.get("published_at")
    elif platform == "bluesky" and content.get("bluesky_uri"):
        status = status or "published"
        platform_post_id = platform_post_id or content.get("bluesky_uri")

    if status is None:
        status = queue.get("queue_status") if queue and requested else "generated"

    return {
        "requested": requested,
        "status": status,
        "platform_post_id": platform_post_id,
        "platform_url": platform_url,
        "published_at": published_at,
        "error": error,
        "attempt_count": state.get("attempt_count") if state else None,
        "next_retry_at": state.get("next_retry_at") if state else None,
    }


def _render_platform_posts(
    platform: str,
    x_posts: list[str],
    content_type: str,
    adapter: BlueskyPlatformAdapter,
    suggestions: HashtagSuggestions | None = None,
) -> list[dict]:
    if platform == "bluesky":
        texts = [
            adapter.adapt(
                post,
                content_type,
                suggested_hashtags=(
                    suggestions.bluesky
                    if suggestions and index == len(x_posts) - 1
                    else None
                ),
            )
            for index, post in enumerate(x_posts)
        ]
    else:
        texts = x_posts

    total = len(texts)
    return [
        {
            "index": index,
            "total": total,
            "text": text,
            "counts": _post_counts(text),
        }
        for index, text in enumerate(texts, start=1)
    ]


def build_publication_preview(
    db: Any,
    *,
    content_id: int | None = None,
    queue_id: int | None = None,
    bluesky_adapter: BlueskyPlatformAdapter | None = None,
    include_hashtag_suggestions: bool = False,
    restricted_prompt_behavior: str = STRICT_RESTRICTED_BEHAVIOR,
    allow_restricted_knowledge: bool = False,
) -> dict:
    """Build a platform preview for one generated content or queue row."""
    if (content_id is None) == (queue_id is None):
        raise ValueError("Pass exactly one of content_id or queue_id")

    queue = None
    if queue_id is not None:
        record = _fetch_queue_record(db, queue_id)
        if not record:
            raise PreviewRecordNotFound(f"publish_queue id {queue_id} not found")
        queue = {
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
        content = record
    else:
        content = _fetch_content_record(db, content_id)
        if not content:
            raise PreviewRecordNotFound(f"generated_content id {content_id} not found")
        queue = _fetch_latest_queue_for_content(db, content_id)

    adapter = bluesky_adapter or BlueskyPlatformAdapter()
    requested = set(_requested_platforms(queue.get("queue_platform") if queue else None))
    x_posts = _split_x_posts(content.get("content") or "", content["content_type"])
    hashtag_suggestions = (
        suggest_hashtags(
            content.get("content") or "",
            topics=_fetch_content_topics(db, content["id"]),
        )
        if include_hashtag_suggestions
        else None
    )
    claim_check = _claim_check_status(
        _fetch_claim_check_summary(db, content["id"])
    )
    persona_guard = _persona_guard_status(
        _fetch_persona_guard_summary(db, content["id"])
    )
    alt_text = validate_alt_text(
        content.get("image_alt_text"),
        image_prompt=content.get("image_prompt"),
        image_path=content.get("image_path"),
        content_type=content.get("content_type"),
    ).as_dict()
    license_guard = check_publication_license_guard(
        db,
        content["id"],
        restricted_prompt_behavior=restricted_prompt_behavior,
        allow_restricted=allow_restricted_knowledge,
    ).as_dict()
    attribution_guard = check_publication_attribution_guard(
        db,
        content["id"],
        content.get("content") or "",
    ).as_dict()

    platforms = {}
    for platform in ("x", "bluesky"):
        state = _fetch_publication_state(db, content["id"], platform)
        posts = _render_platform_posts(
            platform,
            x_posts,
            content["content_type"],
            adapter,
            hashtag_suggestions,
        )
        platform_attribution_guard = check_publication_attribution_guard(
            db,
            content["id"],
            [post["text"] for post in posts],
        ).as_dict()
        platforms[platform] = {
            "status": _platform_status(
                content,
                platform,
                state,
                queue,
                platform in requested,
            ),
            "posts": posts,
            "suggested_hashtags": (
                list(hashtag_suggestions.for_platform(platform))
                if hashtag_suggestions
                else []
            ),
            "image_path": content.get("image_path"),
            "image_alt_text": content.get("image_alt_text"),
            "alt_text": alt_text,
            "license_guard": license_guard,
            "attribution_guard": platform_attribution_guard,
        }

    return {
        "content": {
            "id": content["id"],
            "content_type": content["content_type"],
            "image_path": content.get("image_path"),
            "image_prompt": content.get("image_prompt"),
            "image_alt_text": content.get("image_alt_text"),
            "published": content.get("published"),
            "published_url": content.get("published_url"),
            "tweet_id": content.get("tweet_id"),
            "bluesky_uri": content.get("bluesky_uri"),
        },
        "queue": queue,
        "claim_check": claim_check,
        "persona_guard": persona_guard,
        "alt_text": alt_text,
        "license_guard": license_guard,
        "attribution_guard": attribution_guard,
        "hashtag_suggestions": (
            hashtag_suggestions.as_dict() if hashtag_suggestions else None
        ),
        "platforms": platforms,
    }


def preview_to_json(preview: dict) -> str:
    """Serialize a preview as stable, readable JSON."""
    return json.dumps(preview, indent=2, sort_keys=True)


def visual_post_artifact_filename(content_id: int | None, *, artifact_format: str = "json") -> str:
    """Return a stable filename for a visual post review artifact."""
    if artifact_format not in {"json", "markdown"}:
        raise ValueError("artifact_format must be 'json' or 'markdown'")
    extension = "json" if artifact_format == "json" else "md"
    prefix = f"visual-post-{content_id}" if content_id is not None else "visual-post-preview"
    return f"{prefix}.{extension}"


def visual_post_artifact_to_json(artifact: dict) -> str:
    """Serialize a visual post artifact as stable, readable JSON."""
    return json.dumps(artifact, indent=2, sort_keys=True)


def format_visual_post_artifact(artifact: dict) -> str:
    """Render a visual post artifact for manual review."""
    run = artifact.get("run") or {}
    content = artifact.get("content") or {}
    image = artifact.get("image") or {}
    preview = artifact.get("preview") or {}

    lines = [
        "# Visual Post Review",
        "",
        f"- Artifact type: {artifact.get('artifact_type', 'visual_post_review')}",
        f"- Generated at: {artifact.get('generated_at', 'unknown')}",
        f"- Outcome: {run.get('outcome', 'unknown')}",
    ]
    if content.get("id") is not None:
        lines.append(f"- Content ID: {content['id']}")
    if run.get("planned_topic_id") is not None:
        lines.append(f"- Planned topic ID: {run['planned_topic_id']}")
    if run.get("planned_topic") and run["planned_topic"].get("topic"):
        topic = run["planned_topic"]["topic"]
        angle = run["planned_topic"].get("angle")
        lines.append(
            f"- Planned topic: {topic}" + (f" ({angle})" if angle else "")
        )
    if run.get("rejection_reason"):
        lines.append(f"- Rejection reason: {run['rejection_reason']}")
    if run.get("published_url"):
        lines.append(f"- Published URL: {run['published_url']}")
    if run.get("tweet_id"):
        lines.append(f"- Tweet ID: {run['tweet_id']}")

    lines.extend(
        [
            "",
            "## Final Text",
            "",
            content.get("text") or artifact.get("final_text") or "",
            "",
            "## Image",
            "",
            f"- Path: {image.get('path') or content.get('image_path') or 'n/a'}",
            f"- Style: {image.get('style') or 'n/a'}",
            f"- Provider: {image.get('provider') or 'n/a'}",
            f"- Alt text: {image.get('alt_text') or content.get('image_alt_text') or 'n/a'}",
            "",
            "### Image Spec",
            "",
            image.get("spec") or content.get("image_prompt") or "",
            "",
            "## Publication Preview",
            "",
            format_preview(preview) if preview else "",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def write_visual_post_artifact(
    artifact: dict,
    path: str | Path,
    *,
    artifact_format: str = "json",
) -> Path:
    """Write a visual post artifact as JSON or markdown."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if artifact_format == "json":
        body = visual_post_artifact_to_json(artifact) + "\n"
    elif artifact_format == "markdown":
        body = format_visual_post_artifact(artifact)
    else:
        raise ValueError("artifact_format must be 'json' or 'markdown'")
    target.write_text(body, encoding="utf-8")
    return target


def format_preview(preview: dict) -> str:
    """Format a preview for terminal review."""
    content = preview["content"]
    lines = [
        f"Content {content['id']} ({content['content_type']})",
    ]

    queue = preview.get("queue")
    if queue:
        lines.append(
            "Queue {queue_id}: {queue_status} for {queue_platform} at {scheduled_at}".format(
                **queue
            )
        )

    if content.get("image_path"):
        lines.append(f"Image: {content['image_path']}")
    if content.get("image_alt_text"):
        lines.append(f"Alt text: {content['image_alt_text']}")

    alt_text = preview.get("alt_text")
    if alt_text and alt_text.get("required"):
        lines.append(f"Alt text guard: {alt_text['status']}")
        for issue in alt_text.get("issues", []):
            lines.append(f"- {issue['code']}: {issue['message']}")

    claim_check = preview.get("claim_check")
    if claim_check:
        lines.append(
            "Claim check: {status} ({supported_count} supported, "
            "{unsupported_count} unsupported)".format(**claim_check)
        )
        if claim_check.get("annotation_text"):
            lines.append("Unsupported claims:")
            lines.extend(
                f"- {line}"
                for line in claim_check["annotation_text"].splitlines()
                if line.strip()
            )

    persona_guard = preview.get("persona_guard")
    if persona_guard:
        score = persona_guard.get("score")
        score_text = "n/a" if score is None else f"{float(score):.2f}"
        lines.append(f"Persona guard: {persona_guard['status']} (score {score_text})")
        if persona_guard.get("reasons"):
            lines.append("Persona guard reasons:")
            lines.extend(f"- {reason}" for reason in persona_guard["reasons"])

    license_guard = preview.get("license_guard")
    if license_guard:
        sources = license_guard.get("restricted_sources") or []
        lines.append(
            "License guard: {status} ({count} restricted sources)".format(
                status=license_guard["status"],
                count=len(sources),
            )
        )
        for source in sources:
            source_url = source.get("source_url") or "no source URL"
            lines.append(
                "- knowledge {knowledge_id}: {license} {source_url}".format(
                    knowledge_id=source["knowledge_id"],
                    license=source["license"],
                    source_url=source_url,
                )
            )

    attribution_guard = preview.get("attribution_guard")
    if attribution_guard:
        missing_sources = attribution_guard.get("missing_sources") or []
        required_sources = attribution_guard.get("required_sources") or []
        lines.append(
            "Attribution guard: {status} ({missing} missing citations, "
            "{required} attribution-required sources)".format(
                status=attribution_guard["status"],
                missing=len(missing_sources),
                required=len(required_sources),
            )
        )
        for source in missing_sources:
            source_url = source.get("source_url") or "no source URL"
            author = source.get("author") or "unknown author"
            lines.append(
                "- knowledge {knowledge_id}: {license} {author} {source_url}".format(
                    knowledge_id=source["knowledge_id"],
                    license=source["license"],
                    author=author,
                    source_url=source_url,
                )
            )

    hashtag_suggestions = preview.get("hashtag_suggestions")
    if hashtag_suggestions:
        lines.append("Suggested hashtags:")
        for platform in ("x", "bluesky", "linkedin"):
            tags = hashtag_suggestions.get(platform) or []
            lines.append(f"- {platform.upper()}: {' '.join(tags) if tags else 'none'}")

    for platform, rendered in preview["platforms"].items():
        status = rendered["status"]
        requested = "requested" if status["requested"] else "not requested"
        lines.append("")
        lines.append(f"{platform.upper()} ({requested}, status: {status['status']})")
        if status.get("platform_url"):
            lines.append(f"URL: {status['platform_url']}")
        if status.get("platform_post_id"):
            lines.append(f"Post ID: {status['platform_post_id']}")
        if status.get("error"):
            lines.append(f"Error: {status['error']}")

        for post in rendered["posts"]:
            counts = post["counts"]
            lines.append(
                "Post {index}/{total} ({characters} chars, {graphemes} graphemes):".format(
                    index=post["index"],
                    total=post["total"],
                    characters=counts["characters"],
                    graphemes=counts["graphemes"],
                )
            )
            lines.append(post["text"])

    return "\n".join(lines)
