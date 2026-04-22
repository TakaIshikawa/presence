"""Preview generated content exactly as it would be sent to platforms."""

from __future__ import annotations

import json
from typing import Any

from synthesis.alt_text_guard import validate_alt_text
from synthesis.hashtag_suggester import HashtagSuggestions, suggest_hashtags

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

    platforms = {}
    for platform in ("x", "bluesky"):
        state = _fetch_publication_state(db, content["id"], platform)
        platforms[platform] = {
            "status": _platform_status(
                content,
                platform,
                state,
                queue,
                platform in requested,
            ),
            "posts": _render_platform_posts(
                platform,
                x_posts,
                content["content_type"],
                adapter,
                hashtag_suggestions,
            ),
            "suggested_hashtags": (
                list(hashtag_suggestions.for_platform(platform))
                if hashtag_suggestions
                else []
            ),
            "image_path": content.get("image_path"),
            "image_alt_text": content.get("image_alt_text"),
            "alt_text": alt_text,
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
        "hashtag_suggestions": (
            hashtag_suggestions.as_dict() if hashtag_suggestions else None
        ),
        "platforms": platforms,
    }


def preview_to_json(preview: dict) -> str:
    """Serialize a preview as stable, readable JSON."""
    return json.dumps(preview, indent=2, sort_keys=True)


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
