"""Preview generated content exactly as it would be sent to platforms."""

from __future__ import annotations

import json
from typing import Any

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
) -> list[dict]:
    if platform == "bluesky":
        texts = [adapter.adapt(post, content_type) for post in x_posts]
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
            ),
            "image_path": content.get("image_path"),
        }

    return {
        "content": {
            "id": content["id"],
            "content_type": content["content_type"],
            "image_path": content.get("image_path"),
            "image_prompt": content.get("image_prompt"),
            "published": content.get("published"),
            "published_url": content.get("published_url"),
            "tweet_id": content.get("tweet_id"),
            "bluesky_uri": content.get("bluesky_uri"),
        },
        "queue": queue,
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
