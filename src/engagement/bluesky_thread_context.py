"""Utilities for enriching queued Bluesky replies with thread context."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any


class BlueskyThreadContextError(ValueError):
    """Raised when a Bluesky thread payload cannot provide required context."""


@dataclass(frozen=True)
class ThreadContextUpdate:
    """Metadata update for a queued Bluesky reply."""

    metadata: dict[str, Any]
    platform_metadata: str


def normalize_at_uri(ref: str | None, *, default_handle: str | None = None) -> str:
    """Normalize supported Bluesky post references to an AT URI."""
    value = (ref or "").strip()
    if not value:
        raise BlueskyThreadContextError("missing Bluesky post reference")
    if value.startswith("at://"):
        return value

    url_match = re.search(r"bsky\.app/profile/([^/]+)/post/([^/?#]+)", value)
    if url_match:
        actor, rkey = url_match.groups()
        return f"at://{actor}/app.bsky.feed.post/{rkey}"

    path_match = re.match(r"([^/]+)/app\.bsky\.feed\.post/([^/?#]+)$", value)
    if path_match:
        actor, rkey = path_match.groups()
        return f"at://{actor}/app.bsky.feed.post/{rkey}"

    if "/" not in value and default_handle:
        return f"at://{default_handle}/app.bsky.feed.post/{value}"

    raise BlueskyThreadContextError(f"unsupported Bluesky post reference: {ref}")


def parse_platform_metadata(value: str | dict[str, Any] | None) -> dict[str, Any]:
    """Parse stored platform metadata into a mutable dict."""
    if isinstance(value, dict):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def metadata_is_incomplete(value: str | dict[str, Any] | None) -> bool:
    """Return True when Bluesky reply metadata lacks imported thread context."""
    metadata = parse_platform_metadata(value)
    required = (
        "root_uri",
        "root_post_text",
        "root_author_handle",
        "parent_post_uri",
        "parent_post_text",
        "parent_author_handle",
    )
    return any(not metadata.get(key) for key in required)


def build_reply_context_update(
    row: dict[str, Any],
    *,
    client: Any | None = None,
    thread_payload: Any | None = None,
    default_handle: str | None = None,
) -> ThreadContextUpdate:
    """Build a merged platform_metadata update for one reply_queue row.

    ``thread_payload`` may be an atproto response, a thread object, or a dict
    containing ``thread``. When omitted, the payload is fetched from ``client``.
    """
    metadata = parse_platform_metadata(row.get("platform_metadata"))
    root_uri = _root_uri(row, metadata, default_handle=default_handle)
    parent_uri = _parent_uri(row, metadata, default_handle=default_handle) or root_uri

    payload = thread_payload
    if payload is None:
        payload = fetch_thread_payload(client, root_uri)

    thread = _thread_from_payload(payload)
    posts = _posts_by_uri(thread)
    if not posts:
        raise BlueskyThreadContextError("thread payload did not contain any posts")

    root_post = posts.get(root_uri) or _post_from_node(thread)
    root_context = _post_context(root_post, uri=root_uri, prefix="root")
    if not root_context.get("root_post_text"):
        raise BlueskyThreadContextError(f"thread payload missing root post text for {root_uri}")

    parent_post = posts.get(parent_uri)
    if parent_post is None:
        raise BlueskyThreadContextError(f"thread payload missing parent post {parent_uri}")
    parent_context = _post_context(parent_post, uri=parent_uri, prefix="parent")
    if not parent_context.get("parent_post_text"):
        raise BlueskyThreadContextError(
            f"thread payload missing parent post text for {parent_uri}"
        )

    merged = dict(metadata)
    merged.update(root_context)
    merged.update(parent_context)
    merged["root_uri"] = root_context["root_uri"]
    merged["parent_post_uri"] = parent_context["parent_post_uri"]

    root_cid = _get(root_post, "cid")
    parent_cid = _get(parent_post, "cid")
    if root_cid:
        merged["root_cid"] = root_cid
        merged.setdefault("reply_root", {"uri": root_uri, "cid": root_cid})
    if parent_cid:
        merged["parent_cid"] = parent_cid
        merged.setdefault("reply_parent", {"uri": parent_uri, "cid": parent_cid})

    return ThreadContextUpdate(
        metadata=merged,
        platform_metadata=json.dumps(merged, sort_keys=True),
    )


def fetch_thread_payload(client: Any, root_uri: str) -> Any:
    """Fetch a thread payload through a BlueskyClient-like object."""
    if client is None:
        raise BlueskyThreadContextError("missing Bluesky client")

    fetch = _callable_attr(client, "get_post_thread_payload")
    if callable(fetch):
        return fetch(root_uri)

    ensure_login = getattr(client, "_ensure_login", None)
    if callable(ensure_login):
        ensure_login()

    fetch = _callable_attr(client, "get_post_thread")
    if callable(fetch):
        return fetch(uri=root_uri)

    raw_client = getattr(client, "client", None)
    fetch = _callable_attr(raw_client, "get_post_thread") if raw_client else None
    if callable(fetch):
        return fetch(uri=root_uri)

    raise BlueskyThreadContextError("Bluesky client cannot fetch post threads")


def _callable_attr(obj: Any, name: str) -> Any:
    if obj is None:
        return None
    if (
        obj.__class__.__module__.startswith("unittest.mock")
        and name not in vars(obj)
        and name not in getattr(obj, "_mock_children", {})
    ):
        return None
    return getattr(obj, name, None)


def _root_uri(
    row: dict[str, Any],
    metadata: dict[str, Any],
    *,
    default_handle: str | None,
) -> str:
    root_ref = metadata.get("reply_root") if isinstance(metadata.get("reply_root"), dict) else {}
    candidates = (
        root_ref.get("uri"),
        metadata.get("root_uri"),
        metadata.get("parent_post_uri"),
        row.get("our_platform_id"),
        row.get("our_tweet_id"),
    )
    for candidate in candidates:
        if candidate:
            return normalize_at_uri(str(candidate), default_handle=default_handle)
    raise BlueskyThreadContextError(f"reply {row.get('id')} has no root URI")


def _parent_uri(
    row: dict[str, Any],
    metadata: dict[str, Any],
    *,
    default_handle: str | None,
) -> str | None:
    parent_ref = (
        metadata.get("reply_parent")
        if isinstance(metadata.get("reply_parent"), dict)
        else {}
    )
    candidates = (
        parent_ref.get("uri"),
        metadata.get("parent_uri"),
        metadata.get("parent_post_uri"),
        row.get("our_platform_id"),
        row.get("our_tweet_id"),
    )
    for candidate in candidates:
        if candidate:
            return normalize_at_uri(str(candidate), default_handle=default_handle)
    return None


def _thread_from_payload(payload: Any) -> Any:
    thread = _get(payload, "thread")
    if thread is not None:
        return thread
    if _get(payload, "post") is not None:
        return payload
    raise BlueskyThreadContextError("thread payload missing thread root")


def _posts_by_uri(thread: Any) -> dict[str, Any]:
    posts: dict[str, Any] = {}
    for node in _walk_thread(thread):
        post = _post_from_node(node)
        uri = _get(post, "uri")
        if uri:
            posts[str(uri)] = post
    return posts


def _walk_thread(node: Any):
    if node is None:
        return
    if _post_from_node(node) is not None:
        yield node
    for reply in _get(node, "replies") or []:
        yield from _walk_thread(reply)


def _post_from_node(node: Any) -> Any:
    return _get(node, "post")


def _post_context(post: Any, *, uri: str, prefix: str) -> dict[str, Any]:
    author = _get(post, "author") or {}
    record = _get(post, "record") or {}
    text = _get(record, "text")
    uri_key = "parent_post_uri" if prefix == "parent" else f"{prefix}_uri"
    return {
        uri_key: uri,
        f"{prefix}_post_text": text or "",
        f"{prefix}_author_handle": _get(author, "handle"),
        f"{prefix}_author_did": _get(author, "did"),
    }


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)
