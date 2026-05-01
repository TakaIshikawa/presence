"""Ingest Mastodon mention notifications into the reply queue."""

from __future__ import annotations

import json
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlparse


MASTODON_PLATFORM = "mastodon"
DEFAULT_LIMIT = 40


@dataclass(frozen=True)
class MastodonMention:
    notification_id: str
    status_id: str
    inbound_author_handle: str
    inbound_author_id: str
    inbound_text: str
    inbound_url: str | None
    inbound_created_at: str | None
    our_status_id: str
    our_content_id: int | None
    our_post_text: str
    metadata: dict[str, Any]


class _HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        if data:
            self.parts.append(data)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"br", "p", "div"} and self.parts:
            self.parts.append("\n")

    def text(self) -> str:
        return " ".join("".join(self.parts).split())


def strip_html(value: str | None) -> str:
    parser = _HTMLTextExtractor()
    parser.feed(value or "")
    return parser.text()


def normalize_author_handle(account: dict[str, Any] | None, *, base_url: str = "") -> str:
    account = account or {}
    acct = str(account.get("acct") or account.get("username") or "").strip().lstrip("@")
    if not acct:
        return "unknown"
    if "@" in acct:
        return f"@{acct.lower()}"

    host = urlparse(str(account.get("url") or "")).hostname or urlparse(base_url).hostname
    return f"@{acct.lower()}@{host.lower()}" if host else f"@{acct.lower()}"


def inbound_status_url(status: dict[str, Any], account: dict[str, Any] | None) -> str | None:
    if status.get("url"):
        return str(status["url"])
    uri = status.get("uri")
    if isinstance(uri, str) and uri.startswith(("http://", "https://")):
        return uri
    account_url = (account or {}).get("url")
    status_id = status.get("id")
    if account_url and status_id:
        return f"{str(account_url).rstrip('/')}/{status_id}"
    return None


def fetch_mastodon_mention_notifications(
    *,
    base_url: str,
    access_token: str,
    cursor: str | None = None,
    limit: int = DEFAULT_LIMIT,
    session: Any | None = None,
    timeout: float = 30.0,
) -> list[dict[str, Any]]:
    """Fetch account mention notifications from a Mastodon-compatible API."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    if not base_url:
        raise ValueError("base_url is required")
    if not access_token:
        raise ValueError("access_token is required")

    if session is None:
        import requests

        session = requests

    params: dict[str, Any] = {"types[]": "mention", "limit": limit}
    if cursor:
        params["since_id"] = cursor
    response = session.get(
        f"{base_url.rstrip('/')}/api/v1/notifications",
        headers={"Authorization": f"Bearer {access_token}"},
        params=params,
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("Mastodon notifications response must be a list")
    return payload


def normalize_notification(
    notification: dict[str, Any],
    *,
    db: Any,
    base_url: str = "",
) -> MastodonMention | None:
    """Return a reply_queue-compatible mention, or None for non-mentions."""
    if notification.get("type") != "mention":
        return None
    notification_id = _clean_id(notification.get("id"))
    status = notification.get("status")
    if not notification_id or not isinstance(status, dict):
        return None
    status_id = _clean_id(status.get("id"))
    if not status_id:
        return None

    account = status.get("account") or notification.get("account") or {}
    our_status_id = _clean_id(status.get("in_reply_to_id")) or status_id
    our_content = find_mastodon_content(db, our_status_id) if our_status_id else None
    metadata = {
        "notification_id": notification_id,
        "status_id": status_id,
        "created_at": status.get("created_at"),
        "visibility": status.get("visibility"),
        "uri": status.get("uri"),
        "in_reply_to_account_id": status.get("in_reply_to_account_id"),
        "mentions": status.get("mentions") or [],
    }

    return MastodonMention(
        notification_id=notification_id,
        status_id=status_id,
        inbound_author_handle=normalize_author_handle(account, base_url=base_url),
        inbound_author_id=_clean_id(account.get("id")) or "",
        inbound_text=strip_html(status.get("content")),
        inbound_url=inbound_status_url(status, account),
        inbound_created_at=status.get("created_at"),
        our_status_id=our_status_id or status_id,
        our_content_id=int(our_content["id"]) if our_content else None,
        our_post_text=str(our_content["content"]) if our_content else "",
        metadata={k: v for k, v in metadata.items() if v not in (None, "", [])},
    )


def find_mastodon_content(db: Any, status_id: str) -> dict[str, Any] | None:
    """Find generated content for a Mastodon publication status ID."""
    if not status_id or not getattr(db, "conn", None):
        return None
    row = db.conn.execute(
        """SELECT gc.id, gc.content, gc.content_type
           FROM generated_content gc
           INNER JOIN content_publications cp ON cp.content_id = gc.id
           WHERE lower(cp.platform) = 'mastodon'
             AND (cp.platform_post_id = ?
              OR cp.platform_url LIKE ?)
           ORDER BY cp.published_at DESC, cp.id DESC
           LIMIT 1""",
        (status_id, f"%/{status_id}"),
    ).fetchone()
    return dict(row) if row else None


def processed_mastodon_ids(db: Any) -> set[str]:
    """Return queued Mastodon notification and status IDs."""
    ids: set[str] = set()
    if not getattr(db, "conn", None):
        return ids
    rows = db.conn.execute(
        """SELECT inbound_tweet_id, platform_metadata
           FROM reply_queue
           WHERE lower(COALESCE(platform, '')) = 'mastodon'"""
    ).fetchall()
    for row in rows:
        inbound_id = row["inbound_tweet_id"] if hasattr(row, "keys") else row[0]
        metadata_raw = row["platform_metadata"] if hasattr(row, "keys") else row[1]
        if inbound_id:
            ids.add(str(inbound_id))
        try:
            metadata = json.loads(metadata_raw or "{}")
        except (TypeError, json.JSONDecodeError):
            metadata = {}
        for key in ("notification_id", "status_id"):
            if metadata.get(key):
                ids.add(str(metadata[key]))
    return ids


def ingest_mastodon_mentions(
    *,
    db: Any,
    notifications: list[dict[str, Any]],
    base_url: str = "",
    dry_run: bool = False,
    cursor: str | None = None,
) -> dict[str, Any]:
    """Normalize and optionally insert unseen Mastodon mentions."""
    processed_ids = processed_mastodon_ids(db)
    inserted: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    highest_id = cursor

    for notification in notifications:
        notification_id = _clean_id(notification.get("id"))
        highest_id = _max_id(highest_id, notification_id)
        mention = normalize_notification(notification, db=db, base_url=base_url)
        if not mention:
            skipped.append({"notification_id": notification_id, "reason": "not_ingestable"})
            continue
        if mention.notification_id in processed_ids or mention.status_id in processed_ids:
            skipped.append(
                {
                    "notification_id": mention.notification_id,
                    "status_id": mention.status_id,
                    "reason": "already_processed",
                }
            )
            continue

        item = {
            "notification_id": mention.notification_id,
            "status_id": mention.status_id,
            "author_handle": mention.inbound_author_handle,
            "inbound_url": mention.inbound_url,
            "our_status_id": mention.our_status_id,
            "our_content_id": mention.our_content_id,
        }
        if not dry_run:
            reply_id = db.insert_reply_draft(
                inbound_tweet_id=mention.status_id,
                inbound_author_handle=mention.inbound_author_handle,
                inbound_author_id=mention.inbound_author_id,
                inbound_text=mention.inbound_text,
                our_tweet_id=mention.our_status_id,
                our_content_id=mention.our_content_id,
                our_post_text=mention.our_post_text,
                draft_text="",
                platform=MASTODON_PLATFORM,
                inbound_url=mention.inbound_url,
                our_platform_id=mention.our_status_id,
                platform_metadata=json.dumps(mention.metadata, sort_keys=True),
                intent="other",
                priority="normal",
                status="pending",
            )
            item["reply_queue_id"] = reply_id
            processed_ids.update({mention.notification_id, mention.status_id})
        inserted.append(item)

    return {
        "platform": MASTODON_PLATFORM,
        "dry_run": dry_run,
        "cursor": cursor,
        "next_cursor": highest_id,
        "counts": {
            "fetched": len(notifications),
            "inserted": len(inserted),
            "skipped": len(skipped),
        },
        "inserted": inserted,
        "skipped": skipped,
    }


def poll_mastodon_mentions(
    *,
    db: Any,
    base_url: str,
    access_token: str,
    limit: int = DEFAULT_LIMIT,
    dry_run: bool = False,
    session: Any | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """Fetch Mastodon mentions, ingest unseen ones, and advance the cursor."""
    cursor = db.get_platform_reply_cursor(MASTODON_PLATFORM)
    notifications = fetch_mastodon_mention_notifications(
        base_url=base_url,
        access_token=access_token,
        cursor=cursor,
        limit=limit,
        session=session,
        timeout=timeout,
    )
    report = ingest_mastodon_mentions(
        db=db,
        notifications=notifications,
        base_url=base_url,
        dry_run=dry_run,
        cursor=cursor,
    )
    next_cursor = report.get("next_cursor")
    if not dry_run and next_cursor and next_cursor != cursor:
        db.set_platform_reply_cursor(MASTODON_PLATFORM, str(next_cursor))
    return report


def format_mastodon_mentions_text(report: dict[str, Any]) -> str:
    action = "Would insert" if report.get("dry_run") else "Inserted"
    counts = report["counts"]
    lines = [
        f"{action} {counts['inserted']} Mastodon mention"
        f"{'' if counts['inserted'] == 1 else 's'}.",
        f"Fetched {counts['fetched']}; skipped {counts['skipped']}.",
    ]
    if report.get("next_cursor"):
        lines.append(f"Next cursor: {report['next_cursor']}")
    return "\n".join(lines)


def format_mastodon_mentions_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def _clean_id(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _max_id(current: str | None, candidate: str | None) -> str | None:
    if not candidate:
        return current
    if not current:
        return candidate
    try:
        return str(max(int(current), int(candidate)))
    except ValueError:
        return max(current, candidate)
