"""Backfill canonical platform URLs for durable publication rows."""

from __future__ import annotations

from dataclasses import dataclass


SUPPORTED_PLATFORMS = {"x", "bluesky"}


@dataclass(frozen=True)
class PublicationAccountHandles:
    """Configured account handles used to derive canonical post URLs."""

    x_username: str | None = None
    bluesky_handle: str | None = None


def _normalize_handle(handle: str | None) -> str | None:
    if not handle:
        return None
    normalized = handle.strip().lstrip("@")
    return normalized or None


def x_publication_url(tweet_id: str | None, username: str | None) -> str | None:
    """Build a canonical X status URL when both ID and account handle exist."""
    tweet = str(tweet_id).strip() if tweet_id else ""
    handle = _normalize_handle(username)
    if not tweet or not handle:
        return None
    return f"https://x.com/{handle}/status/{tweet}"


def _parse_bluesky_at_uri(uri: str | None) -> tuple[str | None, str | None]:
    """Return (repo, rkey) from an app.bsky.feed.post AT URI."""
    if not uri:
        return None, None
    parts = uri.strip().split("/")
    if len(parts) != 5 or parts[0] != "at:" or parts[1] != "":
        return None, None
    repo, collection, rkey = parts[2], parts[3], parts[4]
    if collection != "app.bsky.feed.post" or not repo or not rkey:
        return None, None
    return repo, rkey


def bluesky_publication_url(
    at_uri: str | None,
    configured_handle: str | None,
) -> str | None:
    """Build a Bluesky web URL from an AT URI and a resolvable profile handle."""
    repo, rkey = _parse_bluesky_at_uri(at_uri)
    if not repo or not rkey:
        return None
    handle = _normalize_handle(configured_handle)
    if not handle and not repo.startswith("did:") and "." in repo:
        handle = repo
    if not handle:
        return None
    return f"https://bsky.app/profile/{handle}/post/{rkey}"


def _candidate_post_id(row: dict) -> str | None:
    if row.get("platform") == "x":
        return row.get("platform_post_id") or row.get("tweet_id")
    if row.get("platform") == "bluesky":
        return row.get("platform_post_id") or row.get("bluesky_uri")
    return row.get("platform_post_id")


def derive_publication_url(
    row: dict,
    handles: PublicationAccountHandles,
) -> tuple[str | None, str | None]:
    """Return (url, unresolved_reason) for a backfill candidate."""
    platform = row.get("platform")
    if platform == "x":
        post_id = _candidate_post_id(row)
        url = x_publication_url(post_id, handles.x_username)
        if url:
            return url, None
        if not post_id:
            return None, "missing_tweet_id"
        return None, "missing_x_username"

    if platform == "bluesky":
        post_id = _candidate_post_id(row)
        url = bluesky_publication_url(post_id, handles.bluesky_handle)
        if url:
            return url, None
        if not post_id:
            return None, "missing_bluesky_uri"
        return None, "unresolvable_bluesky_uri"

    return None, "unsupported_platform"


def backfill_publication_urls(
    db,
    handles: PublicationAccountHandles,
    days: int = 30,
    platform: str | None = None,
    dry_run: bool = False,
) -> dict:
    """Find missing publication URLs, optionally apply updates, and report results."""
    selected_platform = platform or "all"
    if selected_platform != "all" and selected_platform not in SUPPORTED_PLATFORMS:
        raise ValueError(f"unsupported platform: {selected_platform}")

    candidates = db.list_publication_url_backfill_candidates(
        days=days,
        platform=selected_platform,
    )
    updates = []
    unresolved = []

    for row in candidates:
        url, reason = derive_publication_url(row, handles)
        base = {
            "publication_id": row["publication_id"],
            "content_id": row["content_id"],
            "platform": row["platform"],
            "platform_post_id": _candidate_post_id(row),
            "current_platform_url": row.get("platform_url"),
        }
        if not url:
            unresolved.append({**base, "reason": reason})
            continue

        applied = False if dry_run else db.update_publication_platform_url(
            row["publication_id"],
            url,
        )
        updates.append(
            {
                **base,
                "platform_url": url,
                "applied": applied,
            }
        )

    return {
        "dry_run": dry_run,
        "platform": selected_platform,
        "days": days,
        "candidate_count": len(candidates),
        "update_count": len(updates),
        "unresolved_count": len(unresolved),
        "updates": updates,
        "unresolved": unresolved,
    }


def format_backfill_table(report: dict) -> str:
    """Format a compact operator report."""
    lines = [
        (
            f"Publication URL backfill: {report['update_count']} update(s), "
            f"{report['unresolved_count']} unresolved, "
            f"{report['candidate_count']} candidate(s)"
        )
    ]
    if report["dry_run"]:
        lines.append("Mode: dry-run")

    if report["updates"]:
        lines.append("")
        lines.append("Updates:")
        for update in report["updates"]:
            action = "would update" if report["dry_run"] else "updated"
            if not report["dry_run"] and not update["applied"]:
                action = "skipped"
            lines.append(
                f"- {action} publication {update['publication_id']} "
                f"({update['platform']} content {update['content_id']}): "
                f"{update['platform_url']}"
            )

    if report["unresolved"]:
        lines.append("")
        lines.append("Unresolved:")
        for item in report["unresolved"]:
            lines.append(
                f"- publication {item['publication_id']} "
                f"({item['platform']} content {item['content_id']}): "
                f"{item['reason']}"
            )

    return "\n".join(lines)
