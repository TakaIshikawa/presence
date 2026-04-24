#!/usr/bin/env python3
"""Process scheduled posts from publish queue at optimal times."""

import argparse
import signal
import sys
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta, timezone

WATCHDOG_TIMEOUT = 300  # 5 minutes
DEFAULT_MAX_RETRY_DELAY_MINUTES = 360

logger = logging.getLogger(__name__)


def _requested_platforms(platform: str) -> list[str]:
    if platform == 'all':
        return ['x', 'bluesky']
    return [platform]


def _already_published(item: dict, platform: str) -> bool:
    if platform == 'x':
        return bool(item.get('published'))
    if platform == 'bluesky':
        return bool(item.get('bluesky_uri'))
    return False


def _max_retry_delay_minutes(config) -> int:
    publish_queue_config = getattr(config, "publish_queue", None)
    value = getattr(publish_queue_config, "max_retry_delay_minutes", None)
    if isinstance(value, (int, float)) and value > 0:
        return int(value)
    return DEFAULT_MAX_RETRY_DELAY_MINUTES


def _ready_for_attempt(db, content_id: int, platform: str, now_iso: str) -> bool:
    state = db.get_publication_state(content_id, platform)
    if not state:
        return True
    next_retry_at = state.get("next_retry_at")
    return not (
        state.get("status") == "failed"
        and next_retry_at is not None
        and next_retry_at > now_iso
    )


def _timeout_handler(signum, frame):
    logger.error("WATCHDOG: Publish queue process exceeded 5-minute timeout, exiting")
    sys.exit(1)


def _defer_queue_item(db, queue_id: int, scheduled_at: str) -> None:
    db.conn.execute(
        """UPDATE publish_queue
           SET scheduled_at = ?, status = 'queued',
               error = NULL, error_category = NULL, hold_reason = NULL
           WHERE id = ?""",
        (scheduled_at, queue_id),
    )
    db.conn.commit()


def _result_error_category(result, platform: str) -> str:
    category = getattr(result, "error_category", None)
    if category:
        return normalize_error_category(category)
    return classify_publish_error(getattr(result, "error", None), platform=platform)


def _queue_error_category(errors: list[tuple[str, str]]) -> str:
    categories = {category for _, category in errors}
    if len(categories) == 1:
        return categories.pop()
    return "unknown"


def _result_metadata(result) -> dict:
    metadata = {}
    for key in ("status_code", "request_id", "response_id"):
        value = getattr(result, key, None)
        if value is not None:
            metadata[key] = value
    raw = getattr(result, "metadata", None)
    if isinstance(raw, dict):
        metadata.update(raw)
    return metadata


def _record_attempt_from_result(
    db,
    queue_id: int,
    content_id: int,
    platform: str,
    result,
    attempted_at: str,
    error_category: str | None = None,
) -> None:
    success = bool(getattr(result, "success", False))
    platform_post_id = None
    if platform == "x":
        platform_post_id = getattr(result, "tweet_id", None)
    elif platform == "bluesky":
        platform_post_id = getattr(result, "uri", None)
    db.record_publication_attempt(
        queue_id=queue_id,
        content_id=content_id,
        platform=platform,
        attempted_at=attempted_at,
        success=success,
        platform_post_id=platform_post_id,
        platform_url=getattr(result, "url", None),
        error=None if success else str(getattr(result, "error", "")),
        error_category=error_category,
        response_metadata=_result_metadata(result),
    )


def _variant_type_for_content_type(content_type: str) -> str:
    if content_type == "x_thread":
        return "thread"
    return "post"


def _copy_parts(content: str, content_type: str) -> list[str]:
    if content_type == "x_thread":
        return parse_thread_content(content)
    return [content]


def _daily_platform_limits(config) -> dict[str, int]:
    publishing_config = getattr(config, "publishing", None)
    value = getattr(publishing_config, "daily_platform_limits", None)
    if not isinstance(value, dict):
        return {}
    return {
        str(platform): int(limit)
        for platform, limit in value.items()
        if isinstance(limit, int) and not isinstance(limit, bool) and limit >= 0
    }


def _platform_limit_reached(
    db,
    platform: str,
    limits: dict[str, int],
    counts: dict[str, int],
    day_start_iso: str,
) -> bool:
    if platform not in limits:
        return False
    if platform not in counts:
        counts[platform] = db.count_platform_publications_since(platform, day_start_iso)
    return counts[platform] >= limits[platform]


def _next_daily_slot(
    scheduled_at: str,
    now: datetime,
    embargo_windows: list,
) -> datetime:
    try:
        base = datetime.fromisoformat(scheduled_at)
    except (TypeError, ValueError):
        base = now
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    next_slot = base + timedelta(days=1)
    while next_slot <= now:
        next_slot += timedelta(days=1)
    return next_allowed_slot(next_slot, embargo_windows)


# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from evaluation.posting_schedule import (
    embargo_windows_from_config,
    is_embargoed,
    next_allowed_slot,
)
from output.bluesky_client import BlueskyClient
from output.attribution_guard import check_publication_attribution_guard
from output.license_guard import (
    check_publication_license_guard,
    restricted_prompt_behavior_from_config,
)
from output.publish_errors import classify_publish_error, normalize_error_category
from synthesis.alt_text_guard import validate_alt_text

try:
    from output.x_client import XClient, parse_thread_content
except ModuleNotFoundError as exc:
    if exc.name != "tweepy":
        raise

    class XClient:
        def __init__(self, *args, **kwargs):
            raise ModuleNotFoundError(
                "tweepy is required for X publishing"
            ) from exc

    def parse_thread_content(content: str) -> list[str]:
        return [content]

try:
    from output.cross_poster import CrossPoster
except ModuleNotFoundError as exc:
    if exc.name != "tweepy":
        raise

    class CrossPoster:
        def __init__(self, bluesky_client=None):
            self.bluesky_client = bluesky_client

        def adapt_for_bluesky(self, text: str, content_type: str = "x_post") -> str:
            return text


def _alt_text_guard_mode(config) -> str:
    publishing_config = getattr(config, "publishing", None)
    mode = getattr(publishing_config, "alt_text_guard_mode", "strict")
    if mode in {"strict", "warning"}:
        return mode
    return "strict"


def _persona_guard_publish_mode(config) -> str:
    publishing_config = getattr(config, "publishing", None)
    mode = getattr(publishing_config, "persona_guard_publish_mode", "warning")
    if mode in {"strict", "warning", "disabled"}:
        return mode
    return "warning"


def _persona_guard_failure_message(summary: dict | None) -> str | None:
    if not summary:
        return None
    if not summary.get("checked") or summary.get("passed"):
        return None
    status = summary.get("status") or "failed"
    score = summary.get("score")
    reasons = summary.get("reasons") or []
    pieces = [f"status={status}"]
    if score is not None:
        pieces.append(f"score={float(score):.2f}")
    if reasons:
        pieces.append("reasons=" + "; ".join(str(reason) for reason in reasons))
    return "Persona guard failed: " + ", ".join(pieces)


def _enforce_persona_guard(db, queue_id: int, content_id: int, config) -> bool:
    """Return True when publishing may continue for this queue item."""
    mode = _persona_guard_publish_mode(config)
    if mode == "disabled":
        return True

    summary = db.get_persona_guard_summary(content_id)
    message = _persona_guard_failure_message(summary)
    if not message:
        return True

    if mode == "strict":
        db.hold_publish_queue_item(queue_id, reason=message)
        logger.error(f"  {message}; holding queue item {queue_id}")
        return False

    logger.warning(f"  Persona guard warning: {message}")
    return True


def _alt_text_guard_error(item: dict) -> str | None:
    result = validate_alt_text(
        item.get("image_alt_text"),
        image_prompt=item.get("image_prompt"),
        image_path=item.get("image_path"),
        content_type=item.get("content_type"),
    )
    if result.passed:
        return None
    return "; ".join(
        f"{issue.code}: {issue.message}" for issue in result.issues
    )


def _license_guard_summary(license_guard: dict) -> str:
    return "; ".join(
        "knowledge {knowledge_id}: {license} {source_url}".format(
            knowledge_id=source["knowledge_id"],
            license=source["license"],
            source_url=source.get("source_url") or "no source URL",
        )
        for source in license_guard.get("restricted_sources", [])
    )


def _attribution_guard_summary(attribution_guard: dict) -> str:
    return "; ".join(
        "knowledge {knowledge_id}: {license} {author} {source_url}".format(
            knowledge_id=source["knowledge_id"],
            license=source["license"],
            author=source.get("author") or "unknown author",
            source_url=source.get("source_url") or "no source URL",
        )
        for source in attribution_guard.get("missing_sources", [])
    )


def _content_copy_for_platform(
    db,
    content_id: int,
    platform: str,
    variant_type: str,
    fallback_content: str,
) -> str:
    copy = db.get_content_variant_or_original(content_id, platform, variant_type)
    if copy:
        return copy["content"]
    return fallback_content


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--allow-restricted-knowledge",
        action="store_true",
        help="Publish content even when it is linked to restricted knowledge",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args([] if argv is None else argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(WATCHDOG_TIMEOUT)

    with script_context() as (config, db):
        # Initialize clients
        x_client = XClient(
            config.x.api_key,
            config.x.api_secret,
            config.x.access_token,
            config.x.access_token_secret
        )

        bluesky_client = None
        if config.bluesky and config.bluesky.enabled:
            bluesky_client = BlueskyClient(
                config.bluesky.handle,
                config.bluesky.app_password
            )

        # Get current time
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        day_start_iso = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        max_retry_delay_minutes = _max_retry_delay_minutes(config)
        embargo_windows = embargo_windows_from_config(config)
        daily_limits = _daily_platform_limits(config)
        daily_counts = {}
        restricted_prompt_behavior = restricted_prompt_behavior_from_config(config)

        # Get due queue items
        due_items = db.get_due_queue_items(now_iso)

        if not due_items:
            logger.info("No posts due for publishing")
            update_monitoring("run-publish-queue")
            return

        if is_embargoed(now, embargo_windows):
            next_slot = next_allowed_slot(now, embargo_windows)
            next_slot_iso = next_slot.isoformat()
            for item in due_items:
                _defer_queue_item(db, item["id"], next_slot_iso)
            logger.info(
                f"Publishing embargo active; deferred {len(due_items)} posts to {next_slot_iso}"
            )
            update_monitoring("run-publish-queue")
            return

        logger.info(f"Processing {len(due_items)} scheduled posts...")

        for item in due_items:
            queue_id = item['id']
            content_id = item['content_id']
            platform = item['platform']
            content_type = item['content_type']
            scheduled_at = item['scheduled_at']
            image_path = item.get("image_path")
            image_alt_text = item.get("image_alt_text") or ""

            logger.info(f"Publishing content {content_id} (queued for {scheduled_at})")

            try:
                requested_platforms = _requested_platforms(platform)
                pending_platforms = [
                    p for p in requested_platforms
                    if (
                        not _already_published(item, p)
                        and _ready_for_attempt(db, content_id, p, now_iso)
                    )
                ]

                if not pending_platforms:
                    db.mark_queue_published(queue_id)
                    logger.info(f"  Queue item {queue_id} already completed")
                    continue

                if not _enforce_persona_guard(db, queue_id, content_id, config):
                    continue

                license_guard = check_publication_license_guard(
                    db,
                    content_id,
                    restricted_prompt_behavior=restricted_prompt_behavior,
                    allow_restricted=args.allow_restricted_knowledge,
                ).as_dict()
                if license_guard["blocked"]:
                    reason = (
                        "License guard blocked restricted knowledge sources: "
                        f"{_license_guard_summary(license_guard)}"
                    )
                    db.hold_publish_queue_item(queue_id, reason=reason)
                    logger.error(f"  {reason}")
                    continue
                if license_guard["restricted_sources"]:
                    logger.warning(
                        "  License guard warning: "
                        f"{_license_guard_summary(license_guard)}"
                    )

                variant_type = _variant_type_for_content_type(content_type)
                x_copy = db.get_content_variant_or_original(
                    content_id,
                    "x",
                    variant_type,
                )
                if not x_copy:
                    raise ValueError(f"content {content_id} not found")
                x_content = x_copy["content"]
                x_parts = _copy_parts(x_content, content_type)

                for pending_platform in pending_platforms:
                    platform_content = _content_copy_for_platform(
                        db,
                        content_id,
                        pending_platform,
                        variant_type,
                        x_content,
                    )
                    attribution_guard = check_publication_attribution_guard(
                        db,
                        content_id,
                        platform_content,
                    ).as_dict()
                    if attribution_guard["blocked"]:
                        reason = (
                            "Attribution guard blocked missing citations: "
                            f"{_attribution_guard_summary(attribution_guard)}"
                        )
                        db.hold_publish_queue_item(queue_id, reason=reason)
                        logger.error(f"  {reason}")
                        break
                else:
                    attribution_guard = None

                if attribution_guard and attribution_guard["blocked"]:
                    continue

                alt_text_error = _alt_text_guard_error(item)
                if alt_text_error:
                    if _alt_text_guard_mode(config) == "strict":
                        db.mark_queue_failed(
                            queue_id,
                            f"Alt text guard failed: {alt_text_error}",
                            error_category="media",
                        )
                        logger.error(f"  Alt text guard failed: {alt_text_error}")
                        continue
                    logger.warning(f"  Alt text guard warning: {alt_text_error}")

                platform_errors = []
                deferred_platforms = []

                # Publish to X if needed
                if 'x' in pending_platforms:
                    if _platform_limit_reached(
                        db, "x", daily_limits, daily_counts, day_start_iso
                    ):
                        deferred_platforms.append("x")
                        logger.info("  X daily publish cap reached; deferring")
                    else:
                        if content_type == 'x_thread':
                            result = x_client.post_thread(x_parts)
                        elif image_path:
                            result = x_client.post_with_media(
                                text=x_content,
                                media_path=image_path,
                                alt_text=image_alt_text,
                            )
                        else:
                            result = x_client.post(x_content)

                        if result.success:
                            _record_attempt_from_result(
                                db,
                                queue_id,
                                content_id,
                                "x",
                                result,
                                now_iso,
                            )
                            db.mark_published(content_id, result.url, tweet_id=result.tweet_id)
                            daily_counts["x"] = daily_counts.get("x", 0) + 1
                            logger.info(f"  Posted to X: {result.url}")
                        else:
                            category = _result_error_category(result, "x")
                            _record_attempt_from_result(
                                db,
                                queue_id,
                                content_id,
                                "x",
                                result,
                                now_iso,
                                error_category=category,
                            )
                            logger.error(f"  X posting failed: {result.error}")
                            db.upsert_publication_failure(
                                content_id,
                                "x",
                                str(result.error),
                                max_retry_delay_minutes=max_retry_delay_minutes,
                                error_category=category,
                            )
                            platform_errors.append((f"X: {result.error}", category))

                # Cross-post to Bluesky if needed and configured
                if 'bluesky' in pending_platforms:
                    if _platform_limit_reached(
                        db, "bluesky", daily_limits, daily_counts, day_start_iso
                    ):
                        deferred_platforms.append("bluesky")
                        logger.info("  Bluesky daily publish cap reached; deferring")
                    elif bluesky_client:
                        bsky_copy = db.get_content_variant_or_original(
                            content_id,
                            "bluesky",
                            variant_type,
                        )
                        if not bsky_copy:
                            raise ValueError(f"content {content_id} not found")
                        if bsky_copy["source"] == "variant":
                            bsky_tweets = _copy_parts(bsky_copy["content"], content_type)
                        else:
                            cross_poster = CrossPoster(bluesky_client=bluesky_client)
                            bsky_tweets = [
                                cross_poster.adapt_for_bluesky(t, content_type)
                                for t in x_parts
                            ]

                        if content_type == 'x_thread':
                            bsky_result = bluesky_client.post_thread(bsky_tweets)
                        elif image_path and hasattr(bluesky_client, "post_with_media"):
                            bsky_result = bluesky_client.post_with_media(
                                text=bsky_tweets[0],
                                media_path=image_path,
                                alt_text=image_alt_text,
                            )
                        else:
                            bsky_result = bluesky_client.post(bsky_tweets[0])

                        if bsky_result.success:
                            _record_attempt_from_result(
                                db,
                                queue_id,
                                content_id,
                                "bluesky",
                                bsky_result,
                                now_iso,
                            )
                            db.mark_published_bluesky(
                                content_id,
                                bsky_result.uri,
                                url=getattr(bsky_result, "url", None),
                            )
                            daily_counts["bluesky"] = daily_counts.get("bluesky", 0) + 1
                            logger.info(f"  Posted to Bluesky: {bsky_result.url}")
                        else:
                            category = _result_error_category(bsky_result, "bluesky")
                            _record_attempt_from_result(
                                db,
                                queue_id,
                                content_id,
                                "bluesky",
                                bsky_result,
                                now_iso,
                                error_category=category,
                            )
                            logger.error(f"  Bluesky posting failed: {bsky_result.error}")
                            db.upsert_publication_failure(
                                content_id,
                                "bluesky",
                                str(bsky_result.error),
                                max_retry_delay_minutes=max_retry_delay_minutes,
                                error_category=category,
                            )
                            platform_errors.append((f"Bluesky: {bsky_result.error}", category))
                    else:
                        category = "auth"
                        logger.error("  Bluesky posting failed: client not configured")
                        db.record_publication_attempt(
                            queue_id=queue_id,
                            content_id=content_id,
                            platform="bluesky",
                            attempted_at=now_iso,
                            success=False,
                            error="client not configured",
                            error_category=category,
                        )
                        db.upsert_publication_failure(
                            content_id,
                            "bluesky",
                            "client not configured",
                            max_retry_delay_minutes=max_retry_delay_minutes,
                            error_category=category,
                        )
                        platform_errors.append(("Bluesky: client not configured", category))

                if platform_errors:
                    error_text = "; ".join(error for error, _ in platform_errors)
                    db.mark_queue_failed(
                        queue_id,
                        error_text,
                        error_category=_queue_error_category(platform_errors),
                    )
                    logger.info(f"  Queue item {queue_id} failed for: {error_text}")
                elif deferred_platforms:
                    next_slot = _next_daily_slot(scheduled_at, now, embargo_windows)
                    next_slot_iso = next_slot.isoformat()
                    _defer_queue_item(db, queue_id, next_slot_iso)
                    logger.info(
                        f"  Queue item {queue_id} deferred to {next_slot_iso} "
                        f"for daily caps: {', '.join(deferred_platforms)}"
                    )
                else:
                    # Mark queue item as published
                    db.mark_queue_published(queue_id)
                    logger.info(f"  Queue item {queue_id} completed")

            except (sqlite3.Error, KeyError, IndexError, AttributeError, TypeError, ValueError) as e:
                logger.error(f"  Unexpected error publishing content {content_id}: {e}")
                db.mark_queue_failed(
                    queue_id,
                    str(e),
                    error_category=classify_publish_error(e),
                )

    update_monitoring("run-publish-queue")
    logger.info("Done")


if __name__ == "__main__":
    main(sys.argv[1:])
