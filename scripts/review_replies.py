#!/usr/bin/env python3
"""Interactive review of pending reply drafts."""

import json
import logging
import sys
from pathlib import Path
from types import SimpleNamespace

logger = logging.getLogger(__name__)

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from storage.db import Database
from output.x_client import XClient
from output.bluesky_client import BlueskyClient
from review_helpers import truncate, read_char, format_relationship_context


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    config = load_config()

    db = Database(config.paths.database)
    db.connect()
    db.init_schema(str(Path(__file__).parent.parent / "schema.sql"))

    pending = db.get_pending_replies()
    if not pending:
        logger.info("No pending reply drafts.")
        db.close()
        return

    logger.info(f"\n{len(pending)} pending reply draft{'s' if len(pending) != 1 else ''}\n")

    x_client = None
    bluesky_client = None

    quit_requested = False
    posted = 0

    for i, reply in enumerate(pending):
        if quit_requested:
            break

        logger.info(f"{'─' * 60}")
        header = f"{i + 1}/{len(pending)}  @{reply['inbound_author_handle']} replied to your post"

        # Relationship context (from cultivate enrichment stored at poll time)
        ctx_line = format_relationship_context(reply["relationship_context"])
        if ctx_line:
            header += f"\n     [{ctx_line}]"

        # Quality score
        quality_line = _format_quality_line(reply["quality_score"], reply["quality_flags"])
        if quality_line:
            header += f"\n     [{quality_line}]"

        logger.info(header)
        logger.info("")
        logger.info(f"  Your post:   \"{truncate(reply['our_post_text'], 120)}\"")
        logger.info(f"  Their reply: \"{truncate(reply['inbound_text'], 120)}\"")
        logger.info(f"  Draft:       \"{truncate(reply['draft_text'], 120)}\"")
        logger.info("")

        while True:
            sys.stdout.write("  [a]pprove  [e]dit  [d]ismiss  [s]kip  [q]uit > ")
            sys.stdout.flush()

            choice = read_char().lower()
            logger.info(choice)

            if choice == "q":
                quit_requested = True
                break

            elif choice == "a":
                result = _publish_reply(reply, reply["draft_text"], config, x_client, bluesky_client)
                x_client = result["x_client"]
                bluesky_client = result["bluesky_client"]
                if _record_publish_result(db, reply, result["publish_result"]):
                    posted += 1
                break

            elif choice == "e":
                edited = input("  Your reply: ").strip()
                if not edited:
                    logger.info("  Empty, cancelled.")
                    continue
                if len(edited) > 280:
                    logger.info(f"  Too long ({len(edited)} chars, max 280). Try again.")
                    continue
                result = _publish_reply(reply, edited, config, x_client, bluesky_client)
                x_client = result["x_client"]
                bluesky_client = result["bluesky_client"]
                if _record_publish_result(db, reply, result["publish_result"]):
                    posted += 1
                break

            elif choice == "d":
                db.update_reply_status(reply["id"], "dismissed")
                logger.info("  Dismissed.")
                break

            else:  # skip or any other key
                logger.info("  Skipped.")
                break

    logger.info(f"\nDone. {posted} replies posted.")
    db.close()


def _get_x_client(config, x_client):
    if x_client is None:
        x_client = XClient(
            config.x.api_key,
            config.x.api_secret,
            config.x.access_token,
            config.x.access_token_secret
        )
    return x_client


def _get_bluesky_client(config, bluesky_client):
    if bluesky_client is None:
        bluesky_config = getattr(config, "bluesky", None)
        if not bluesky_config or not getattr(bluesky_config, "enabled", False):
            raise RuntimeError("Bluesky is not configured or enabled")
        bluesky_client = BlueskyClient(
            bluesky_config.handle,
            bluesky_config.app_password,
        )
    return bluesky_client


def _publish_reply(reply, text, config, x_client, bluesky_client):
    platform = (reply.get("platform") or "x").lower()
    if platform == "bluesky":
        try:
            bluesky_client = _get_bluesky_client(config, bluesky_client)
            publish_result = bluesky_client.reply_from_queue_metadata(
                text,
                inbound_uri=reply.get("inbound_tweet_id"),
                inbound_cid=reply.get("inbound_cid"),
                platform_metadata=reply.get("platform_metadata"),
                our_platform_id=reply.get("our_platform_id") or reply.get("our_tweet_id"),
            )
        except RuntimeError as e:
            publish_result = SimpleNamespace(
                success=False,
                error=str(e),
                url=None,
            )
    else:
        x_client = _get_x_client(config, x_client)
        publish_result = x_client.reply(text, reply["inbound_tweet_id"])
    return {
        "publish_result": publish_result,
        "x_client": x_client,
        "bluesky_client": bluesky_client,
    }


def _posted_platform_id(result):
    return getattr(result, "uri", None) or getattr(result, "tweet_id", None)


def _record_publish_result(db, reply, result) -> bool:
    if result.success:
        posted_platform_id = _posted_platform_id(result)
        db.update_reply_status(
            reply["id"],
            "posted",
            posted_tweet_id=getattr(result, "tweet_id", None),
            posted_platform_id=posted_platform_id,
        )
        logger.info(f"  Posted: {result.url}")
        return True

    logger.error(f"  Error: {result.error}")
    return False


def _format_quality_line(quality_score, quality_flags_json):
    """Format quality score and flags for display. Returns None if no score."""
    if quality_score is None:
        return None
    flags = []
    if quality_flags_json:
        try:
            flags = json.loads(quality_flags_json)
        except (json.JSONDecodeError, TypeError):
            pass
    result = f"Quality: {quality_score:.1f}/10"
    if flags:
        result += f" ⚠ {', '.join(flags)}"
    return result


if __name__ == "__main__":
    main()
