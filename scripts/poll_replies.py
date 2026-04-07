#!/usr/bin/env python3
"""Poll for replies to our published posts and draft responses."""

import json
import signal
import sys
from pathlib import Path

WATCHDOG_TIMEOUT = 600  # 10 minutes


def _timeout_handler(signum, frame):
    print("WATCHDOG: Reply poll exceeded 10-minute timeout, exiting")
    sys.exit(1)


# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from storage.db import Database
from output.x_client import XClient
from engagement.reply_drafter import ReplyDrafter


def main():
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(WATCHDOG_TIMEOUT)

    config = load_config()

    # Check if replies are enabled
    if config.replies and not config.replies.enabled:
        print("Replies disabled in config, skipping")
        return

    # Initialize components
    db = Database(config.paths.database)
    db.connect()
    db.init_schema(str(Path(__file__).parent.parent / "schema.sql"))

    x_client = XClient(
        config.x.api_key,
        config.x.api_secret,
        config.x.access_token,
        config.x.access_token_secret
    )

    drafter = ReplyDrafter(
        api_key=config.anthropic.api_key,
        model=config.synthesis.model,
        timeout=config.timeouts.anthropic_seconds,
    )

    # Cultivate integration (optional — works without it)
    bridge = None
    evaluator = None
    if config.cultivate and config.cultivate.enabled:
        from engagement.cultivate_bridge import CultivateBridge
        bridge = CultivateBridge.try_connect(config.cultivate.db_path)
        if bridge:
            print("Cultivate integration active")
        else:
            print("Cultivate DB not found, continuing without enrichment")

        from engagement.reply_evaluator import ReplyEvaluator
        evaluator = ReplyEvaluator(
            api_key=config.anthropic.api_key,
            model=config.synthesis.model,
        )

    # Check daily reply cap
    max_daily = config.replies.max_daily_replies if config.replies else 10
    replies_today = db.count_replies_today()
    if replies_today >= max_daily:
        print(f"Daily reply cap reached ({replies_today}/{max_daily}), skipping")
        db.close()
        return

    # Get our user ID for filtering
    me = x_client.client.get_me()
    my_user_id = str(me.data.id)
    my_handle = me.data.username

    # Load cursor
    since_id = db.get_last_mention_id()
    print(f"Polling mentions since_id={since_id or 'None'}")

    # Fetch mentions
    try:
        mentions, users_by_id = x_client.get_mentions(
            since_id=since_id, max_results=50
        )
    except Exception as e:
        print(f"Error fetching mentions: {e}")
        db.close()
        return

    if not mentions:
        print("No new mentions")
        db.close()
        return

    print(f"Found {len(mentions)} mentions")

    # Track highest mention ID for cursor
    max_mention_id = since_id
    drafted = 0
    skipped = 0
    remaining_cap = max_daily - replies_today

    for mention in mentions:
        tweet_id = mention["id"]

        # Update cursor (mention IDs are monotonically increasing)
        if max_mention_id is None or tweet_id > max_mention_id:
            max_mention_id = tweet_id

        # Skip if already processed
        if db.is_reply_processed(tweet_id):
            skipped += 1
            continue

        # Skip self-mentions (our own tweets)
        if mention["author_id"] == my_user_id:
            skipped += 1
            continue

        # Skip if not a reply to us (just a mention in someone else's tweet)
        if mention["in_reply_to_user_id"] != my_user_id:
            skipped += 1
            continue

        # Find the conversation root — this should be one of our published tweets
        # The conversation_id is the root tweet of the thread
        conversation_id = mention.get("conversation_id")
        our_content = None
        our_tweet_id = None

        if conversation_id:
            our_content = db.get_content_by_tweet_id(conversation_id)
            if our_content:
                our_tweet_id = conversation_id

        if not our_content:
            # Not a reply to one of our tracked posts
            skipped += 1
            continue

        # Check daily cap
        if drafted >= remaining_cap:
            print(f"Daily reply cap reached during processing ({replies_today + drafted}/{max_daily})")
            break

        # Get author handle
        author_handle = "unknown"
        author_id = mention["author_id"]
        if author_id in users_by_id:
            author_handle = users_by_id[author_id]["username"]

        # Cultivate: look up relationship context
        person_context = None
        if bridge and config.cultivate.enrich_replies:
            person_context = bridge.get_person_context(author_handle)
            if person_context:
                print(f"  Context: {person_context.stage_name} (stage {person_context.engagement_stage}), "
                      f"{person_context.tier_name} (tier {person_context.dunbar_tier})")

        # Cultivate: forward mention event
        if bridge and config.cultivate.forward_mentions:
            try:
                bridge.record_mention_event(
                    tweet_id=tweet_id,
                    author_x_id=author_id,
                    author_handle=author_handle,
                    text=mention["text"],
                    created_at=mention.get("created_at", ""),
                )
            except Exception as e:
                print(f"  Warning: failed to forward mention to cultivate: {e}")

        # Draft reply (enriched with relationship context if available)
        print(f"  Drafting reply to @{author_handle}: \"{mention['text'][:60]}...\"")
        try:
            draft = drafter.draft(
                our_post=our_content["content"],
                their_reply=mention["text"],
                their_handle=author_handle,
                self_handle=my_handle,
                person_context=person_context,
            )
        except Exception as e:
            print(f"  Error drafting reply: {e}")
            continue

        # Quality evaluation (if evaluator configured)
        relationship_context = None
        quality_score = None
        quality_flags = None
        if person_context:
            relationship_context = person_context.to_json()
        if evaluator:
            threshold = config.cultivate.reply_quality_threshold
            eval_result = evaluator.evaluate(
                draft=draft,
                our_post=our_content["content"],
                their_reply=mention["text"],
                threshold=threshold,
                person_context=person_context,
            )
            quality_score = eval_result.score
            quality_flags = json.dumps(eval_result.flags) if eval_result.flags else None
            if not eval_result.passes:
                flag_str = ", ".join(eval_result.flags) if eval_result.flags else "low score"
                print(f"  Quality flag: {eval_result.score:.1f}/10 ({flag_str})")

        # Store in queue
        db.insert_reply_draft(
            inbound_tweet_id=tweet_id,
            inbound_author_handle=author_handle,
            inbound_author_id=author_id,
            inbound_text=mention["text"],
            our_tweet_id=our_tweet_id,
            our_content_id=our_content["id"],
            our_post_text=our_content["content"],
            draft_text=draft,
            relationship_context=relationship_context,
            quality_score=quality_score,
            quality_flags=quality_flags,
        )

        print(f"  Draft: \"{draft[:80]}...\"" if len(draft) > 80 else f"  Draft: \"{draft}\"")
        drafted += 1

    # Update cursor
    if max_mention_id and max_mention_id != since_id:
        db.set_last_mention_id(max_mention_id)

    if bridge:
        bridge.close()
    db.close()
    print(f"\nDone. {drafted} drafted, {skipped} skipped.")


if __name__ == "__main__":
    main()
