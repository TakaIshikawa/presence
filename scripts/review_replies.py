#!/usr/bin/env python3
"""Interactive review of pending reply drafts."""

import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from storage.db import Database
from output.x_client import XClient
from review_helpers import truncate, read_char, format_relationship_context


def main():
    config = load_config()

    db = Database(config.paths.database)
    db.connect()
    db.init_schema(str(Path(__file__).parent.parent / "schema.sql"))

    pending = db.get_pending_replies()
    if not pending:
        print("No pending reply drafts.")
        db.close()
        return

    print(f"\n{len(pending)} pending reply draft{'s' if len(pending) != 1 else ''}\n")

    x_client = XClient(
        config.x.api_key,
        config.x.api_secret,
        config.x.access_token,
        config.x.access_token_secret
    )

    quit_requested = False
    posted = 0

    for i, reply in enumerate(pending):
        if quit_requested:
            break

        print(f"{'─' * 60}")
        header = f"{i + 1}/{len(pending)}  @{reply['inbound_author_handle']} replied to your post"

        # Relationship context (from cultivate enrichment stored at poll time)
        ctx_line = format_relationship_context(reply["relationship_context"])
        if ctx_line:
            header += f"\n     [{ctx_line}]"

        # Quality score
        quality_line = _format_quality_line(reply["quality_score"], reply["quality_flags"])
        if quality_line:
            header += f"\n     [{quality_line}]"

        print(header)
        print()
        print(f"  Your post:   \"{truncate(reply['our_post_text'], 120)}\"")
        print(f"  Their reply: \"{truncate(reply['inbound_text'], 120)}\"")
        print(f"  Draft:       \"{truncate(reply['draft_text'], 120)}\"")
        print()

        while True:
            sys.stdout.write("  [a]pprove  [e]dit  [d]ismiss  [s]kip  [q]uit > ")
            sys.stdout.flush()

            choice = read_char().lower()
            print(choice)

            if choice == "q":
                quit_requested = True
                break

            elif choice == "a":
                result = x_client.reply(
                    reply["draft_text"],
                    reply["inbound_tweet_id"]
                )
                if result.success:
                    db.update_reply_status(
                        reply["id"], "posted",
                        posted_tweet_id=result.tweet_id
                    )
                    print(f"  Posted: {result.url}")
                    posted += 1
                else:
                    print(f"  Error: {result.error}")
                break

            elif choice == "e":
                edited = input("  Your reply: ").strip()
                if not edited:
                    print("  Empty, cancelled.")
                    continue
                if len(edited) > 280:
                    print(f"  Too long ({len(edited)} chars, max 280). Try again.")
                    continue
                result = x_client.reply(edited, reply["inbound_tweet_id"])
                if result.success:
                    db.update_reply_status(
                        reply["id"], "posted",
                        posted_tweet_id=result.tweet_id
                    )
                    print(f"  Posted: {result.url}")
                    posted += 1
                else:
                    print(f"  Error: {result.error}")
                break

            elif choice == "d":
                db.update_reply_status(reply["id"], "dismissed")
                print("  Dismissed.")
                break

            else:  # skip or any other key
                print("  Skipped.")
                break

    print(f"\nDone. {posted} replies posted.")
    db.close()


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
