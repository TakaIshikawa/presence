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
        context_parts = []
        if reply["relationship_context"]:
            try:
                ctx = json.loads(reply["relationship_context"])
                if ctx.get("engagement_stage") is not None:
                    context_parts.append(f"{ctx.get('stage_name', '?')} (stage {ctx['engagement_stage']})")
                if ctx.get("dunbar_tier") is not None:
                    context_parts.append(f"{ctx.get('tier_name', '?')} (tier {ctx['dunbar_tier']})")
                if ctx.get("relationship_strength") is not None:
                    context_parts.append(f"strength: {ctx['relationship_strength']:.2f}")
            except (json.JSONDecodeError, KeyError):
                pass
        if context_parts:
            header += f"\n     [{' | '.join(context_parts)}]"

        # Quality score
        if reply["quality_score"] is not None:
            score = reply["quality_score"]
            flags = []
            if reply["quality_flags"]:
                try:
                    flags = json.loads(reply["quality_flags"])
                except json.JSONDecodeError:
                    pass
            quality_str = f"Quality: {score:.1f}/10"
            if flags:
                quality_str += f" ⚠ {', '.join(flags)}"
            header += f"\n     [{quality_str}]"

        print(header)
        print()
        print(f"  Your post:   \"{_truncate(reply['our_post_text'], 120)}\"")
        print(f"  Their reply: \"{_truncate(reply['inbound_text'], 120)}\"")
        print(f"  Draft:       \"{_truncate(reply['draft_text'], 120)}\"")
        print()

        while True:
            sys.stdout.write("  [a]pprove  [e]dit  [d]ismiss  [s]kip  [q]uit > ")
            sys.stdout.flush()

            choice = _read_char().lower()
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


def _truncate(text: str, max_len: int) -> str:
    """Truncate text with ellipsis."""
    if not text:
        return ""
    if len(text) <= max_len:
        return text
    return text[:max_len - 3] + "..."


def _read_char() -> str:
    """Read a single character from stdin without requiring Enter."""
    try:
        import tty
        import termios
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        return ch
    except (ImportError, termios.error):
        # Fallback for non-terminal environments
        return input().strip()[:1] if True else ""


if __name__ == "__main__":
    main()
