#!/usr/bin/env python3
"""Assemble and send weekly newsletter via Buttondown."""

import sys
import subprocess
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from storage.db import Database
from output.newsletter import NewsletterAssembler, ButtondownClient


def main():
    config = load_config()

    if not config.newsletter or not config.newsletter.enabled:
        print("Newsletter not enabled, skipping")
        return

    if not config.newsletter.api_key:
        print("Newsletter API key not configured, skipping")
        return

    # Initialize components
    db = Database(config.paths.database)
    db.connect()
    db.init_schema(str(Path(__file__).parent.parent / "schema.sql"))

    # Idempotency: skip if already sent this week
    last_send = db.get_last_newsletter_send()
    if last_send and (datetime.now(timezone.utc) - last_send).days < 6:
        print(f"Newsletter already sent {(datetime.now(timezone.utc) - last_send).days} days ago, skipping")
        db.close()
        return

    # Compute week range (last 7 days)
    now = datetime.now(timezone.utc)
    week_end = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = week_end - timedelta(days=7)

    print(f"Assembling newsletter for {week_start.date()} to {week_end.date()}")

    # Assemble content
    assembler = NewsletterAssembler(db, site_url="https://takaishikawa.com")
    content = assembler.assemble(week_start, week_end)

    if not content.body_markdown.strip():
        print("No content published this week, skipping newsletter")
        db.close()
        return

    print(f"Subject: {content.subject}")
    print(f"Content IDs included: {content.source_content_ids}")

    # Check for --dry-run flag
    if "--dry-run" in sys.argv:
        print("\n--- DRY RUN (not sending) ---\n")
        print(content.body_markdown)
        db.close()
        return

    # Send via Buttondown
    client = ButtondownClient(config.newsletter.api_key)
    subscriber_count = client.get_subscriber_count()
    print(f"Subscribers: {subscriber_count}")

    result = client.send(content.subject, content.body_markdown)

    if result.success:
        db.insert_newsletter_send(
            issue_id=result.issue_id or "",
            subject=content.subject,
            content_ids=content.source_content_ids,
            subscriber_count=subscriber_count,
        )
        print(f"Newsletter sent: {result.url}")
    else:
        print(f"Send failed: {result.error}")

    db.close()
    _update_monitoring()
    print("Done")


def _update_monitoring():
    """Sync run state to operations.yaml for tact maintainer monitoring."""
    try:
        sync_script = Path(__file__).parent / "update_operations_state.py"
        if sync_script.exists():
            subprocess.run(
                [sys.executable, str(sync_script), "--operation", "send-newsletter"],
                check=False, capture_output=True,
            )
    except Exception:
        pass


if __name__ == "__main__":
    main()
