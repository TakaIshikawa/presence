#!/usr/bin/env python3
"""Assemble and send weekly newsletter via Buttondown."""

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from output.newsletter import NewsletterAssembler, ButtondownClient


def main():
    with script_context() as (config, db):
        if not config.newsletter or not config.newsletter.enabled:
            print("Newsletter not enabled, skipping")
            return

        if not config.newsletter.api_key:
            print("Newsletter API key not configured, skipping")
            return

        # Idempotency: skip if already sent this week
        last_send = db.get_last_newsletter_send()
        if last_send and (datetime.now(timezone.utc) - last_send).days < 6:
            print(f"Newsletter already sent {(datetime.now(timezone.utc) - last_send).days} days ago, skipping")
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
            return

        print(f"Subject: {content.subject}")
        print(f"Content IDs included: {content.source_content_ids}")

        # Check for --dry-run flag
        if "--dry-run" in sys.argv:
            print("\n--- DRY RUN (not sending) ---\n")
            print(content.body_markdown)
            return

        # Send via Buttondown
        client = ButtondownClient(config.newsletter.api_key, timeout=config.timeouts.http_seconds)
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

    update_monitoring("send-newsletter")
    print("Done")


if __name__ == "__main__":
    main()
