#!/usr/bin/env python3
"""Assemble and send weekly newsletter via Buttondown."""

import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from output.newsletter import (
    NewsletterAssembler,
    NewsletterSubjectCandidate,
    ButtondownClient,
)


def _arg_value(name: str) -> str:
    """Read a simple --name value CLI option without changing existing argv usage."""
    if name not in sys.argv:
        return ""
    index = sys.argv.index(name)
    if index + 1 >= len(sys.argv):
        return ""
    return sys.argv[index + 1].strip()


def _config_text(obj, *names: str) -> str:
    """Return the first configured string value from a possibly mocked config."""
    for name in names:
        value = getattr(obj, name, "")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _manual_subject_override(config) -> str:
    """Resolve an explicit subject override from CLI or newsletter config."""
    return (
        _arg_value("--subject")
        or _config_text(
            config.newsletter,
            "subject_override",
            "manual_subject",
            "subject",
        )
    )


def _select_subject(content, manual_subject: str = "") -> str:
    """Pick the outgoing subject while letting manual overrides win."""
    if manual_subject:
        return manual_subject
    candidates = getattr(content, "subject_candidates", None) or []
    if candidates:
        return candidates[0].subject
    return content.subject


def _subject_candidates_for_storage(content, selected_subject: str, manual_subject: str):
    """Include manual overrides in persisted candidates without dropping evaluated ones."""
    candidates = list(getattr(content, "subject_candidates", None) or [])
    if manual_subject and all(c.subject != manual_subject for c in candidates):
        top_score = max((c.score for c in candidates), default=0.0)
        candidates.insert(
            0,
            NewsletterSubjectCandidate(
                subject=manual_subject,
                score=round(top_score + 0.01, 2),
                rationale="manual override",
                source="manual",
                metadata={"manual_override": True},
            ),
        )
    return candidates


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    with script_context() as (config, db):
        if not config.newsletter or not config.newsletter.enabled:
            logger.info("Newsletter not enabled, skipping")
            return

        if not config.newsletter.api_key:
            logger.warning("Newsletter API key not configured, skipping")
            return

        # Idempotency: skip if already sent this week
        last_send = db.get_last_newsletter_send()
        if last_send and (datetime.now(timezone.utc) - last_send).days < 6:
            logger.info(f"Newsletter already sent {(datetime.now(timezone.utc) - last_send).days} days ago, skipping")
            return

        # Compute week range (last 7 days)
        now = datetime.now(timezone.utc)
        week_end = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = week_end - timedelta(days=7)

        logger.info(f"Assembling newsletter for {week_start.date()} to {week_end.date()}")

        # Assemble content
        assembler = NewsletterAssembler(
            db,
            site_url="https://takaishikawa.com",
            utm_source=getattr(config.newsletter, "utm_source", ""),
            utm_medium=getattr(config.newsletter, "utm_medium", ""),
            utm_campaign_template=getattr(
                config.newsletter, "utm_campaign_template", ""
            ),
        )
        content = assembler.assemble(week_start, week_end)

        if not content.body_markdown.strip():
            logger.info("No content published this week, skipping newsletter")
            return

        manual_subject = _manual_subject_override(config)
        selected_subject = _select_subject(content, manual_subject)
        logger.info(f"Subject: {selected_subject}")
        logger.debug(f"Content IDs included: {content.source_content_ids}")

        # Check for --dry-run flag
        if "--dry-run" in sys.argv:
            logger.info("\n--- DRY RUN (not sending) ---\n")
            logger.info(content.body_markdown)
            return

        # Send via Buttondown
        client = ButtondownClient(config.newsletter.api_key, timeout=config.timeouts.http_seconds)
        subscriber_count = client.get_subscriber_count()
        logger.info(f"Subscribers: {subscriber_count}")

        candidates_for_storage = _subject_candidates_for_storage(
            content,
            selected_subject=selected_subject,
            manual_subject=manual_subject,
        )
        inserter = getattr(db, "insert_newsletter_subject_candidates", None)
        if callable(inserter) and candidates_for_storage:
            try:
                inserter(
                    candidates_for_storage,
                    content_ids=content.source_content_ids,
                    week_start=week_start,
                    week_end=week_end,
                    selected_subject=selected_subject,
                )
            except Exception as e:
                logger.debug("Newsletter subject candidate storage failed: %s", e)

        result = client.send(selected_subject, content.body_markdown)

        if result.success:
            send_kwargs = {
                "issue_id": result.issue_id or "",
                "subject": selected_subject,
                "content_ids": content.source_content_ids,
                "subscriber_count": subscriber_count,
            }
            if content.metadata:
                send_kwargs["metadata"] = content.metadata
            db.insert_newsletter_send(**send_kwargs)
            logger.info(f"Newsletter sent: {result.url}")
        else:
            logger.error(f"Send failed: {result.error}")

    update_monitoring("send-newsletter")
    logger.info("Done")


if __name__ == "__main__":
    main()
