#!/usr/bin/env python3
"""Fetch Buttondown newsletter engagement metrics for recent sent issues."""

import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from output.newsletter import ButtondownClient

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (config, db):
        if not config.newsletter or not config.newsletter.enabled:
            logger.info("Newsletter not enabled, skipping metrics fetch")
            return

        if not config.newsletter.api_key:
            logger.warning("Newsletter API key not configured, skipping metrics fetch")
            return

        sends = db.get_newsletter_sends_needing_metrics(max_age_days=90)
        if not sends:
            logger.info("No newsletter issues need metrics fetching")
            return

        client = ButtondownClient(
            config.newsletter.api_key,
            timeout=config.timeouts.http_seconds,
        )

        fetched = 0
        for send in sends:
            metrics = client.get_email_analytics(send["issue_id"])
            if metrics is None:
                logger.warning("Failed to fetch metrics for issue %s", send["issue_id"])
                continue

            db.insert_newsletter_engagement(
                newsletter_send_id=send["id"],
                issue_id=metrics.issue_id,
                opens=metrics.opens,
                clicks=metrics.clicks,
                unsubscribes=metrics.unsubscribes,
            )
            fetched += 1
            logger.info(
                "  %s: %s opens, %s clicks, %s unsubscribes",
                metrics.issue_id,
                metrics.opens,
                metrics.clicks,
                metrics.unsubscribes,
            )

    update_monitoring("fetch-newsletter-metrics")
    logger.info("Done. Fetched newsletter metrics for %s issues.", fetched)


if __name__ == "__main__":
    main()
