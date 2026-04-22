#!/usr/bin/env python3
"""Fetch Buttondown newsletter engagement metrics for recent sent issues."""

import argparse
import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from output.newsletter import ButtondownClient

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Buttondown newsletter engagement and subscriber metrics."
    )
    parser.add_argument(
        "--subscribers",
        action="store_true",
        help="Fetch aggregate subscriber metrics instead of issue engagement metrics.",
    )
    return parser.parse_args(argv)


def _fetch_subscriber_metrics(client: ButtondownClient, db) -> bool:
    metrics = client.get_subscriber_metrics()
    if metrics is None:
        logger.warning("Failed to fetch newsletter subscriber metrics")
        return False

    db.insert_newsletter_subscriber_metrics(
        subscriber_count=metrics.subscriber_count,
        active_subscriber_count=metrics.active_subscriber_count,
        unsubscribes=metrics.unsubscribes,
        churn_rate=metrics.churn_rate,
        new_subscribers=metrics.new_subscribers,
        net_subscriber_change=metrics.net_subscriber_change,
        raw_metrics=metrics.raw_metrics,
    )
    logger.info(
        "Subscriber metrics: %s subscribers, %s active, %s unsubscribes",
        metrics.subscriber_count,
        metrics.active_subscriber_count,
        metrics.unsubscribes,
    )
    return True


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv or [])
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

        client = ButtondownClient(
            config.newsletter.api_key,
            timeout=config.timeouts.http_seconds,
        )

        if args.subscribers:
            fetched = _fetch_subscriber_metrics(client, db)
            if fetched:
                update_monitoring("fetch-newsletter-subscribers")
                logger.info("Done. Fetched newsletter subscriber metrics.")
            return

        sends = db.get_newsletter_sends_needing_metrics(max_age_days=90)
        if not sends:
            logger.info("No newsletter issues need metrics fetching")
            return

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
    main(sys.argv[1:])
