#!/usr/bin/env python3
"""Generate campaign retrospective reports."""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.campaign_retrospective import (  # noqa: E402
    CampaignRetrospectiveGenerator,
    format_json_report,
    format_markdown_report,
)
from runner import script_context  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate an after-action report for a content campaign"
    )
    parser.add_argument("campaign_id", type=int, help="Content campaign ID to report")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON instead of markdown",
    )
    parser.add_argument(
        "--top-limit",
        type=int,
        default=5,
        help="Number of top content items to include (default: 5)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (config, db):
        report = CampaignRetrospectiveGenerator(db).build_report(
            campaign_id=args.campaign_id,
            top_limit=args.top_limit,
        )
        if args.json:
            print(format_json_report(report))
        else:
            print(format_markdown_report(report))


if __name__ == "__main__":
    main()
