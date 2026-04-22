#!/usr/bin/env python3
"""Emit operational health summaries for background automation."""

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.operations_health import (
    format_operations_health,
    summarize_operations_health,
    thresholds_from_config,
)
from runner import script_context
from update_operations_state import deliver_operations_alerts, webhook_config_from_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Summarize operational health across polling, replies, queue, pipeline, and engagement fetches."
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--webhook-dry-run",
        action="store_true",
        help="Build and print the webhook payload without posting or updating dedupe state",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (config, db):
        summary = summarize_operations_health(
            db,
            thresholds=thresholds_from_config(config),
        )
        webhook_result = deliver_operations_alerts(
            db.conn,
            summary,
            webhook_config_from_config(config),
            source="operations_health",
            http_timeout=config.timeouts.http_seconds,
            dry_run=args.webhook_dry_run,
        )

    if args.format == "json":
        output = {"summary": summary}
        if args.webhook_dry_run:
            output["webhook"] = webhook_result
        print(json.dumps(output if args.webhook_dry_run else summary, indent=2))
    else:
        print(format_operations_health(summary))
        if args.webhook_dry_run:
            print(json.dumps(webhook_result["payload"] or {}, indent=2))

    raise SystemExit(0 if summary["status"] == "ok" else 1)


if __name__ == "__main__":
    main()
