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

    if args.format == "json":
        print(json.dumps(summary, indent=2))
    else:
        print(format_operations_health(summary))

    raise SystemExit(0 if summary["status"] == "ok" else 1)


if __name__ == "__main__":
    main()
