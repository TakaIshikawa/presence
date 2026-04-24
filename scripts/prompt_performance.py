#!/usr/bin/env python3
"""Report prompt-version performance across evaluation and publishing signals."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.prompt_performance import (  # noqa: E402
    PromptPerformanceAnalyzer,
    format_prompt_performance_json,
    format_prompt_performance_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Look back this many days (default: 90)",
    )
    parser.add_argument(
        "--prompt-type",
        help="Restrict to one prompt type",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--min-runs",
        type=int,
        default=3,
        help="Minimum rows needed before ranking a prompt version (default: 3)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        report = PromptPerformanceAnalyzer(db).build_report(
            days=args.days,
            prompt_type=args.prompt_type,
            min_runs=args.min_runs,
        )

    if args.format == "json":
        print(format_prompt_performance_json(report))
    else:
        print(format_prompt_performance_text(report))


if __name__ == "__main__":
    main()
