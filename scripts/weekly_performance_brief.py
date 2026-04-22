#!/usr/bin/env python3
"""Generate a weekly performance brief artifact."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.performance_brief import (
    PerformanceBrief,
    PerformanceBriefBuilder,
    brief_to_dict,
    format_markdown_brief,
    parse_week_start,
)
from runner import script_context


def format_json_brief(brief: PerformanceBrief) -> str:
    """Format a weekly brief as machine-readable JSON."""
    return json.dumps(brief_to_dict(brief), indent=2)


def artifact_path(output_dir: str | Path, week_start: str, mode: str) -> Path:
    """Return the deterministic artifact path for a brief mode."""
    suffix = "json" if mode == "json" else "md"
    return Path(output_dir) / f"weekly_performance_brief_{week_start}.{suffix}"


def write_artifact(
    brief: PerformanceBrief,
    output_dir: str | Path,
    mode: str,
) -> Path:
    """Write a JSON or Markdown brief artifact and return the path."""
    path = artifact_path(output_dir, brief.week_start, mode)
    path.parent.mkdir(parents=True, exist_ok=True)
    if mode == "json":
        path.write_text(format_json_brief(brief) + "\n", encoding="utf-8")
    else:
        path.write_text(format_markdown_brief(brief), encoding="utf-8")
    return path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--week-start",
        help="ISO date for the Monday/week start to report, for example 2026-04-20",
    )
    parser.add_argument(
        "--output-dir",
        help="Directory to write the artifact. If omitted, output is printed.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--json",
        action="store_true",
        help="Emit a JSON brief",
    )
    mode.add_argument(
        "--markdown",
        action="store_true",
        help="Emit a Markdown brief (default)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    week_start = parse_week_start(args.week_start)
    mode = "json" if args.json else "markdown"

    with script_context() as (_config, db):
        brief = PerformanceBriefBuilder(db).build(week_start)

    if args.output_dir:
        path = write_artifact(brief, args.output_dir, mode)
        print(str(path))
        return

    if mode == "json":
        print(format_json_brief(brief))
    else:
        print(format_markdown_brief(brief), end="")


if __name__ == "__main__":
    main()
