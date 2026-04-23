#!/usr/bin/env python3
"""Inspect the current content mix and preview the next planner decision."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.content_mix import ContentMixPlanner  # noqa: E402

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--accumulated-tokens",
        type=int,
        default=0,
        help="Estimated source depth for the next run (default: 0)",
    )
    prompt_group = parser.add_mutually_exclusive_group()
    prompt_group.add_argument(
        "--has-prompts",
        dest="has_prompts",
        action="store_true",
        default=True,
        help="Treat the next run as having prompt context (default).",
    )
    prompt_group.add_argument(
        "--no-prompts",
        dest="has_prompts",
        action="store_false",
        help="Treat the next run as having no prompt context.",
    )
    parser.add_argument(
        "--recent-limit",
        type=int,
        default=6,
        help="How many recent published items to inspect for the mix snapshot (default: 6)",
    )
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON")
    return parser.parse_args(argv)


def build_report(db, accumulated_tokens: int, has_prompts: bool, recent_limit: int) -> dict:
    planner = ContentMixPlanner(db, recent_limit=recent_limit)
    snapshot = planner.snapshot()
    decision = planner.choose(
        accumulated_tokens=accumulated_tokens,
        has_prompts=has_prompts,
    )
    return {
        "inputs": {
            "accumulated_tokens": accumulated_tokens,
            "has_prompts": has_prompts,
        },
        "snapshot": asdict(snapshot),
        "decision": asdict(decision),
    }


def format_json_report(report: dict) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_text_report(report: dict) -> str:
    snapshot = report["snapshot"]
    decision = report["decision"]
    counts = snapshot["counts"]
    recent = snapshot["recent_content_types"]
    inputs = report["inputs"]
    lines = [
        "CONTENT MIX SNAPSHOT",
        f"- Recent window: last {snapshot['recent_limit']} published items",
        f"- Recent sequence: {', '.join(recent) if recent else 'none'}",
        (
            "- Counts: "
            f"x_post={counts['x_post']}, "
            f"x_thread={counts['x_thread']}, "
            f"x_visual={counts['x_visual']}, "
            f"blog_post={counts['blog_post']}"
        ),
        "",
        "PLANNER DECISION",
        f"- Input tokens: {inputs['accumulated_tokens']}",
        f"- Has prompts: {str(inputs['has_prompts']).lower()}",
        f"- Content type: {decision['content_type']}",
        f"- Reason: {decision['reason']}",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        report = build_report(
            db,
            accumulated_tokens=args.accumulated_tokens,
            has_prompts=args.has_prompts,
            recent_limit=args.recent_limit,
        )

    if args.json:
        print(format_json_report(report))
    else:
        print(format_text_report(report))


if __name__ == "__main__":
    main()
