#!/usr/bin/env python3
"""Plan the next newsletter call to action."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_cta import (  # noqa: E402
    CtaCandidate,
    fetch_recent_newsletter_sends,
    plan_newsletter_cta,
    selection_to_json,
    selection_to_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "candidates",
        help="YAML or JSON file containing CTA candidates.",
    )
    parser.add_argument(
        "--campaign-tag",
        action="append",
        default=[],
        help="Campaign tag to prefer; may be provided more than once.",
    )
    parser.add_argument(
        "--recent-limit",
        type=int,
        default=10,
        help="Number of recent newsletter sends to inspect (default: 10).",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def load_candidates(path: str | Path) -> list[CtaCandidate]:
    """Load CTA candidates from a YAML or JSON file."""
    candidate_path = Path(path)
    try:
        raw_text = candidate_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"could not read candidate file: {exc}") from exc

    try:
        if candidate_path.suffix.lower() == ".json":
            data = json.loads(raw_text)
        else:
            data = yaml.safe_load(raw_text)
    except (json.JSONDecodeError, yaml.YAMLError) as exc:
        raise ValueError(f"invalid candidate file: {exc}") from exc

    raw_candidates = _candidate_entries(data)
    return [CtaCandidate.from_mapping(item) for item in raw_candidates]


def render_selection(args: argparse.Namespace) -> str:
    candidates = load_candidates(args.candidates)
    with script_context() as (_config, db):
        recent_sends = fetch_recent_newsletter_sends(db, limit=args.recent_limit)
    selection = plan_newsletter_cta(
        candidates,
        recent_sends=recent_sends,
        campaign_tags=args.campaign_tag,
    )
    if args.format == "json":
        return selection_to_json(selection) + "\n"
    return selection_to_text(selection) + "\n"


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        print(render_selection(args), end="")
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


def _candidate_entries(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        entries = data
    elif isinstance(data, dict) and isinstance(data.get("candidates"), list):
        entries = data["candidates"]
    elif isinstance(data, dict) and isinstance(data.get("ctas"), list):
        entries = data["ctas"]
    else:
        raise ValueError("candidate file must contain a list, candidates, or ctas")

    if not entries:
        raise ValueError("candidate file must include at least one CTA candidate")
    if not all(isinstance(item, dict) for item in entries):
        raise ValueError("CTA candidate entries must be objects")
    return entries


if __name__ == "__main__":
    raise SystemExit(main())
