#!/usr/bin/env python3
"""Auto-select the strongest stored variant for one content/platform pair."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.content_variant_selector import select_content_variant  # noqa: E402
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--content-id", type=int, required=True)
    parser.add_argument("--platform", required=True)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the selected variant to content_variants.selected",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    parser.add_argument(
        "--explain",
        action="store_true",
        help="Include ranked candidate score components in text output",
    )
    return parser.parse_args(argv)


def _print_text(result: dict) -> None:
    action = "Selected" if result["apply"] else "Would select"
    print(
        f"{action} variant {result['selected_variant_id']} "
        f"({result['selected_variant_type']}) for "
        f"content_id={result['content_id']} platform={result['platform']}."
    )
    if result["history_fallback"]:
        print(f"Historical engagement fallback: {result['history_source']}")


def _print_explanation(result: dict) -> None:
    print("")
    print("Ranked candidates:")
    for index, candidate in enumerate(result["candidates"], start=1):
        components = candidate["components"]
        history = candidate["historical"]
        print(
            f"{index}. id={candidate['id']} type={candidate['variant_type']} "
            f"score={candidate['score']:.4f} "
            f"history_avg={history['average_score']:.4f} "
            f"history_n={history['count']}"
        )
        print(
            "   components: "
            f"platform={components['platform_match']:.2f}, "
            f"type={components['variant_type']:.2f}, "
            f"selected={components['selected_state']:.2f}, "
            f"history={components['historical_engagement']:.2f}, "
            f"freshness={components['freshness']:.2f}"
        )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            result = select_content_variant(
                db,
                content_id=args.content_id,
                platform=args.platform,
                apply=args.apply,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_text(result)
        if args.explain:
            _print_explanation(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
