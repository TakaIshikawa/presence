#!/usr/bin/env python3
"""Report whether retrieved knowledge appears in generated outputs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.knowledge_citation_yield import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_UNUSED_GENERATIONS,
    build_knowledge_citation_yield_report,
    format_knowledge_citation_yield_json,
    format_knowledge_citation_yield_text,
)


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _load_rows(path: str | None) -> list[dict[str, object]]:
    if not path:
        return []
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError(f"{path} must contain a JSON array")
    return [dict(row) for row in data]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retrievals-json", required=True, help="JSON array of retrieval rows")
    parser.add_argument("--outputs-json", help="JSON array of generated output rows")
    parser.add_argument("--min-unused-generations", type=_positive_int, default=DEFAULT_MIN_UNUSED_GENERATIONS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        report = build_knowledge_citation_yield_report(
            _load_rows(args.retrievals_json),
            _load_rows(args.outputs_json),
            min_unused_generations=args.min_unused_generations,
            limit=args.limit,
        )
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_knowledge_citation_yield_text(report)
        if args.table or args.format == "text"
        else format_knowledge_citation_yield_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
