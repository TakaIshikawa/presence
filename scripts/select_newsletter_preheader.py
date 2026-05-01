#!/usr/bin/env python3
"""Select a deterministic newsletter preheader from a draft payload."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_preheader import (  # noqa: E402
    DEFAULT_MAX_LENGTH,
    DEFAULT_MIN_LENGTH,
    format_preheader_selection_json,
    format_preheader_selection_text,
    select_newsletter_preheader,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input",
        nargs="?",
        help=(
            "Draft payload file. JSON files are parsed as structured payloads; "
            "other files are markdown."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--min-length",
        type=int,
        default=DEFAULT_MIN_LENGTH,
        help=f"Minimum selected preheader length (default: {DEFAULT_MIN_LENGTH}).",
    )
    parser.add_argument(
        "--max-length",
        type=int,
        default=DEFAULT_MAX_LENGTH,
        help=f"Maximum selected preheader length (default: {DEFAULT_MAX_LENGTH}).",
    )
    return parser.parse_args(argv)


def load_payload(path_value: str | None) -> dict | str:
    """Read a structured JSON payload or markdown draft from path/stdin."""
    if path_value:
        path = Path(path_value)
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ValueError(f"could not read input file: {exc}") from exc
        if path.suffix.lower() == ".json":
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError(f"malformed JSON input: {exc}") from exc
            if not isinstance(payload, dict):
                raise ValueError("JSON input must be an object")
            return payload
        return raw

    raw = sys.stdin.read()
    if not raw.strip():
        raise ValueError("no input provided")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    if not isinstance(payload, dict):
        raise ValueError("JSON input must be an object")
    return payload


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        payload = load_payload(args.input)
        selection = select_newsletter_preheader(
            payload,
            min_length=args.min_length,
            max_length=args.max_length,
        )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_preheader_selection_json(selection), end="")
    else:
        print(format_preheader_selection_text(selection))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
