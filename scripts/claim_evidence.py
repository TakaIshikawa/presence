#!/usr/bin/env python3
"""Export claim evidence artifacts for generated content."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.claim_evidence import (
    SUPPORTED_STATUSES,
    format_claim_evidence_json,
    format_claim_evidence_markdown,
    load_claim_evidence_export,
)


SUPPORTED_FORMATS = {"json", "markdown"}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--content-id",
        type=int,
        help="generated_content.id to export; omit for a status-filtered multi-row export",
    )
    parser.add_argument(
        "--status",
        choices=sorted(SUPPORTED_STATUSES),
        default="all",
        help="Filter exported rows by claim-check status",
    )
    parser.add_argument(
        "--output",
        help="Write output to this path instead of stdout",
    )
    parser.add_argument(
        "--format",
        choices=sorted(SUPPORTED_FORMATS),
        default="json",
        help="Output format",
    )
    return parser.parse_args(argv)


def _render(payload: object, output_format: str) -> str:
    if output_format == "markdown":
        return format_claim_evidence_markdown(payload)  # type: ignore[arg-type]
    return format_claim_evidence_json(payload)  # type: ignore[arg-type]


def _write_text(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        try:
            payload = load_claim_evidence_export(
                db,
                content_id=args.content_id,
                status=args.status,
            )
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc

    body = _render(payload, args.format)
    if args.output:
        output_path = Path(args.output).expanduser()
        _write_text(output_path, body)
        print(f"Exported claim evidence to {output_path}", file=sys.stderr)
    else:
        print(body)


if __name__ == "__main__":
    main()
