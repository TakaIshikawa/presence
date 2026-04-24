#!/usr/bin/env python3
"""Export campaign retrospective reports."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.campaign_retrospective import (
    CampaignRetrospectiveExporter,
    CampaignRetrospectiveGenerator,
    format_json_report,
    format_markdown_report,
    format_markdown_retrospective,
    report_to_dict,
    retrospective_to_dict,
)
from runner import script_context


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("campaign_id", nargs="?", type=int)
    parser.add_argument("--campaign-id", dest="campaign_id_opt", type=int)
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--include-content", action="store_true")
    parser.add_argument("--top-limit", type=int, default=5)
    return parser.parse_args(argv)


def render_report(report, *, json_output: bool) -> str:
    if json_output:
        return json.dumps(retrospective_to_dict(report), indent=2, sort_keys=True)
    return format_markdown_retrospective(report)


def write_output(path: str | Path, body: str) -> Path:
    output_path = Path(path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(body, encoding="utf-8")
    return output_path


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    campaign_id = args.campaign_id_opt or args.campaign_id
    if campaign_id is None:
        raise SystemExit("A campaign id is required.")

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    legacy_mode = (
        args.campaign_id is not None
        and args.campaign_id_opt is None
        and not args.output
        and not args.include_content
    )

    with script_context() as (_config, db):
        if legacy_mode:
            report = CampaignRetrospectiveGenerator(db).build_report(
                campaign_id=campaign_id,
                top_limit=args.top_limit,
            )
            body = format_json_report(report) if args.json else format_markdown_report(report)
        else:
            report = CampaignRetrospectiveExporter(db).build(
                campaign_id,
                include_content=args.include_content,
                top_limit=args.top_limit,
            )
            if report is None:
                raise SystemExit(f"Campaign {campaign_id} not found")
            body = render_report(report, json_output=args.json)

    if args.output:
        output_path = write_output(args.output, body)
        print(str(output_path))
        return

    if legacy_mode and args.json:
        print(json.dumps(report_to_dict(report), indent=2, sort_keys=True))
        return

    print(body, end="" if body.endswith("\n") else "\n")


if __name__ == "__main__":
    main()
