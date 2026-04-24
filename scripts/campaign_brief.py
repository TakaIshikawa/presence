#!/usr/bin/env python3
"""Generate read-only campaign briefs for upcoming planned topics."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.campaign_brief import CampaignBrief, CampaignBriefBuilder, brief_to_dict


def _shorten(text: str | None, width: int = 100) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)] + "..."


def format_json_brief(brief: CampaignBrief) -> str:
    """Format a campaign brief as JSON."""
    return json.dumps(brief_to_dict(brief), indent=2)


def format_markdown_brief(brief: CampaignBrief) -> str:
    """Format a campaign brief as readable Markdown."""
    campaign = brief.campaign or {}
    title = campaign.get("name") or "Upcoming Campaign Topics"
    lines = [
        f"# Campaign Brief: {title}",
        "",
        f"- Generated: {brief.generated_at}",
    ]
    if campaign:
        lines.extend(
            [
                f"- Campaign ID: {campaign['id']}",
                f"- Status: {campaign.get('status') or 'n/a'}",
                f"- Window: {campaign.get('start_date') or 'n/a'} to {campaign.get('end_date') or 'n/a'}",
            ]
        )
        if campaign.get("goal"):
            lines.append(f"- Goal: {campaign['goal']}")
    lines.append("")

    if not brief.topics:
        lines.append("No planned topics found.")
        return "\n".join(lines)

    for index, topic in enumerate(brief.topics, start=1):
        lines.extend(
            [
                f"## {index}. {topic.topic}",
                "",
                f"- Planned topic ID: {topic.planned_topic_id}",
                f"- Angle: {topic.angle or 'n/a'}",
                f"- Target date: {topic.target_date or 'unscheduled'}",
            ]
        )
        if topic.source_material:
            lines.append(f"- Source material: `{_shorten(topic.source_material, 120)}`")
        lines.append("")

        lines.append("### Supporting Evidence")
        if topic.evidence:
            for item in topic.evidence:
                lines.append(
                    f"- **{item.source_type}** {item.title}: {_shorten(item.excerpt)}"
                )
        else:
            lines.append("- none")
        lines.append("")

        lines.append("### Knowledge Snippets")
        if topic.knowledge_snippets:
            for item in topic.knowledge_snippets:
                source = f" ({item.url})" if item.url else ""
                lines.append(f"- **{item.title}**{source}: {_shorten(item.excerpt)}")
        else:
            lines.append("- none")
        lines.append("")

        lines.append("### Previous Related Posts")
        if topic.previous_related_posts:
            for item in topic.previous_related_posts:
                lines.append(f"- **{item.title}**: {_shorten(item.excerpt)}")
        else:
            lines.append("- none")
        lines.append("")

        lines.append("### Risks")
        if topic.risks:
            for risk in topic.risks:
                lines.append(f"- {risk}")
        else:
            lines.append("- none")
        lines.append("")

    return "\n".join(lines).rstrip()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--campaign-id",
        type=int,
        help="Campaign ID to brief. Defaults to the active campaign.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=3,
        help="Number of upcoming planned topics to include (default: 3)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON instead of Markdown",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the brief to this path instead of stdout",
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
        brief = CampaignBriefBuilder(db).build(
            campaign_id=args.campaign_id,
            limit=args.limit,
        )

    output = format_json_brief(brief) if args.json else format_markdown_brief(brief)
    if args.output:
        args.output.write_text(output + "\n", encoding="utf-8")
    else:
        print(output)


if __name__ == "__main__":
    main()
