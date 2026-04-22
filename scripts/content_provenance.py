#!/usr/bin/env python3
"""Inspect provenance for one generated content item."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context


def _shorten(value: Any, width: int = 96) -> str:
    if value is None:
        return "-"
    text = str(value).replace("\n", " ")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _content_status(code: int | None) -> str:
    if code == 1:
        return "published"
    if code == -1:
        return "abandoned"
    return "unpublished"


def format_json_provenance(provenance: dict) -> str:
    """Format provenance as machine-readable JSON."""
    return json.dumps(provenance, indent=2, default=str)


def format_human_provenance(provenance: dict) -> str:
    """Format provenance as concise human-readable text."""
    content = provenance["content"]
    lines = [
        f"Content #{content['id']} ({content['content_type']}, {_content_status(content.get('published'))})",
        f"Generated: {content.get('created_at') or '-'}",
        f"Eval: {content.get('eval_score') if content.get('eval_score') is not None else '-'}",
    ]
    if content.get("content_format"):
        lines.append(f"Format: {content['content_format']}")
    if content.get("published_url"):
        lines.append(f"Published URL: {content['published_url']}")
    lines.append(f"Text: {_shorten(content.get('content'), 180)}")

    commits = provenance["source_commits"]
    lines.append(f"\nSource commits ({len(commits)})")
    if commits:
        for commit in commits:
            label = commit.get("commit_sha")
            if commit.get("matched"):
                label = f"{label} {commit.get('repo_name') or ''}".strip()
            lines.append(f"- {label}: {_shorten(commit.get('commit_message'), 90)}")
    else:
        lines.append("- none")

    messages = provenance["source_messages"]
    lines.append(f"\nClaude messages ({len(messages)})")
    if messages:
        for message in messages:
            prefix = message.get("message_uuid")
            timestamp = message.get("timestamp")
            if timestamp:
                prefix = f"{prefix} @ {timestamp}"
            lines.append(f"- {prefix}: {_shorten(message.get('prompt_text'), 90)}")
    else:
        lines.append("- none")

    links = provenance["knowledge_links"]
    lines.append(f"\nKnowledge links ({len(links)})")
    if links:
        for link in links:
            source = link.get("author") or link.get("source_type") or f"knowledge #{link.get('id')}"
            score = link.get("relevance_score")
            score_text = f"{score:.3f}" if isinstance(score, (int, float)) else "-"
            preview = link.get("insight") or link.get("content")
            lines.append(f"- {source} ({score_text}): {_shorten(preview, 90)}")
    else:
        lines.append("- none")

    variants = provenance["variants"]
    lines.append(f"\nVariants ({len(variants)})")
    if variants:
        for variant in variants:
            key = f"{variant.get('platform')}/{variant.get('variant_type')}"
            lines.append(f"- {key}: {_shorten(variant.get('content'), 90)}")
    else:
        lines.append("- none")

    publications = provenance["publications"]
    lines.append(f"\nPublications ({len(publications)})")
    if publications:
        for pub in publications:
            detail = pub.get("platform_post_id") or pub.get("platform_url") or pub.get("error") or "-"
            lines.append(
                f"- {pub.get('platform')}: {pub.get('status')} "
                f"(attempts {pub.get('attempt_count')}, published {pub.get('published_at') or '-'}) {detail}"
            )
    else:
        lines.append("- none")

    snapshots = provenance["engagement_snapshots"]
    lines.append(f"\nEngagement snapshots ({len(snapshots)})")
    if snapshots:
        for snapshot in snapshots:
            platform = snapshot["platform"]
            reposts = snapshot.get("retweet_count", snapshot.get("repost_count", 0))
            lines.append(
                f"- {platform} @ {snapshot.get('fetched_at')}: "
                f"likes {snapshot.get('like_count', 0)}, reposts {reposts}, "
                f"replies {snapshot.get('reply_count', 0)}, quotes {snapshot.get('quote_count', 0)}, "
                f"score {snapshot.get('engagement_score')}"
            )
    else:
        lines.append("- none")

    runs = provenance["pipeline_runs"]
    lines.append(f"\nPipeline runs ({len(runs)})")
    if runs:
        for run in runs:
            lines.append(
                f"- {run.get('batch_id')} @ {run.get('created_at')}: "
                f"outcome {run.get('outcome') or '-'}, final {run.get('final_score')}, "
                f"published {bool(run.get('published'))}"
            )
            if run.get("rejection_reason"):
                lines.append(f"  rejection: {_shorten(run['rejection_reason'], 100)}")
    else:
        lines.append("- none")

    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("content_id", type=int, help="generated_content.id to inspect")
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        provenance = db.get_content_provenance(args.content_id)

    if provenance is None:
        raise SystemExit(f"Content ID {args.content_id} not found")

    if args.json:
        print(format_json_provenance(provenance))
    else:
        print(format_human_provenance(provenance))


if __name__ == "__main__":
    main()
