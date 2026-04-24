#!/usr/bin/env python3
"""Report clustered content ideas for backlog review."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_idea_clusters import (
    ContentIdeaCluster,
    cluster_content_ideas,
    clusters_to_dicts,
)
from runner import script_context


def format_json_report(clusters: list[ContentIdeaCluster]) -> str:
    return json.dumps({"clusters": clusters_to_dicts(clusters)}, indent=2, sort_keys=True)


def format_text_report(clusters: list[ContentIdeaCluster]) -> str:
    lines = [
        "=" * 70,
        "Content Idea Clusters",
        "=" * 70,
    ]
    if not clusters:
        lines.append("No content idea clusters found.")
        return "\n".join(lines)

    for index, cluster in enumerate(clusters, start=1):
        priorities = ", ".join(
            f"{priority}: {count}" for priority, count in cluster.priority_mix.items()
        )
        sources = ", ".join(cluster.sources) if cluster.sources else "none"
        shared = ", ".join(cluster.shared_terms) if cluster.shared_terms else "none"
        lines.extend(
            [
                "",
                f"{index}. {cluster.label}",
                f"   Idea IDs: {', '.join(str(idea_id) for idea_id in cluster.idea_ids)}",
                f"   Representative note: {cluster.representative_note}",
                f"   Shared terms: {shared}",
                f"   Sources: {sources}",
                f"   Priority mix: {priorities or 'none'}",
            ]
        )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--status",
        default="open",
        help="Content idea status to include. Use 'all' for no status filter.",
    )
    parser.add_argument(
        "--min-cluster-size",
        type=int,
        default=1,
        help="Hide clusters smaller than this size (default: 1)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum content ideas to read before clustering (default: 100)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    status = None if args.status == "all" else args.status

    with script_context() as (_config, db):
        ideas = db.get_content_ideas(status=status, limit=args.limit)

    clusters = cluster_content_ideas(ideas, min_cluster_size=args.min_cluster_size)
    if args.format == "json":
        print(format_json_report(clusters))
    else:
        print(format_text_report(clusters))


if __name__ == "__main__":
    main()
