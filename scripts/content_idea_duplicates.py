#!/usr/bin/env python3
"""Report duplicate clusters in the open content idea inbox."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.content_idea_duplicates import (
    ContentIdeaDuplicateCluster,
    clusters_to_dict,
    find_duplicate_clusters,
)


def _shorten(text: str | None, width: int = 76) -> str:
    value = " ".join(str(text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def format_json_report(clusters: list[ContentIdeaDuplicateCluster]) -> str:
    return json.dumps(clusters_to_dict(clusters), indent=2)


def format_text_report(clusters: list[ContentIdeaDuplicateCluster]) -> str:
    lines = [
        "",
        "=" * 70,
        "Content Idea Duplicate Clusters",
        "=" * 70,
        "",
        f"Clusters: {len(clusters)}",
    ]
    if not clusters:
        lines.extend(["", "- none", "", "=" * 70])
        return "\n".join(lines)

    for index, cluster in enumerate(clusters, start=1):
        lines.append("")
        lines.append(
            f"Cluster {index}: primary #{cluster.primary_idea_id}; "
            f"ideas {', '.join(f'#{idea_id}' for idea_id in cluster.idea_ids)}"
        )
        lines.append(
            f"  Reasons: {', '.join(cluster.reasons)}; "
            f"max similarity {cluster.max_similarity:.2f}"
        )
        if cluster.shared_source_identifiers:
            identifiers = ", ".join(
                f"{key}={value}"
                for key, value in cluster.shared_source_identifiers.items()
            )
            lines.append(f"  Shared source IDs: {identifiers}")
        for member in cluster.members:
            topic = f" [{member.topic}]" if member.topic else ""
            lines.append(
                f"  - #{member.id} {member.priority}{topic}: {_shorten(member.note)}"
            )

    lines.extend(["", "=" * 70])
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--min-similarity",
        type=float,
        default=0.86,
        help="Minimum lexical similarity for duplicate grouping (default: 0.86)",
    )
    parser.add_argument("--topic", help="Only compare open ideas in this topic")
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--include-low-priority",
        action="store_true",
        help="Include low-priority ideas in duplicate clustering",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        with script_context() as (_config, db):
            clusters = find_duplicate_clusters(
                db,
                min_similarity=args.min_similarity,
                topic=args.topic,
                include_low_priority=args.include_low_priority,
            )
    except ValueError as exc:
        parser.exit(1, f"error: {exc}\n")

    if args.format == "json":
        print(format_json_report(clusters))
    else:
        print(format_text_report(clusters))


if __name__ == "__main__":
    main()
