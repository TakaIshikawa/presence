#!/usr/bin/env python3
"""Track and analyze which knowledge items contributed to generated posts.

Enables attribution analysis and knowledge ROI measurement by showing:
- Which knowledge items are most frequently used
- Which sources drive highest-engagement content
- Full lineage for any specific post
- Knowledge items that were ingested but never used
"""

import argparse
import json
import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context

logger = logging.getLogger(__name__)


def cmd_report(db, days: int, output_json: bool):
    """Print knowledge usage stats: most-used items, highest-engagement sources, unused knowledge."""
    print(f"\n=== Knowledge Usage Report (last {days} days) ===\n")

    # Most-used knowledge items
    usage_stats = db.get_knowledge_usage_stats(days=days)
    if usage_stats:
        print("Most-used knowledge items:")
        print(f"{'ID':<6} {'Source':<15} {'Author':<20} {'Uses':<6} {'Avg Rel':<10} {'Avg Eng':<10}")
        print("-" * 77)
        for item in usage_stats[:15]:
            content_preview = (item['content'] or '')[:30].replace('\n', ' ')
            print(
                f"{item['id']:<6} {item['source_type']:<15} {(item['author'] or 'N/A'):<20} "
                f"{item['usage_count']:<6} {item['avg_relevance']:<10.3f} {item['avg_engagement']:<10.2f}"
            )
            print(f"       Preview: {content_preview}")
        print()
    else:
        print("No knowledge items used in the period.\n")

    # Highest-engagement sources
    valuable_sources = db.get_most_valuable_sources(days=days, min_uses=3)
    if valuable_sources:
        print("Highest-engagement sources (min 3 uses):")
        print(f"{'Source Type':<20} {'Author':<25} {'Uses':<6} {'Avg Engagement':<15}")
        print("-" * 66)
        for src in valuable_sources[:10]:
            print(
                f"{src['source_type']:<20} {src['author']:<25} "
                f"{src['usage_count']:<6} {src['avg_engagement']:<15.2f}"
            )
        print()
    else:
        print("No sources with sufficient usage found.\n")

    # Unused knowledge
    unused = db.get_unused_knowledge(days=days)
    if unused:
        print(f"Unused knowledge items ({len(unused)} total):")
        for item in unused[:10]:
            content_preview = (item['content'] or '')[:60].replace('\n', ' ')
            print(f"  ID {item['id']} [{item['source_type']}] @{item['author']}: {content_preview}")
        if len(unused) > 10:
            print(f"  ... and {len(unused) - 10} more")
        print()
    else:
        print("All knowledge items have been used.\n")

    if output_json:
        report_data = {
            "usage_stats": usage_stats,
            "valuable_sources": valuable_sources,
            "unused_knowledge": unused,
        }
        print("\n=== JSON Output ===")
        print(json.dumps(report_data, indent=2))


def cmd_trace(db, content_id: int, output_json: bool):
    """Show full lineage for a specific post: which knowledge items contributed."""
    print(f"\n=== Content Lineage for ID {content_id} ===\n")

    # Get content details
    cursor = db.conn.execute(
        "SELECT content, content_type, eval_score, published FROM generated_content WHERE id = ?",
        (content_id,)
    )
    row = cursor.fetchone()
    if not row:
        print(f"Error: Content ID {content_id} not found")
        return

    content_text = row[0]
    content_type = row[1]
    eval_score = row[2]
    published = row[3]

    print(f"Content Type: {content_type}")
    print(f"Eval Score: {eval_score:.1f}/10")
    print(f"Published: {'Yes' if published == 1 else 'No'}")
    print(f"\nContent Preview:\n{content_text[:200]}...\n")

    # Get knowledge lineage
    lineage = db.get_content_lineage(content_id)
    if not lineage:
        print("No knowledge items linked to this content.")
        if output_json:
            print("\n=== JSON Output ===")
            print(json.dumps({
                "content_id": content_id,
                "content_type": content_type,
                "eval_score": eval_score,
                "published": published == 1,
                "lineage": []
            }, indent=2))
        return

    print(f"Knowledge items used ({len(lineage)} total):\n")
    print(f"{'ID':<6} {'Source':<15} {'Author':<20} {'Relevance':<12} {'Attribution':<12}")
    print("-" * 65)
    for item in lineage:
        print(
            f"{item['id']:<6} {item['source_type']:<15} {(item['author'] or 'N/A'):<20} "
            f"{item['relevance_score']:<12.3f} {'Required' if item['attribution_required'] else 'Not required':<12}"
        )
        if item['insight']:
            print(f"       Insight: {item['insight'][:80]}")
        else:
            content_preview = (item['content'] or '')[:80].replace('\n', ' ')
            print(f"       Content: {content_preview}")
        if item['source_url']:
            print(f"       URL: {item['source_url']}")
        if item.get("canonical_url") and item["canonical_url"] != item.get("source_url"):
            print(f"       Canonical: {item['canonical_url']}")
        if item.get("link_title"):
            print(f"       Title: {item['link_title']}")
        print()

    if output_json:
        print("\n=== JSON Output ===")
        print(json.dumps({
            "content_id": content_id,
            "content_type": content_type,
            "eval_score": eval_score,
            "published": published == 1,
            "lineage": lineage
        }, indent=2, default=str))


def cmd_roi(db, days: int, output_json: bool):
    """Show source ROI: average engagement of posts using each source vs. those that didn't."""
    print(f"\n=== Knowledge Source ROI (last {days} days) ===\n")

    # Get all published content with engagement in the period
    cursor = db.conn.execute(
        """SELECT gc.id, COALESCE(pe.engagement_score, 0) AS engagement
           FROM generated_content gc
           LEFT JOIN (
               SELECT content_id, engagement_score,
                      ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY fetched_at DESC) AS rn
               FROM post_engagement
           ) pe ON pe.content_id = gc.id AND pe.rn = 1
           WHERE gc.published = 1
             AND gc.published_at >= datetime('now', ?)""",
        (f'-{days} days',)
    )
    all_content = {row[0]: row[1] for row in cursor.fetchall()}

    if not all_content:
        print("No published content in the period.")
        return

    # Get content IDs that used knowledge
    cursor = db.conn.execute(
        """SELECT DISTINCT content_id
           FROM content_knowledge_links ckl
           INNER JOIN generated_content gc ON gc.id = ckl.content_id
           WHERE gc.published_at >= datetime('now', ?)""",
        (f'-{days} days',)
    )
    content_with_knowledge = {row[0] for row in cursor.fetchall()}

    # Calculate engagement with vs without knowledge
    with_knowledge = [eng for cid, eng in all_content.items() if cid in content_with_knowledge]
    without_knowledge = [eng for cid, eng in all_content.items() if cid not in content_with_knowledge]

    avg_with = sum(with_knowledge) / len(with_knowledge) if with_knowledge else 0
    avg_without = sum(without_knowledge) / len(without_knowledge) if without_knowledge else 0

    print("Overall Impact:")
    print(f"  Posts using knowledge: {len(with_knowledge)} (avg engagement: {avg_with:.2f})")
    print(f"  Posts without knowledge: {len(without_knowledge)} (avg engagement: {avg_without:.2f})")
    print(f"  Lift: {(avg_with - avg_without):.2f} ({((avg_with / avg_without - 1) * 100) if avg_without > 0 else 0:.1f}%)\n")

    # Per-source ROI
    valuable_sources = db.get_most_valuable_sources(days=days, min_uses=1)
    if valuable_sources:
        print("Per-source ROI:")
        print(f"{'Source Type':<20} {'Author':<25} {'Uses':<6} {'Avg Eng':<12} {'vs Overall':<12}")
        print("-" * 75)
        overall_avg = sum(all_content.values()) / len(all_content) if all_content else 0
        for src in valuable_sources[:15]:
            lift = src['avg_engagement'] - overall_avg
            lift_pct = (lift / overall_avg * 100) if overall_avg > 0 else 0
            print(
                f"{src['source_type']:<20} {src['author']:<25} "
                f"{src['usage_count']:<6} {src['avg_engagement']:<12.2f} "
                f"{lift:+.2f} ({lift_pct:+.1f}%)"
            )
        print()

    if output_json:
        roi_data = {
            "overall": {
                "with_knowledge_count": len(with_knowledge),
                "with_knowledge_avg": avg_with,
                "without_knowledge_count": len(without_knowledge),
                "without_knowledge_avg": avg_without,
                "lift": avg_with - avg_without,
            },
            "per_source": valuable_sources
        }
        print("\n=== JSON Output ===")
        print(json.dumps(roi_data, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=["report", "trace", "roi"],
        help="Command to run: report (usage stats), trace (content lineage), roi (source ROI)"
    )
    parser.add_argument(
        "content_id",
        nargs="?",
        type=int,
        help="Content ID for trace command"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Days to look back (default: 30)"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.command == "trace" and args.content_id is None:
        parser.error("trace command requires content_id")

    with script_context() as (config, db):
        if args.command == "report":
            cmd_report(db, args.days, args.json)
        elif args.command == "trace":
            cmd_trace(db, args.content_id, args.json)
        elif args.command == "roi":
            cmd_roi(db, args.days, args.json)


if __name__ == "__main__":
    main()
