#!/usr/bin/env python3
"""CLI for curating published content quality."""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context

VALID_FLAGS = ("good", "too_specific")
VALID_FEEDBACK = ("reject", "revise", "prefer")

logger = logging.getLogger(__name__)


def cmd_list(db, content_type: str = "x_post"):
    """Show recent published posts with curation status."""
    cursor = db.conn.execute(
        """SELECT id, content, eval_score, curation_quality, auto_quality, published_at
           FROM generated_content
           WHERE content_type = ? AND published = 1
           ORDER BY published_at DESC
           LIMIT 20""",
        (content_type,)
    )
    rows = cursor.fetchall()
    if not rows:
        logger.info("No published posts found")
        return

    for row in rows:
        manual = row["curation_quality"] or "-"
        auto = row["auto_quality"] or "-"
        preview = row["content"][:70].replace("\n", " ")
        score = row["eval_score"] or 0
        date = (row["published_at"] or "")[:10]
        logger.info(f"  [{row['id']:>3}] m:{manual:<13} a:{auto:<14} {score:.1f}  {date}  {preview}...")


def cmd_flag(db, content_id: int, quality: str):
    """Flag a post with a curation quality label."""
    if quality not in VALID_FLAGS:
        logger.error(f"Invalid flag '{quality}'. Use: {', '.join(VALID_FLAGS)}")
        sys.exit(1)

    # Verify the post exists
    row = db.conn.execute(
        "SELECT id, content FROM generated_content WHERE id = ?", (content_id,)
    ).fetchone()
    if not row:
        logger.error(f"No content found with id {content_id}")
        sys.exit(1)

    db.set_curation_quality(content_id, quality)
    preview = row["content"][:80].replace("\n", " ")
    logger.info(f"Flagged [{content_id}] as '{quality}': {preview}...")


def cmd_clear(db, content_id: int):
    """Clear curation flag from a post."""
    db.set_curation_quality(content_id, None)
    logger.info(f"Cleared curation flag for [{content_id}]")


def cmd_feedback(
    db,
    content_id: int,
    feedback_type: str,
    notes: str = "",
    replacement_text: str | None = None,
):
    """Record durable user feedback about generated content."""
    if feedback_type not in VALID_FEEDBACK:
        logger.error(
            f"Invalid feedback '{feedback_type}'. Use: {', '.join(VALID_FEEDBACK)}"
        )
        sys.exit(1)

    row = db.conn.execute(
        "SELECT id, content FROM generated_content WHERE id = ?", (content_id,)
    ).fetchone()
    if not row:
        logger.error(f"No content found with id {content_id}")
        sys.exit(1)

    feedback_id = db.add_content_feedback(
        content_id=content_id,
        feedback_type=feedback_type,
        notes=notes,
        replacement_text=replacement_text,
    )
    preview = row["content"][:80].replace("\n", " ")
    suffix = f" replacement: {replacement_text[:80]}" if replacement_text else ""
    logger.info(
        f"Recorded feedback [{feedback_id}] {feedback_type} for [{content_id}]: "
        f"{notes}{suffix} ({preview}...)"
    )


def cmd_stats(db):
    """Show curation statistics."""
    # Manual curation
    cursor = db.conn.execute(
        """SELECT
             COALESCE(curation_quality, 'unreviewed') AS quality,
             COUNT(*) AS count
           FROM generated_content
           WHERE published = 1
           GROUP BY curation_quality
           ORDER BY count DESC"""
    )
    logger.info("Manual curation (published posts):")
    for row in cursor.fetchall():
        logger.info(f"  {row['quality']:<15} {row['count']}")

    # Auto-classification
    cursor = db.conn.execute(
        """SELECT
             COALESCE(auto_quality, 'pending') AS quality,
             COUNT(*) AS count
           FROM generated_content
           WHERE published = 1
           GROUP BY auto_quality
           ORDER BY count DESC"""
    )
    logger.info("\nAuto-classification (engagement-based):")
    for row in cursor.fetchall():
        logger.info(f"  {row['quality']:<15} {row['count']}")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    if len(sys.argv) < 2:
        logger.error("Usage: curate.py {list|flag|clear|feedback|stats}")
        logger.error("  list              Show recent published posts")
        logger.error("  flag <id> <qual>  Flag a post (good, too_specific)")
        logger.error("  clear <id>       Clear curation flag")
        logger.error("  feedback <id> <reject|revise|prefer> <notes> [replacement]")
        logger.error("  stats            Show curation statistics")
        sys.exit(1)

    with script_context() as (config, db):
        cmd = sys.argv[1]
        if cmd == "list":
            cmd_list(db)
        elif cmd == "flag":
            if len(sys.argv) < 4:
                logger.error("Usage: curate.py flag <id> <good|too_specific>")
                sys.exit(1)
            cmd_flag(db, int(sys.argv[2]), sys.argv[3])
        elif cmd == "clear":
            if len(sys.argv) < 3:
                logger.error("Usage: curate.py clear <id>")
                sys.exit(1)
            cmd_clear(db, int(sys.argv[2]))
        elif cmd == "feedback":
            if len(sys.argv) < 5:
                logger.error(
                    "Usage: curate.py feedback <id> <reject|revise|prefer> <notes> [replacement]"
                )
                sys.exit(1)
            replacement_text = sys.argv[5] if len(sys.argv) > 5 else None
            cmd_feedback(
                db,
                int(sys.argv[2]),
                sys.argv[3],
                sys.argv[4],
                replacement_text,
            )
        elif cmd == "stats":
            cmd_stats(db)
        else:
            logger.error(f"Unknown command: {cmd}")
            sys.exit(1)


if __name__ == "__main__":
    main()
