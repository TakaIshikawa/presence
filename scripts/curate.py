#!/usr/bin/env python3
"""CLI for curating published content quality."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from storage.db import Database

VALID_FLAGS = ("good", "too_specific")


def cmd_list(db: Database, content_type: str = "x_post"):
    """Show recent published posts with curation status."""
    cursor = db.conn.execute(
        """SELECT id, content, eval_score, curation_quality, published_at
           FROM generated_content
           WHERE content_type = ? AND published = 1
           ORDER BY published_at DESC
           LIMIT 20""",
        (content_type,)
    )
    rows = cursor.fetchall()
    if not rows:
        print("No published posts found.")
        return

    for row in rows:
        flag = row["curation_quality"] or "-"
        preview = row["content"][:80].replace("\n", " ")
        score = row["eval_score"] or 0
        date = (row["published_at"] or "")[:10]
        print(f"  [{row['id']:>3}] [{flag:<13}] {score:.1f}  {date}  {preview}...")


def cmd_flag(db: Database, content_id: int, quality: str):
    """Flag a post with a curation quality label."""
    if quality not in VALID_FLAGS:
        print(f"Invalid flag '{quality}'. Use: {', '.join(VALID_FLAGS)}")
        sys.exit(1)

    # Verify the post exists
    row = db.conn.execute(
        "SELECT id, content FROM generated_content WHERE id = ?", (content_id,)
    ).fetchone()
    if not row:
        print(f"No content found with id {content_id}")
        sys.exit(1)

    db.set_curation_quality(content_id, quality)
    preview = row["content"][:80].replace("\n", " ")
    print(f"Flagged [{content_id}] as '{quality}': {preview}...")


def cmd_clear(db: Database, content_id: int):
    """Clear curation flag from a post."""
    db.set_curation_quality(content_id, None)
    print(f"Cleared curation flag for [{content_id}]")


def cmd_stats(db: Database):
    """Show curation statistics."""
    cursor = db.conn.execute(
        """SELECT
             COALESCE(curation_quality, 'unreviewed') AS quality,
             COUNT(*) AS count
           FROM generated_content
           WHERE published = 1
           GROUP BY curation_quality
           ORDER BY count DESC"""
    )
    print("Curation stats (published posts):")
    for row in cursor.fetchall():
        print(f"  {row['quality']:<15} {row['count']}")


def main():
    if len(sys.argv) < 2:
        print("Usage: curate.py {list|flag|clear|stats}")
        print("  list              Show recent published posts")
        print("  flag <id> <qual>  Flag a post (good, too_specific)")
        print("  clear <id>       Clear curation flag")
        print("  stats            Show curation statistics")
        sys.exit(1)

    project_root = Path(__file__).parent.parent
    db = Database(str(project_root / "presence.db"))
    db.connect()
    db.init_schema(str(project_root / "schema.sql"))

    cmd = sys.argv[1]
    if cmd == "list":
        cmd_list(db)
    elif cmd == "flag":
        if len(sys.argv) < 4:
            print("Usage: curate.py flag <id> <good|too_specific>")
            sys.exit(1)
        cmd_flag(db, int(sys.argv[2]), sys.argv[3])
    elif cmd == "clear":
        if len(sys.argv) < 3:
            print("Usage: curate.py clear <id>")
            sys.exit(1)
        cmd_clear(db, int(sys.argv[2]))
    elif cmd == "stats":
        cmd_stats(db)
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)

    db.close()


if __name__ == "__main__":
    main()
