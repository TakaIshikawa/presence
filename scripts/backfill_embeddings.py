#!/usr/bin/env python3
"""One-time backfill: embed published content for semantic dedup."""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from knowledge.embeddings import VoyageEmbeddings, serialize_embedding


def main():
    with script_context() as (config, db):
        if not config.embeddings:
            print("No embeddings config found, exiting")
            return

        embedder = VoyageEmbeddings(
            api_key=config.embeddings.api_key,
            model=config.embeddings.model,
        )

        # Find published content without embeddings
        cursor = db.conn.execute(
            """SELECT id, content FROM generated_content
               WHERE published = 1
               AND content_embedding IS NULL
               ORDER BY id"""
        )
        rows = cursor.fetchall()

        if not rows:
            print("No content to backfill")
            return

        print(f"Backfilling embeddings for {len(rows)} published posts...")

        # Process in batches of 20
        batch_size = 20
        total = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            texts = [dict(r)["content"] for r in batch]
            ids = [dict(r)["id"] for r in batch]

            try:
                embeddings = embedder.embed_batch(texts)
            except Exception as e:
                if "RateLimit" in type(e).__name__ or "429" in str(e):
                    print(f"  Rate limited at {total}/{len(rows)}, waiting 25s...")
                    time.sleep(25)
                    embeddings = embedder.embed_batch(texts)
                else:
                    raise

            for content_id, embedding in zip(ids, embeddings):
                db.set_content_embedding(content_id, serialize_embedding(embedding))
                total += 1

            print(f"  Embedded {total}/{len(rows)}")
            # Respect rate limits (3 RPM on free tier)
            if i + batch_size < len(rows):
                time.sleep(22)

        print(f"Done. Backfilled {total} embeddings.")


if __name__ == "__main__":
    main()
