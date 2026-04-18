#!/usr/bin/env python3
"""One-time backfill: embed published content for semantic dedup."""

import sys
import time
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from knowledge.embeddings import VoyageEmbeddings, serialize_embedding, EmbeddingRateLimitError

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    with script_context() as (config, db):
        if not config.embeddings:
            logger.warning("No embeddings config found, exiting")
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
            logger.info("No content to backfill")
            return

        logger.info(f"Backfilling embeddings for {len(rows)} published posts...")

        # Process in batches of 20
        batch_size = 20
        total = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            texts = [dict(r)["content"] for r in batch]
            ids = [dict(r)["id"] for r in batch]

            try:
                embeddings = embedder.embed_batch(texts)
            except EmbeddingRateLimitError as e:
                logger.warning(f"Rate limited at {total}/{len(rows)}, waiting 25s...")
                time.sleep(25)
                embeddings = embedder.embed_batch(texts)

            for content_id, embedding in zip(ids, embeddings):
                db.set_content_embedding(content_id, serialize_embedding(embedding))
                total += 1

            logger.info(f"Embedded {total}/{len(rows)}")
            # Respect rate limits (3 RPM on free tier)
            if i + batch_size < len(rows):
                time.sleep(22)

        logger.info(f"Done. Backfilled {total} embeddings.")


if __name__ == "__main__":
    main()
