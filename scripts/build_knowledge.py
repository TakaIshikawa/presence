#!/usr/bin/env python3
"""Build knowledge base from existing content."""

import logging
import sqlite3
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Rate limiting: seconds between API calls (Voyage free tier: 3 RPM)
API_DELAY_SECONDS = 25

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from storage.db import Database
from ingestion.claude_logs import ClaudeLogParser
from knowledge.embeddings import get_embedding_provider, EmbeddingError
from knowledge.store import KnowledgeStore
from knowledge.ingest import InsightExtractor, InsightExtractionError, ingest_own_post, ingest_own_conversation


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger(__name__)

    config = load_config()

    if not config.embeddings:
        logger.error("embeddings not configured in config.yaml")
        logger.error("Add embeddings section with provider, model, and api_key")
        sys.exit(1)

    # Initialize
    db = Database(config.paths.database)
    db.connect()
    db.init_schema(str(Path(__file__).parent.parent / "schema.sql"))

    logger.info(f"Using embedding provider: {config.embeddings.provider}")
    embedder = get_embedding_provider(
        config.embeddings.provider,
        config.embeddings.api_key,
        config.embeddings.model
    )

    store = KnowledgeStore(db.conn, embedder)
    extractor = InsightExtractor(config.anthropic.api_key, config.synthesis.model)

    # 1. Ingest own X posts from generated_content table
    logger.info("=== Ingesting own X posts ===")
    cursor = db.conn.execute(
        """SELECT id, content, published_url FROM generated_content
           WHERE content_type = 'x_post' AND published = 1"""
    )
    posts = cursor.fetchall()
    logger.info(f"Found {len(posts)} published posts")

    for i, post in enumerate(posts):
        post_id = str(post["id"])
        if store.exists("own_post", post_id):
            logger.debug(f"Skipping post {post_id} (already exists)")
            continue

        logger.info(f"Processing post {post_id}...")
        try:
            ingest_own_post(
                store=store,
                extractor=extractor,
                post_id=post_id,
                content=post["content"],
                url=post["published_url"] or "",
                author=config.github.username
            )
            logger.info(f"Ingested post {post_id}")
            time.sleep(API_DELAY_SECONDS)  # Rate limiting
        except (InsightExtractionError, EmbeddingError, sqlite3.Error) as e:
            logger.error(f"Failed to ingest post {post_id}: {e}")
            time.sleep(API_DELAY_SECONDS)  # Still wait on error

    # 2. Ingest Claude Code conversations (last 30 days)
    logger.info("=== Ingesting Claude Code conversations ===")
    parser = ClaudeLogParser(
        config.paths.claude_logs,
        config.paths.allowed_projects,
        redaction_patterns=config.privacy.redaction_patterns,
    )
    since = datetime.now(timezone.utc) - timedelta(days=30)

    conversations = list(parser.get_messages_since(since))
    parser.log_skipped_project_counts("build_knowledge")
    logger.info(f"Found {len(conversations)} messages in last 30 days")

    # Filter to substantial prompts
    substantial = [c for c in conversations if len(c.prompt_text) >= 100]
    logger.info(f"{len(substantial)} are substantial (100+ chars)")

    ingested = 0
    for i, msg in enumerate(substantial[:50]):  # Limit to 50 to stay within rate limits
        if store.exists("own_conversation", msg.message_uuid):
            logger.debug(f"Skipping conversation {msg.message_uuid[:8]} (already exists)")
            continue

        logger.info(f"Processing conversation {msg.message_uuid[:8]} ({i+1}/{min(50, len(substantial))})")
        try:
            result = ingest_own_conversation(
                store=store,
                extractor=extractor,
                message_uuid=msg.message_uuid,
                prompt=msg.prompt_text,
                project_path=msg.project_path
            )
            if result:
                logger.info(f"Ingested conversation {msg.message_uuid[:8]}")
                ingested += 1
            time.sleep(API_DELAY_SECONDS)  # Rate limiting
        except (InsightExtractionError, EmbeddingError, sqlite3.Error) as e:
            logger.error(f"Failed to ingest conversation {msg.message_uuid[:8]}: {e}")
            time.sleep(API_DELAY_SECONDS)  # Still wait on error

    logger.info(f"Ingested {ingested} new conversations")

    db.close()
    logger.info("=== Knowledge base built ===")


if __name__ == "__main__":
    main()
