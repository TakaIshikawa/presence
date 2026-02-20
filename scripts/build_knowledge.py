#!/usr/bin/env python3
"""Build knowledge base from existing content."""

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from storage.db import Database
from ingestion.claude_logs import ClaudeLogParser
from knowledge.embeddings import get_embedding_provider
from knowledge.store import KnowledgeStore
from knowledge.ingest import InsightExtractor, ingest_own_post, ingest_own_conversation


def main():
    config = load_config()

    if not config.embeddings:
        print("Error: embeddings not configured in config.yaml")
        print("Add embeddings section with provider, model, and api_key")
        sys.exit(1)

    # Initialize
    db = Database(config.paths.database)
    db.connect()
    db.init_schema(str(Path(__file__).parent.parent / "schema.sql"))

    print(f"Using embedding provider: {config.embeddings.provider}")
    embedder = get_embedding_provider(
        config.embeddings.provider,
        config.embeddings.api_key,
        config.embeddings.model
    )

    store = KnowledgeStore(db.conn, embedder)
    extractor = InsightExtractor(config.anthropic.api_key, config.synthesis.model)

    # 1. Ingest own X posts from generated_content table
    print("\n=== Ingesting own X posts ===")
    cursor = db.conn.execute(
        """SELECT id, content, published_url FROM generated_content
           WHERE content_type = 'x_post' AND published = 1"""
    )
    posts = cursor.fetchall()
    print(f"Found {len(posts)} published posts")

    for post in posts:
        post_id = str(post["id"])
        if store.exists("own_post", post_id):
            continue

        print(f"  Processing post {post_id}...")
        try:
            ingest_own_post(
                store=store,
                extractor=extractor,
                post_id=post_id,
                content=post["content"],
                url=post["published_url"] or "",
                author=config.github.username
            )
            print(f"    ✓ Ingested")
        except Exception as e:
            print(f"    ✗ Error: {e}")

    # 2. Ingest Claude Code conversations (last 30 days)
    print("\n=== Ingesting Claude Code conversations ===")
    parser = ClaudeLogParser(config.paths.claude_logs)
    since = datetime.now(timezone.utc) - timedelta(days=30)

    conversations = list(parser.get_messages_since(since))
    print(f"Found {len(conversations)} messages in last 30 days")

    # Filter to substantial prompts
    substantial = [c for c in conversations if len(c.prompt_text) >= 100]
    print(f"  {len(substantial)} are substantial (100+ chars)")

    ingested = 0
    for msg in substantial[:100]:  # Limit to avoid too many API calls
        if store.exists("own_conversation", msg.message_uuid):
            continue

        print(f"  Processing conversation {msg.message_uuid[:8]}...")
        try:
            result = ingest_own_conversation(
                store=store,
                extractor=extractor,
                message_uuid=msg.message_uuid,
                prompt=msg.prompt_text,
                project_path=msg.project_path
            )
            if result:
                print(f"    ✓ Ingested")
                ingested += 1
        except Exception as e:
            print(f"    ✗ Error: {e}")

    print(f"\nIngested {ingested} new conversations")

    db.close()
    print("\n=== Knowledge base built ===")


if __name__ == "__main__":
    main()
