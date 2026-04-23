#!/usr/bin/env python3
"""Backfill published content embeddings for semantic dedup."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from knowledge.embeddings import (
    EmbeddingRateLimitError,
    VoyageEmbeddings,
    serialize_embedding,
)

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 20
RATE_LIMIT_RETRY_SLEEP_SECONDS = 25
BETWEEN_BATCH_SLEEP_SECONDS = 22
META_KEY_PREFIX = "backfill_embeddings:last_id"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--content-type",
        help="Only backfill rows for this generated content type",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of rows to embed in this run",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from the last stored checkpoint in the meta table",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be embedded without calling the embedder or writing to the DB",
    )
    return parser.parse_args(argv if argv is not None else [])


def _checkpoint_key(content_type: str | None) -> str:
    return f"{META_KEY_PREFIX}:{content_type or 'all'}"


def _load_checkpoint(db, content_type: str | None) -> int | None:
    raw_value = db.get_meta(_checkpoint_key(content_type))
    if not raw_value:
        return None
    try:
        payload = json.loads(raw_value)
        if isinstance(payload, dict) and "last_id" in payload:
            return int(payload["last_id"])
        return int(raw_value)
    except (TypeError, ValueError, json.JSONDecodeError):
        logger.warning("Ignoring invalid checkpoint value for %s", _checkpoint_key(content_type))
        return None


def _save_checkpoint(db, content_type: str | None, last_id: int) -> None:
    db.set_meta(_checkpoint_key(content_type), json.dumps({"last_id": last_id}))


def _fetch_rows(db, content_type: str | None, resume_from: int | None, limit: int | None) -> list[dict]:
    query = [
        "SELECT id, content",
        "FROM generated_content",
        "WHERE published = 1",
        "AND content_embedding IS NULL",
    ]
    params: list[object] = []

    if content_type:
        query.append("AND content_type = ?")
        params.append(content_type)

    if resume_from is not None:
        query.append("AND id > ?")
        params.append(resume_from)

    query.append("ORDER BY id")

    if limit is not None:
        query.append("LIMIT ?")
        params.append(limit)

    cursor = db.conn.execute("\n".join(query), params)
    return [dict(row) for row in cursor.fetchall()]


def _select_rows_for_run(
    db,
    content_type: str | None,
    resume: bool,
    limit: int | None,
) -> tuple[list[dict], int | None]:
    checkpoint = _load_checkpoint(db, content_type) if resume else None
    rows = _fetch_rows(db, content_type, checkpoint, limit)
    return rows, checkpoint


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv or [])
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (config, db):
        if not config.embeddings:
            logger.warning("No embeddings config found, exiting")
            return

        rows, checkpoint = _select_rows_for_run(
            db,
            content_type=args.content_type,
            resume=args.resume,
            limit=args.limit,
        )

        if not rows:
            if args.resume and checkpoint is not None:
                logger.info(
                    "No content to backfill for checkpoint %s%s",
                    checkpoint,
                    f" and content type {args.content_type}" if args.content_type else "",
                )
            else:
                logger.info("No content to backfill")
            return

        logger.info(
            "Backfilling embeddings for %s published posts%s%s%s",
            len(rows),
            f" of type {args.content_type}" if args.content_type else "",
            f" starting after id {checkpoint}" if args.resume and checkpoint is not None else "",
            " (dry-run)" if args.dry_run else "",
        )

        if args.dry_run:
            logger.info(
                "Dry-run: would embed ids %s",
                [row["id"] for row in rows],
            )
            return

        embedder = VoyageEmbeddings(
            api_key=config.embeddings.api_key,
            model=config.embeddings.model,
        )

        batch_size = DEFAULT_BATCH_SIZE
        total = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            texts = [row["content"] for row in batch]
            ids = [row["id"] for row in batch]

            try:
                embeddings = embedder.embed_batch(texts)
            except EmbeddingRateLimitError:
                logger.warning(
                    "Rate limited at %s/%s, waiting %ss...",
                    total,
                    len(rows),
                    RATE_LIMIT_RETRY_SLEEP_SECONDS,
                )
                time.sleep(RATE_LIMIT_RETRY_SLEEP_SECONDS)
                embeddings = embedder.embed_batch(texts)

            for content_id, embedding in zip(ids, embeddings):
                db.set_content_embedding(content_id, serialize_embedding(embedding))
                total += 1

            if args.resume:
                _save_checkpoint(db, args.content_type, ids[-1])

            logger.info("Embedded %s/%s", total, len(rows))

            if i + batch_size < len(rows):
                time.sleep(BETWEEN_BATCH_SLEEP_SECONDS)

        logger.info("Done. Backfilled %s embeddings.", total)


if __name__ == "__main__":
    main(sys.argv[1:])
