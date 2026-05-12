"""Shared script runner utilities."""

import logging
from pathlib import Path
from contextlib import contextmanager
from collections.abc import Generator

from config import load_config, Config
from storage.db import Database

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent
SCHEMA_PATH = str(PROJECT_ROOT / "schema.sql")


@contextmanager
def script_context() -> Generator[tuple[Config, Database], None, None]:
    """Context manager providing config + connected database."""
    config = load_config()
    db = Database(config.paths.database)
    db.connect()
    db.init_schema(SCHEMA_PATH)
    _sync_curated_sources(config, db)
    try:
        yield config, db
    finally:
        db.close()


def _sync_curated_sources(config: Config, db: Database) -> None:
    """Sync config-driven curated sources into the DB (idempotent)."""
    if not config.curated_sources:
        return
    if config.curated_sources.x_accounts:
        db.sync_config_sources(
            [{"identifier": a.identifier, "name": a.name, "license": a.license, "feed_url": a.feed_url}
             for a in config.curated_sources.x_accounts],
            "x_account",
        )
    if config.curated_sources.blogs:
        db.sync_config_sources(
            [{"identifier": b.identifier, "name": b.name, "license": b.license, "feed_url": b.feed_url}
             for b in config.curated_sources.blogs],
            "blog",
        )
    if getattr(config.curated_sources, "newsletters", None):
        db.sync_config_sources(
            [{"identifier": n.identifier, "name": n.name, "license": n.license, "feed_url": n.feed_url}
             for n in config.curated_sources.newsletters],
            "newsletter",
        )


def update_monitoring(operation: str) -> None:
    """Compatibility hook for scripts that report completion."""
    logger.debug("Monitoring update skipped for operation '%s'", operation)
