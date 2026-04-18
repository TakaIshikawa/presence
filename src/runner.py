"""Shared script runner utilities."""

import sys
import logging
import subprocess
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
            [{"identifier": a.identifier, "name": a.name, "license": a.license}
             for a in config.curated_sources.x_accounts],
            "x_account",
        )
    if config.curated_sources.blogs:
        db.sync_config_sources(
            [{"identifier": b.identifier, "name": b.name, "license": b.license}
             for b in config.curated_sources.blogs],
            "blog",
        )


def update_monitoring(operation: str) -> None:
    """Sync run state to operations.yaml for tact monitoring."""
    try:
        sync_script = PROJECT_ROOT / "scripts" / "update_operations_state.py"
        if sync_script.exists():
            subprocess.run(
                [sys.executable, str(sync_script), "--operation", operation],
                check=False,
                capture_output=True,
            )
    except (OSError, subprocess.SubprocessError, ValueError) as e:
        logger.debug(f"Failed to update monitoring for operation '{operation}': {e}")
