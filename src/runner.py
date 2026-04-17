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
    try:
        yield config, db
    finally:
        db.close()


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
