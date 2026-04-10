"""Shared script runner utilities."""

import logging
import sys
import subprocess
from pathlib import Path
from contextlib import contextmanager

from config import load_config
from storage.db import Database

PROJECT_ROOT = Path(__file__).parent.parent
SCHEMA_PATH = str(PROJECT_ROOT / "schema.sql")

logger = logging.getLogger(__name__)


@contextmanager
def script_context():
    """Context manager providing config + connected database."""
    config = load_config()
    db = Database(config.paths.database)
    db.connect()
    db.init_schema(SCHEMA_PATH)
    try:
        yield config, db
    finally:
        db.close()


def update_monitoring(operation: str):
    """Sync run state to operations.yaml for tact monitoring."""
    try:
        sync_script = PROJECT_ROOT / "scripts" / "update_operations_state.py"
        if sync_script.exists():
            subprocess.run(
                [sys.executable, str(sync_script), "--operation", operation],
                check=False,
                capture_output=True,
            )
    except Exception:
        logger.debug("update_monitoring failed for %s", operation, exc_info=True)
