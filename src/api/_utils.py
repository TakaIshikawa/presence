from __future__ import annotations
import sqlite3
from datetime import datetime, timedelta
from typing import Any

def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try: return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error: return set()
def has_table(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None
def missing_schema(artifact_type: str, missing: list[str]) -> dict[str, Any]:
    return {"artifact_type": artifact_type, "status": "missing_schema", "missing": missing, "summary": {}}
def iso_days_ago(days: int) -> str:
    return (datetime.utcnow() - timedelta(days=days)).replace(microsecond=0).isoformat() + "Z"
def rowdict(row: sqlite3.Row | tuple, names: list[str] | None = None) -> dict[str, Any]:
    return dict(row) if isinstance(row, sqlite3.Row) else dict(zip(names or [], row))
