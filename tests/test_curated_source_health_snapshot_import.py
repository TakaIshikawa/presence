from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.curated_source_health_snapshot_import import parse_curated_source_health_snapshots, upsert_curated_source_health_snapshots

SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "import_curated_source_health_snapshots.py"
spec = importlib.util.spec_from_file_location("import_curated_source_health_snapshots_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_parse_upsert_and_cli(tmp_path, capsys):
    raw = '{"url":"HTTPS://Example.com/a/","status":"ok","checked_at":"2026-01-01T00:00:00+00:00","http_code":200}\n'
    rows = parse_curated_source_health_snapshots(raw)
    assert rows[0]["url"] == "https://example.com/a"
    conn = sqlite3.connect(":memory:")
    summary = upsert_curated_source_health_snapshots(conn, rows)
    assert summary["upserted_count"] == 1
    upsert_curated_source_health_snapshots(conn, [{**rows[0], "status": "failed"}])
    assert conn.execute("SELECT status FROM curated_source_health_snapshots").fetchone()[0] == "failed"
    path = tmp_path / "snapshots.jsonl"
    path.write_text(raw)
    db_path = tmp_path / "health.sqlite"
    assert script.main(["--db", str(db_path), "--input", str(path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["parsed_count"] == 1
    assert script.main(["--db", str(db_path), "--input", str(path), "--dry-run", "--format", "text"]) == 0
    assert "dry_run=True" in capsys.readouterr().out
