from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.content_persona_guard_publication_overrides import build_content_persona_guard_publication_overrides_report, build_content_persona_guard_publication_overrides_report_from_db


NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_persona_guard_publication_overrides.py"
spec = importlib.util.spec_from_file_location("content_persona_guard_publication_overrides_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_flags_guard_overrides():
    report = build_content_persona_guard_publication_overrides_report([
        {"content_id": 1, "publication_status": "published", "guard_checked": 1, "guard_passed": 0, "guard_status": "failed"},
        {"content_id": 2, "publication_status": "published", "guard_checked": 0, "guard_status": "unchecked"},
        {"content_id": 3, "publication_status": "queued", "guard_checked": 1, "guard_passed": 0, "guard_status": "failed"},
    ], now=NOW)
    assert report["artifact_type"] == "content_persona_guard_publication_overrides"
    assert report["totals"]["by_reason"] == {"failed_guard_published": 1, "unchecked_guard_published": 1, "failed_guard_queued": 1}


def test_db_loader_and_cli(tmp_path, capsys):
    path = tmp_path / "pg.sqlite"
    conn = sqlite3.connect(path)
    conn.executescript("""CREATE TABLE generated_content (id INTEGER PRIMARY KEY, published INTEGER);
    CREATE TABLE content_persona_guard (content_id INTEGER, checked INTEGER, passed INTEGER, status TEXT, score REAL, reasons TEXT);
    CREATE TABLE content_publications (content_id INTEGER, platform TEXT, status TEXT);
    CREATE TABLE content_variants (content_id INTEGER, selected INTEGER);
    INSERT INTO generated_content VALUES (1, 0);
    INSERT INTO content_persona_guard VALUES (1, 1, 0, 'failed', 0.2, 'tone');
    INSERT INTO content_publications VALUES (1, 'x', 'queued');""")
    conn.commit()
    assert build_content_persona_guard_publication_overrides_report_from_db(conn, now=NOW)["totals"]["by_reason"]["failed_guard_queued"] == 1
    assert script.main(["--db", str(path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "content_persona_guard_publication_overrides"
