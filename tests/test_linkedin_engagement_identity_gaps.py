from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.linkedin_engagement_identity_gaps import build_linkedin_engagement_identity_gaps_report, build_linkedin_engagement_identity_gaps_report_from_db


NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "linkedin_engagement_identity_gaps.py"
spec = importlib.util.spec_from_file_location("linkedin_engagement_identity_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_flags_identity_gaps():
    report = build_linkedin_engagement_identity_gaps_report([
        {"id": 1, "content_id": 1, "resolved_content_id": None, "linkedin_url": "", "post_id": ""},
        {"id": 2, "content_id": 2, "resolved_content_id": 2, "linkedin_url": "u", "post_id": "a", "publication_url": "other"},
        {"id": 3, "content_id": 3, "resolved_content_id": 3, "linkedin_url": "u", "post_id": ""},
    ], now=NOW)
    assert report["artifact_type"] == "linkedin_engagement_identity_gaps"
    assert report["totals"]["by_reason"] == {"missing_content": 1, "missing_platform_identity": 1, "publication_identity_conflict": 1, "duplicate_identity": 1}


def test_db_loader_handles_optional_publications_and_cli(tmp_path, capsys):
    path = tmp_path / "li.sqlite"
    conn = sqlite3.connect(path)
    conn.executescript("""CREATE TABLE generated_content (id INTEGER PRIMARY KEY);
    CREATE TABLE linkedin_engagement (content_id INTEGER, linkedin_url TEXT, post_id TEXT);
    INSERT INTO generated_content VALUES (1);
    INSERT INTO linkedin_engagement VALUES (1, NULL, NULL);""")
    conn.commit()
    assert build_linkedin_engagement_identity_gaps_report_from_db(conn, now=NOW)["totals"]["by_reason"]["missing_platform_identity"] == 1
    assert build_linkedin_engagement_identity_gaps_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"] == ["generated_content", "linkedin_engagement"]
    assert script.main(["--db", str(path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "linkedin_engagement_identity_gaps"
    assert script.main(["--limit", "0"]) == 2
