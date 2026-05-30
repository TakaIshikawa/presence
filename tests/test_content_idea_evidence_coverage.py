from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.content_idea_evidence_coverage import (
    build_content_idea_evidence_coverage_report,
    build_content_idea_evidence_coverage_report_from_db,
    format_content_idea_evidence_coverage_json,
    format_content_idea_evidence_coverage_text,
)


NOW = datetime(2026, 5, 30, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "content_idea_evidence_coverage.py"
spec = importlib.util.spec_from_file_location("content_idea_evidence_coverage_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_no_single_stale_missing_artifact_and_sufficient_evidence():
    rows = [
        {"idea_id": "none", "idea_text": "No evidence"},
        {"idea_id": "single", "idea_text": "One source", "source_url": "https://a.example/post", "evidence_type": "source", "evidence_at": "2026-05-01T00:00:00+00:00"},
        {"idea_id": "stale", "idea_text": "Old source", "source_url": "https://a.example/old", "evidence_type": "artifact", "evidence_at": "2025-01-01T00:00:00+00:00", "author_experience": "1"},
        {"idea_id": "good", "idea_text": "Ready", "source_url": "https://a.example/a", "evidence_type": "artifact", "evidence_at": "2026-05-01T00:00:00+00:00", "author_experience": "1"},
        {"idea_id": "good", "idea_text": "Ready", "source_url": "https://b.example/b", "evidence_type": "source", "evidence_at": "2026-05-02T00:00:00+00:00"},
    ]
    report = build_content_idea_evidence_coverage_report(rows, now=NOW)
    by_id = {item["idea_id"]: item for item in report["findings"]}
    assert by_id["none"]["issue_codes"] == ["no_linked_sources", "missing_author_experience_artifact"]
    assert "weak_source_diversity" in by_id["single"]["issue_codes"]
    assert "missing_author_experience_artifact" in by_id["single"]["issue_codes"]
    assert "stale_evidence" in by_id["stale"]["issue_codes"]
    assert "good" not in by_id


def test_custom_threshold_db_adapter_formatters_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE content_ideas (id TEXT, title TEXT)")
    conn.execute("CREATE TABLE content_idea_evidence (idea_id TEXT, source_url TEXT, evidence_type TEXT, evidence_at TEXT, author_experience TEXT)")
    conn.execute("INSERT INTO content_ideas VALUES ('i1', 'Idea')")
    conn.execute("INSERT INTO content_idea_evidence VALUES ('i1', 'https://a.example/a', 'artifact', '2026-05-01T00:00:00+00:00', '1')")
    conn.commit()
    report = build_content_idea_evidence_coverage_report_from_db(conn, now=NOW, min_sources=2)
    assert report["findings"][0]["issue_codes"] == ["insufficient_sources", "weak_source_diversity"]
    assert json.loads(format_content_idea_evidence_coverage_json(report))["artifact_type"] == "content_idea_evidence_coverage"
    assert "Content Idea Evidence Coverage" in format_content_idea_evidence_coverage_text(report)

    db_path = tmp_path / "ideas.sqlite"
    disk = sqlite3.connect(db_path)
    conn.backup(disk)
    disk.close()
    assert script.main(["--db", str(db_path), "--format", "text", "--min-sources", "2"]) == 0
    assert "weak_source_diversity" in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--limit", "0"]) == 2
