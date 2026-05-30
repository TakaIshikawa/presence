from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.knowledge_source_citation_freshness import (
    build_knowledge_source_citation_freshness_report,
    build_knowledge_source_citation_freshness_report_from_db,
    format_knowledge_source_citation_freshness_json,
    format_knowledge_source_citation_freshness_text,
)


NOW = datetime(2026, 5, 30, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_source_citation_freshness.py"
spec = importlib.util.spec_from_file_location("knowledge_source_citation_freshness_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_fresh_stale_undated_and_verification_findings():
    report = build_knowledge_source_citation_freshness_report(
        [
            {"source_id": "fresh", "source_url": "https://example.com/f", "published_at": "2026-05-01T00:00:00+00:00", "last_checked_at": "2026-05-25T00:00:00+00:00"},
            {"source_id": "stale", "source_url": "https://example.com/s", "published_at": "2025-01-01T00:00:00+00:00", "last_checked_at": "2026-05-20T00:00:00+00:00"},
            {"source_id": "undated", "source_url": "https://example.org/u", "last_checked_at": "2026-05-20T00:00:00+00:00"},
            {"source_id": "never", "source_url": "https://example.net/n", "published_at": "2026-04-01T00:00:00+00:00"},
            {"source_id": "oldcheck", "source_url": "https://example.net/o", "published_at": "2026-04-01T00:00:00+00:00", "last_checked_at": "2026-01-01T00:00:00+00:00"},
        ],
        now=NOW,
    )
    by_id = {item["source_id"]: item for item in report["findings"]}
    assert "fresh" not in by_id
    assert by_id["stale"]["issue_codes"] == ["stale_published_date"]
    assert by_id["undated"]["issue_codes"] == ["undated"]
    assert by_id["never"]["issue_codes"] == ["never_verified"]
    assert by_id["oldcheck"]["issue_codes"] == ["old_verification"]


def test_custom_thresholds_formatters_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE knowledge_sources (id TEXT, url TEXT, published_at TEXT, ingested_at TEXT, last_checked_at TEXT)")
    conn.execute("INSERT INTO knowledge_sources VALUES ('s1', 'https://docs.example.com/a', '2026-04-01T00:00:00+00:00', '2026-04-02T00:00:00+00:00', '2026-05-01T00:00:00+00:00')")
    conn.commit()
    report = build_knowledge_source_citation_freshness_report_from_db(
        conn,
        now=NOW,
        max_published_age_days=30,
        max_verification_age_days=20,
    )
    assert report["summary"]["issue_counts"] == {"old_verification": 1, "stale_published_date": 1}
    assert json.loads(format_knowledge_source_citation_freshness_json(report))["artifact_type"] == "knowledge_source_citation_freshness"
    assert "Knowledge Source Citation Freshness" in format_knowledge_source_citation_freshness_text(report)

    db_path = tmp_path / "ks.sqlite"
    disk = sqlite3.connect(db_path)
    conn.backup(disk)
    disk.close()
    assert script.main(["--db", str(db_path), "--format", "text", "--max-published-age-days", "30", "--max-verification-age-days", "20"]) == 0
    assert "stale_published_date" in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--limit", "0"]) == 2
