from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.knowledge_source_author_credential_gaps import build_knowledge_source_author_credential_gaps_report


NOW = datetime(2026, 5, 20, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_source_author_credential_gaps.py"
spec = importlib.util.spec_from_file_location("knowledge_source_author_credential_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_missing_stale_clean_and_uncited_sources(tmp_path, capsys):
    report = build_knowledge_source_author_credential_gaps_report(
        [
            {"source_id": "missing-author", "canonical_url": "https://a.test", "citation_count": 3, "last_cited_at": NOW.isoformat(), "author_credential": "editor", "author_affiliation": "Desk"},
            {"source_id": "missing-credential", "canonical_url": "https://b.test", "citation_count": 1, "last_cited_at": NOW.isoformat(), "author_name": "A", "author_affiliation": "Desk"},
            {"source_id": "stale", "canonical_url": "https://c.test", "citation_count": 2, "last_cited_at": NOW.isoformat(), "author_name": "B", "author_affiliation": "Desk", "author_credential": "Reporter", "credential_updated_at": "2024-01-01T00:00:00+00:00"},
            {"source_id": "clean", "canonical_url": "https://d.test", "citation_count": 4, "last_cited_at": NOW.isoformat(), "author_name": "C", "author_affiliation": "Desk", "author_credential": "Analyst", "credential_updated_at": NOW.isoformat()},
            {"source_id": "uncited", "canonical_url": "https://e.test", "citation_count": 0},
        ],
        now=NOW,
    )
    rows = report["rows"]
    assert {key for row in rows for key in row} >= {"source_id", "canonical_url", "author_name", "missing_fields", "citation_count", "last_cited_at", "priority_score"}
    assert {row["source_id"] for row in rows} == {"missing-author", "missing-credential", "stale"}
    assert next(row for row in rows if row["source_id"] == "stale")["missing_fields"] == ["stale_credential"]

    db_path = tmp_path / "sources.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE knowledge_sources (id TEXT, canonical_url TEXT, author_name TEXT, author_affiliation TEXT, author_credential TEXT, citation_count INTEGER, last_cited_at TEXT)")
    conn.execute("INSERT INTO knowledge_sources VALUES ('s1','https://x.test','','','',5,?)", (NOW.isoformat(),))
    conn.commit()
    assert script.main(["--db", str(db_path), "--format", "json", "--now", NOW.isoformat()]) == 0
    assert json.loads(capsys.readouterr().out)["rows"][0]["source_id"] == "s1"


def test_validation_errors():
    with pytest.raises(ValueError, match="stale_days must be positive"):
        build_knowledge_source_author_credential_gaps_report([], stale_days=0)
