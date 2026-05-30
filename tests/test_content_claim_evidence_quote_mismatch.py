from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.content_claim_evidence_quote_mismatch import build_content_claim_evidence_quote_mismatch_report_from_db, format_content_claim_evidence_quote_mismatch_json, format_content_claim_evidence_quote_mismatch_text

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "content_claim_evidence_quote_mismatch.py"
spec = importlib.util.spec_from_file_location("content_claim_evidence_quote_mismatch_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _db() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript(
        """
        CREATE TABLE content_claim_checks (id INTEGER PRIMARY KEY, content_id TEXT);
        CREATE TABLE content_claim_evidence (
          id INTEGER PRIMARY KEY,
          claim_check_id INTEGER,
          quote_text TEXT,
          excerpt_text TEXT,
          source_url TEXT
        );
        """
    )
    return c


def test_exact_and_normalized_matches_are_not_findings():
    c = _db()
    c.execute("INSERT INTO content_claim_checks VALUES (1,'c1')")
    c.execute("INSERT INTO content_claim_evidence VALUES (1,1,'The API shipped in 2024.','the api shipped in 2024 and stayed stable.','u')")
    report = build_content_claim_evidence_quote_mismatch_report_from_db(c)
    assert report["findings"] == []
    assert json.loads(format_content_claim_evidence_quote_mismatch_json(report))["artifact_type"] == "content_claim_evidence_quote_mismatch"
    assert "No content claim evidence" in format_content_claim_evidence_quote_mismatch_text(report)


def test_flags_fuzzy_mismatch_missing_quote_and_missing_excerpt(tmp_path, capsys):
    c = _db()
    c.execute("INSERT INTO content_claim_checks VALUES (1,'c1')")
    c.execute("INSERT INTO content_claim_checks VALUES (2,'c2')")
    c.execute("INSERT INTO content_claim_checks VALUES (3,'c3')")
    c.execute("INSERT INTO content_claim_evidence VALUES (1,1,'Revenue rose 18 percent','Revenue fell 9 percent','u1')")
    c.execute("INSERT INTO content_claim_evidence VALUES (2,2,'','Some excerpt','u2')")
    c.execute("INSERT INTO content_claim_evidence VALUES (3,3,'Quoted claim',NULL,'u3')")
    report = build_content_claim_evidence_quote_mismatch_report_from_db(c, distance_threshold=0.1)
    assert {r["issue_type"] for r in report["findings"]} == {"quote_edit_distance", "missing_quote", "missing_excerpt"}
    c.commit()
    path = tmp_path / "db.sqlite"
    with sqlite3.connect(path) as out:
        c.backup(out)
    assert script.main(["--db", str(path), "--format", "text", "--distance-threshold", "0.1"]) == 0
    assert "Content Claim Evidence Quote Mismatch" in capsys.readouterr().out
    assert script.main(["--db", str(path), "--limit", "0"]) == 2
    assert script.main(["--db", str(path), "--distance-threshold", "2"]) == 2


def test_schema_gaps_are_reported():
    report = build_content_claim_evidence_quote_mismatch_report_from_db(sqlite3.connect(":memory:"))
    assert report["missing_tables"] == ["content_claim_checks", "content_claim_evidence"]
    c = sqlite3.connect(":memory:")
    c.executescript("CREATE TABLE content_claim_checks (content_id TEXT); CREATE TABLE content_claim_evidence (quote_text TEXT);")
    report = build_content_claim_evidence_quote_mismatch_report_from_db(c)
    assert report["missing_columns"]["content_claim_checks"] == ["id"]
    assert report["missing_columns"]["content_claim_evidence"] == ["claim_check_id"]
