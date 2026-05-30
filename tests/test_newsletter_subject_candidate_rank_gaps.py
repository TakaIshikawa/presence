from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.newsletter_subject_candidate_rank_gaps import build_newsletter_subject_candidate_rank_gaps_report, build_newsletter_subject_candidate_rank_gaps_report_from_db

NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_subject_candidate_rank_gaps.py"
spec = importlib.util.spec_from_file_location("newsletter_subject_candidate_rank_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_flags_rank_gaps():
    report = build_newsletter_subject_candidate_rank_gaps_report([
        {"id": 1, "newsletter_send_id": 1, "issue_id": "a", "subject": "A", "score": 1, "rank": 1, "selected": 0},
        {"id": 2, "newsletter_send_id": 1, "issue_id": "a", "subject": "B", "score": 0.5, "rank": 1, "selected": 1, "send_subject": "C"},
        {"id": 3, "newsletter_send_id": 2, "issue_id": "b", "subject": "D", "score": 1, "rank": 1, "selected": 0},
    ], score_gap_threshold=0.1, now=NOW)
    assert report["artifact_type"] == "newsletter_subject_candidate_rank_gaps"
    assert report["totals"]["by_reason"] == {"duplicate_rank": 1, "missing_selected_candidate": 1, "selected_low_rank": 0, "send_subject_mismatch": 1}


def test_db_loader_and_cli(tmp_path, capsys):
    path = tmp_path / "nsc.sqlite"
    conn = sqlite3.connect(path)
    conn.executescript("""CREATE TABLE newsletter_sends (id INTEGER, subject TEXT);
    CREATE TABLE newsletter_subject_candidates (id INTEGER, newsletter_send_id INTEGER, issue_id TEXT, subject TEXT, score REAL, rank INTEGER, selected INTEGER);
    INSERT INTO newsletter_sends VALUES (1, 'A');
    INSERT INTO newsletter_subject_candidates VALUES (1, 1, 'i', 'B', 0.5, 2, 1);""")
    conn.commit()
    assert build_newsletter_subject_candidate_rank_gaps_report_from_db(conn, now=NOW)["totals"]["by_reason"]["send_subject_mismatch"] == 1
    assert script.main(["--db", str(path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "newsletter_subject_candidate_rank_gaps"
