from __future__ import annotations
import importlib.util, json, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from evaluation.github_activity_issue_comment_followup_gaps import build_github_activity_issue_comment_followup_gaps_report_from_db, format_github_activity_issue_comment_followup_gaps_json, format_github_activity_issue_comment_followup_gaps_text
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "github_activity_issue_comment_followup_gaps.py"; spec = importlib.util.spec_from_file_location("script_github_activity_issue_comment_followup_gaps", SCRIPT); script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c = sqlite3.connect(":memory:"); c.row_factory = sqlite3.Row
    c.executescript("CREATE TABLE github_issue_comments (id TEXT, repository TEXT, issue_number TEXT, body TEXT, created_at TEXT); CREATE TABLE planned_topics (id TEXT, comment_id TEXT, title TEXT);")
    c.execute("INSERT INTO github_issue_comments VALUES (?,?,?,?,?)", ("c1", "o/r", "12", "@me TODO bug? release note feedback", "2026-05-01T00:00:00Z"))
    c.execute("INSERT INTO github_issue_comments VALUES (?,?,?,?,?)", ("c2", "o/r", "13", "how about this?", "2026-05-01T00:00:00Z")); c.execute("INSERT INTO planned_topics VALUES (?,?,?)", ("p1", "c2", "done"))
    c.commit(); return c
def test_github_comment_followup_report_and_cli(tmp_path, capsys):
    r = build_github_activity_issue_comment_followup_gaps_report_from_db(_db(), now=datetime(2026,5,27,tzinfo=timezone.utc))
    assert r["artifact_type"] == "github_activity_issue_comment_followup_gaps"
    assert r["comment_followup_gaps"][0]["comment_id"] == "c1" and r["comment_followup_gaps"][0]["age_days"] == 26
    assert "bug" in r["comment_followup_gaps"][0]["missing_followup_reason"]
    assert json.loads(format_github_activity_issue_comment_followup_gaps_json(r))["artifact_type"] == r["artifact_type"]
    assert "GitHub Activity" in format_github_activity_issue_comment_followup_gaps_text(r)
    db = tmp_path / "db.sqlite"; out = sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(["--db", str(db), "--format", "text"]) == 0 and capsys.readouterr().out
    assert script.main(["--db", str(db), "--limit", "0"]) == 2
def test_missing_schema():
    assert build_github_activity_issue_comment_followup_gaps_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
