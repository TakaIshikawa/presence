from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.github_activity_release_note_coverage import build_github_activity_release_note_coverage_report


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "github_activity_release_note_coverage.py"
spec = importlib.util.spec_from_file_location("github_activity_release_note_coverage_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_covered_missing_and_late_release_notes(tmp_path, capsys):
    report = build_github_activity_release_note_coverage_report(
        [
            {"repo": "org/app", "activity_id": "covered", "merged_at": "2026-05-01T00:00:00+00:00", "significance": 8},
            {"repo": "org/app", "activity_id": "missing", "merged_at": "2026-05-02T00:00:00+00:00", "significance": 8},
            {"repo": "org/app", "activity_id": "late", "merged_at": "2026-05-01T00:00:00+00:00", "significance": 9},
            {"repo": "org/app", "activity_id": "ignored", "merged_at": "2026-05-01T00:00:00+00:00", "significance": 2},
        ],
        [
            {"repo": "org/app", "activity_id": "covered", "release_tag": "v1", "released_at": "2026-05-03T00:00:00+00:00", "summary": "Adds the important shipped feature"},
            {"repo": "org/app", "activity_id": "late", "release_tag": "v2", "released_at": "2026-05-20T00:00:00+00:00", "summary": "Documents the important shipped feature"},
        ],
    )
    assert {key for row in report["rows"] for key in row} >= {"repo", "activity_id", "merged_at", "release_tag", "coverage_status", "lag_days", "missing_summary_reason"}
    assert {row["coverage_status"] for row in report["rows"]} == {"missing", "late"}
    assert report["totals"]["scoped_activity_count"] == 3

    db_path = tmp_path / "github.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE github_activities (repo TEXT, id TEXT, merged_at TEXT, significance INTEGER)")
    conn.execute("CREATE TABLE github_release_notes (repo TEXT, activity_id TEXT, release_tag TEXT, released_at TEXT, summary TEXT)")
    conn.execute("INSERT INTO github_activities VALUES ('org/app','a1','2026-05-01T00:00:00+00:00',10)")
    conn.commit()
    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["rows"][0]["activity_id"] == "a1"


def test_validation_errors():
    with pytest.raises(ValueError, match="significance_threshold"):
        build_github_activity_release_note_coverage_report([], [], significance_threshold=-1)
