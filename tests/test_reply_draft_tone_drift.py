from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.reply_draft_tone_drift import build_reply_draft_tone_drift_report_from_db, format_reply_draft_tone_drift_json, format_reply_draft_tone_drift_text

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_draft_tone_drift.py"
spec = importlib.util.spec_from_file_location("reply_draft_tone_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE reply_drafts (id INTEGER PRIMARY KEY, platform TEXT, persona TEXT, tone TEXT, guard_tone TEXT, review_status TEXT, created_at TEXT)")
    return conn


def test_tone_drift_findings_and_cli(tmp_path, capsys):
    conn = _conn()
    conn.execute("INSERT INTO reply_drafts VALUES (1, 'linkedin', 'expert', NULL, NULL, 'pending', ?)", (NOW.isoformat(),))
    conn.execute("INSERT INTO reply_drafts VALUES (2, 'x', 'expert', 'formal', 'casual', 'rejected', ?)", (NOW.isoformat(),))
    report = build_reply_draft_tone_drift_report_from_db(conn, now=NOW)
    kinds = {f["finding_type"] for f in report["findings"]}
    assert {"missing_tone_metadata", "persona_guard_tone_mismatch", "reviewer_tone_rejection", "platform_tone_mismatch"} <= kinds
    assert json.loads(format_reply_draft_tone_drift_json(report))["artifact_type"] == "reply_draft_tone_drift"
    assert "Reply Draft Tone Drift" in format_reply_draft_tone_drift_text(report)
    conn.commit()
    dest = sqlite3.connect(tmp_path / "tone.sqlite")
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(tmp_path / "tone.sqlite"), "--lookback-days", "30"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] >= 1


def test_schema_empty_and_validation():
    assert build_reply_draft_tone_drift_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"]
    assert build_reply_draft_tone_drift_report_from_db(_conn(), now=NOW)["empty_state"]["is_empty"]
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
