from __future__ import annotations
import importlib.util, json, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from evaluation.proactive_action_due_date_slippage import build_proactive_action_due_date_slippage_report_from_db, format_proactive_action_due_date_slippage_json, format_proactive_action_due_date_slippage_text
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "proactive_action_due_date_slippage.py"; spec = importlib.util.spec_from_file_location("script_proactive_action_due_date_slippage", SCRIPT); script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c = sqlite3.connect(":memory:"); c.row_factory = sqlite3.Row
    c.executescript("CREATE TABLE proactive_actions (id TEXT, target TEXT, platform TEXT, due_at TEXT, status TEXT);")
    c.execute("INSERT INTO proactive_actions VALUES (?,?,?,?,?)", ("a1", "Ada", "x", "2026-05-20T00:00:00Z", "open"))
    c.execute("INSERT INTO proactive_actions VALUES (?,?,?,?,?)", ("a2", "Done", "x", "2026-05-01T00:00:00Z", "completed"))
    c.commit(); return c
def test_due_date_slippage_report_and_cli(tmp_path, capsys):
    r = build_proactive_action_due_date_slippage_report_from_db(_db(), now=datetime(2026,5,27,tzinfo=timezone.utc))
    assert r["artifact_type"] == "proactive_action_due_date_slippage" and r["slipped_actions"][0]["days_overdue"] == 7
    assert json.loads(format_proactive_action_due_date_slippage_json(r))["artifact_type"] == r["artifact_type"]
    assert "Proactive Action Due Date Slippage" in format_proactive_action_due_date_slippage_text(r)
    db = tmp_path / "db.sqlite"; out = sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(["--db", str(db), "--format", "text"]) == 0 and capsys.readouterr().out
    assert script.main(["--db", str(db), "--limit", "0"]) == 2
def test_missing_schema():
    assert build_proactive_action_due_date_slippage_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
