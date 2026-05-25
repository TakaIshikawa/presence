from __future__ import annotations
import importlib.util, json, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from evaluation.content_calendar_blackout_collision import build_content_calendar_blackout_collision_report_from_db, format_content_calendar_blackout_collision_json, format_content_calendar_blackout_collision_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"content_calendar_blackout_collision.py"; spec=importlib.util.spec_from_file_location("script_content_calendar_blackout_collision",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE content_calendar_items (id TEXT, title TEXT, planned_at TEXT, channel TEXT); CREATE TABLE content_calendar_holidays (date TEXT, name TEXT);")
    c.execute("INSERT INTO content_calendar_items VALUES (?,?,?,?)",("i1","Launch","2026-05-26T10:00:00+00:00","email")); c.execute("INSERT INTO content_calendar_holidays VALUES (?,?)",("2026-05-26","Holiday")); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_content_calendar_blackout_collision_report_from_db(_db(),now=datetime(2026,5,25,tzinfo=timezone.utc)); assert r["artifact_type"]=="content_calendar_blackout_collision"; assert r["findings"]; assert r["summary"]["by_collision_type"]["holiday"]==1
    assert json.loads(format_content_calendar_blackout_collision_json(r))["artifact_type"]=="content_calendar_blackout_collision"; assert "Calendar" in format_content_calendar_blackout_collision_text(r)
    db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close(); assert script.main(["--db",str(db),"--format","text","--lookahead-days","7"])==0; assert capsys.readouterr().out; assert script.main(["--db",str(db),"--limit","0"])==2
def test_missing_schema():
    assert build_content_calendar_blackout_collision_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
