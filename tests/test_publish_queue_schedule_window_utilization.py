from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.publish_queue_schedule_window_utilization import build_publish_queue_schedule_window_utilization_report_from_db, format_publish_queue_schedule_window_utilization_json, format_publish_queue_schedule_window_utilization_text
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'publish_queue_schedule_window_utilization.py'; spec=importlib.util.spec_from_file_location('script_publish_queue_schedule_window_utilization',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(':memory:'); c.row_factory=sqlite3.Row; c.executescript('CREATE TABLE publish_queue (id INTEGER, platform TEXT, scheduled_at TEXT, status TEXT); CREATE TABLE publish_windows (platform TEXT, day_of_week TEXT, start_time TEXT, end_time TEXT, capacity INTEGER);'); c.execute('INSERT INTO publish_queue VALUES (1,?,?,?)',('x','2026-05-24T12:00:00+00:00','queued')); c.execute('INSERT INTO publish_windows VALUES (?,?,?,?,?)',('x','sun','09:00','10:00',1)); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_publish_queue_schedule_window_utilization_report_from_db(_db())
    assert r['artifact_type']=='publish_queue_schedule_window_utilization'
    assert r['findings']
    assert json.loads(format_publish_queue_schedule_window_utilization_json(r))['artifact_type']=='publish_queue_schedule_window_utilization'
    assert 'Publish' in format_publish_queue_schedule_window_utilization_text(r)
    db=tmp_path/'db.sqlite'; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(['--db',str(db),'--format','text']+['--underused-threshold', '.8', '--overfilled-threshold', '1'])==0
    assert capsys.readouterr().out
    assert script.main(['--db',str(db),'--limit','0'])==2
def test_missing_schema():
    r=build_publish_queue_schedule_window_utilization_report_from_db(sqlite3.connect(':memory:'))
    assert r['missing_tables'] or r['missing_columns']
