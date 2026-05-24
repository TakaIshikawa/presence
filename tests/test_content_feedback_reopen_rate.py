from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.content_feedback_reopen_rate import build_content_feedback_reopen_rate_report_from_db, format_content_feedback_reopen_rate_json, format_content_feedback_reopen_rate_text
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'content_feedback_reopen_rate.py'; spec=importlib.util.spec_from_file_location('script_content_feedback_reopen_rate',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(':memory:'); c.row_factory=sqlite3.Row; c.executescript('CREATE TABLE content_feedback_events (feedback_id TEXT, event_type TEXT, reviewer TEXT, content_type TEXT, resolution_reason TEXT, occurred_at TEXT);'); c.executemany('INSERT INTO content_feedback_events VALUES (?,?,?,?,?,?)',[('f1','resolved','r1','blog','fixed','2026-05-01T00:00:00+00:00'),('f1','reopened','r1','blog','fixed','2026-05-02T00:00:00+00:00')]); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_content_feedback_reopen_rate_report_from_db(_db())
    assert r['artifact_type']=='content_feedback_reopen_rate'
    assert r['findings']
    assert json.loads(format_content_feedback_reopen_rate_json(r))['artifact_type']=='content_feedback_reopen_rate'
    assert 'Content' in format_content_feedback_reopen_rate_text(r)
    db=tmp_path/'db.sqlite'; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(['--db',str(db),'--format','text']+['--window-days', '10', '--min-resolved', '1'])==0
    assert capsys.readouterr().out
    assert script.main(['--db',str(db),'--limit','0'])==2
def test_missing_schema():
    r=build_content_feedback_reopen_rate_report_from_db(sqlite3.connect(':memory:'))
    assert r['missing_tables'] or r['missing_columns']
