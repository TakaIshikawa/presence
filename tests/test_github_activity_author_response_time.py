from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.github_activity_author_response_time import build_github_activity_author_response_time_report_from_db, format_github_activity_author_response_time_json, format_github_activity_author_response_time_text
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'github_activity_author_response_time.py'; spec=importlib.util.spec_from_file_location('script_github_activity_author_response_time',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(':memory:'); c.row_factory=sqlite3.Row; c.executescript('CREATE TABLE github_activity (id TEXT, author TEXT, repository TEXT, occurred_at TEXT); CREATE TABLE reply_queue (github_activity_id TEXT, responded_at TEXT, status TEXT);'); c.execute('INSERT INTO github_activity VALUES (?,?,?,?)',('g1','octo','repo','2026-05-01T00:00:00+00:00')); c.execute('INSERT INTO reply_queue VALUES (?,?,?)',('g1','2026-05-03T00:00:00+00:00','draft')); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_github_activity_author_response_time_report_from_db(_db())
    assert r['artifact_type']=='github_activity_author_response_time'
    assert r['findings']
    assert json.loads(format_github_activity_author_response_time_json(r))['artifact_type']=='github_activity_author_response_time'
    assert 'GitHub' in format_github_activity_author_response_time_text(r)
    db=tmp_path/'db.sqlite'; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(['--db',str(db),'--format','text']+['--sla-hours', '24', '--window-days', '30'])==0
    assert capsys.readouterr().out
    assert script.main(['--db',str(db),'--limit','0'])==2
def test_missing_schema():
    r=build_github_activity_author_response_time_report_from_db(sqlite3.connect(':memory:'))
    assert r['missing_tables'] or r['missing_columns']
