from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.reply_draft_source_license_exposure import build_reply_draft_source_license_exposure_report_from_db, format_reply_draft_source_license_exposure_json, format_reply_draft_source_license_exposure_text
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'reply_draft_source_license_exposure.py'; spec=importlib.util.spec_from_file_location('script_reply_draft_source_license_exposure',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(':memory:'); c.row_factory=sqlite3.Row; c.executescript('CREATE TABLE reply_queue (id INTEGER, status TEXT, author TEXT, source_url TEXT, license TEXT);'); c.execute('INSERT INTO reply_queue VALUES (?,?,?,?,?)',(1,'draft','me','https://src','noncommercial')); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_reply_draft_source_license_exposure_report_from_db(_db())
    assert r['artifact_type']=='reply_draft_source_license_exposure'
    assert r['findings']
    assert json.loads(format_reply_draft_source_license_exposure_json(r))['artifact_type']=='reply_draft_source_license_exposure'
    assert 'Reply' in format_reply_draft_source_license_exposure_text(r)
    db=tmp_path/'db.sqlite'; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(['--db',str(db),'--format','text']+['--include-posted'])==0
    assert capsys.readouterr().out
    assert script.main(['--db',str(db),'--limit','0'])==2
def test_missing_schema():
    r=build_reply_draft_source_license_exposure_report_from_db(sqlite3.connect(':memory:'))
    assert r['missing_tables'] or r['missing_columns']
