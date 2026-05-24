from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.publication_attempt_redirect_chain_risk import build_publication_attempt_redirect_chain_risk_report_from_db, format_publication_attempt_redirect_chain_risk_json, format_publication_attempt_redirect_chain_risk_text
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'publication_attempt_redirect_chain_risk.py'; spec=importlib.util.spec_from_file_location('script_publication_attempt_redirect_chain_risk',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(':memory:'); c.row_factory=sqlite3.Row; c.executescript('CREATE TABLE publication_attempts (id INTEGER, platform TEXT, content_id INTEGER, url TEXT, final_url TEXT, redirect_hops INTEGER, status TEXT, checked_at TEXT);'); c.execute('INSERT INTO publication_attempts VALUES (1,?,?,?,?,?,?,?)',('x',10,'https://example.com','http://other.com',5,'success','2026-05-01')); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_publication_attempt_redirect_chain_risk_report_from_db(_db())
    assert r['artifact_type']=='publication_attempt_redirect_chain_risk'
    assert r['findings']
    assert json.loads(format_publication_attempt_redirect_chain_risk_json(r))['artifact_type']=='publication_attempt_redirect_chain_risk'
    assert 'Publication' in format_publication_attempt_redirect_chain_risk_text(r)
    db=tmp_path/'db.sqlite'; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(['--db',str(db),'--format','text']+['--max-hops', '2'])==0
    assert capsys.readouterr().out
    assert script.main(['--db',str(db),'--limit','0'])==2
def test_missing_schema():
    r=build_publication_attempt_redirect_chain_risk_report_from_db(sqlite3.connect(':memory:'))
    assert r['missing_tables'] or r['missing_columns']
