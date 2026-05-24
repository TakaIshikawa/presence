from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.newsletter_delivery_failure_taxonomy import build_newsletter_delivery_failure_taxonomy_report_from_db, format_newsletter_delivery_failure_taxonomy_json, format_newsletter_delivery_failure_taxonomy_text
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'newsletter_delivery_failure_taxonomy.py'; spec=importlib.util.spec_from_file_location('script_newsletter_delivery_failure_taxonomy',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(':memory:'); c.row_factory=sqlite3.Row; c.executescript('CREATE TABLE newsletter_delivery_events (id INTEGER, provider TEXT, issue_id TEXT, subscriber_id TEXT, status TEXT, error_code TEXT, error_message TEXT, occurred_at TEXT);'); c.executemany('INSERT INTO newsletter_delivery_events VALUES (?,?,?,?,?,?,?,?)',[(1,'ses','i1','s1','failed','bounce','Mailbox bounced','2026-05-01'),(2,'ses','i1','s2','failed','timeout','Timed out','2026-05-02')]); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_newsletter_delivery_failure_taxonomy_report_from_db(_db())
    assert r['artifact_type']=='newsletter_delivery_failure_taxonomy'
    assert r['findings']
    assert json.loads(format_newsletter_delivery_failure_taxonomy_json(r))['artifact_type']=='newsletter_delivery_failure_taxonomy'
    assert 'Newsletter' in format_newsletter_delivery_failure_taxonomy_text(r)
    db=tmp_path/'db.sqlite'; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(['--db',str(db),'--format','text']+[])==0
    assert capsys.readouterr().out
    assert script.main(['--db',str(db),'--limit','0'])==2
def test_missing_schema():
    r=build_newsletter_delivery_failure_taxonomy_report_from_db(sqlite3.connect(':memory:'))
    assert r['missing_tables'] or r['missing_columns']
