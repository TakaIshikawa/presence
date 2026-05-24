from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.newsletter_signup_source_quality import build_newsletter_signup_source_quality_report_from_db, format_newsletter_signup_source_quality_json, format_newsletter_signup_source_quality_text
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'newsletter_signup_source_quality.py'; spec=importlib.util.spec_from_file_location('script_newsletter_signup_source_quality',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(':memory:'); c.row_factory=sqlite3.Row; c.executescript('CREATE TABLE newsletter_subscribers (id INTEGER, source TEXT, campaign TEXT, consented_at TEXT, created_at TEXT, email TEXT);'); c.executemany('INSERT INTO newsletter_subscribers VALUES (?,?,?,?,?,?)',[(1,'ad','',None,'2026-05-01T00:00:00+00:00','a@mailinator.com'),(2,'ad','',None,'2026-05-02T00:00:00+00:00','b@example.com'),(3,'ad','spring','2026-05-03','2026-05-03T00:00:00+00:00','c@example.com')]); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_newsletter_signup_source_quality_report_from_db(_db())
    assert r['artifact_type']=='newsletter_signup_source_quality'
    assert r['findings']
    assert json.loads(format_newsletter_signup_source_quality_json(r))['artifact_type']=='newsletter_signup_source_quality'
    assert 'Newsletter' in format_newsletter_signup_source_quality_text(r)
    db=tmp_path/'db.sqlite'; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(['--db',str(db),'--format','text']+['--burst-threshold', '2'])==0
    assert capsys.readouterr().out
    assert script.main(['--db',str(db),'--limit','0'])==2
def test_missing_schema():
    r=build_newsletter_signup_source_quality_report_from_db(sqlite3.connect(':memory:'))
    assert r['missing_tables'] or r['missing_columns']
