from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.content_claim_evidence_domain_concentration import build_content_claim_evidence_domain_concentration_report_from_db, format_content_claim_evidence_domain_concentration_json, format_content_claim_evidence_domain_concentration_text
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'content_claim_evidence_domain_concentration.py'; spec=importlib.util.spec_from_file_location('script_content_claim_evidence_domain_concentration',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(':memory:'); c.row_factory=sqlite3.Row; c.executescript('CREATE TABLE content_claim_checks (id INTEGER, content_id TEXT, claim_id TEXT, evidence_url TEXT);'); c.executemany('INSERT INTO content_claim_checks VALUES (?,?,?,?)',[(1,'c1','a','https://one.com/a'),(2,'c1','b','https://one.com/b'),(3,'c1','c','https://one.com/c')]); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_content_claim_evidence_domain_concentration_report_from_db(_db())
    assert r['artifact_type']=='content_claim_evidence_domain_concentration'
    assert r['findings']
    assert json.loads(format_content_claim_evidence_domain_concentration_json(r))['artifact_type']=='content_claim_evidence_domain_concentration'
    assert 'Content' in format_content_claim_evidence_domain_concentration_text(r)
    db=tmp_path/'db.sqlite'; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(['--db',str(db),'--format','text']+['--min-claims', '2', '--max-domain-share', '.7'])==0
    assert capsys.readouterr().out
    assert script.main(['--db',str(db),'--limit','0'])==2
def test_missing_schema():
    r=build_content_claim_evidence_domain_concentration_report_from_db(sqlite3.connect(':memory:'))
    assert r['missing_tables'] or r['missing_columns']
