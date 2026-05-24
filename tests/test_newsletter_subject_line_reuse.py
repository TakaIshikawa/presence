from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.newsletter_subject_line_reuse import build_newsletter_subject_line_reuse_report_from_db, format_newsletter_subject_line_reuse_json, format_newsletter_subject_line_reuse_text
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'newsletter_subject_line_reuse.py'; spec=importlib.util.spec_from_file_location('newsletter_subject_line_reuse_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(':memory:'); c.row_factory=sqlite3.Row; c.executescript('CREATE TABLE newsletter_issues (id TEXT, subject TEXT, sent_at TEXT);')
    c.executemany('INSERT INTO newsletter_issues VALUES (?,?,?)',[('n1','Launch Checklist!','2026-05-01'),('n2','launch checklist','2026-05-10'),('n3','Different topic','2026-05-11')]); c.commit(); return c
def test_exact_normalized_reuse_and_non_reuse(tmp_path,capsys):
    r=build_newsletter_subject_line_reuse_report_from_db(_db(),similarity_threshold=.8)
    assert r['findings'][0]['reuse_count']==2
    assert 'n3' not in r['findings'][0]['issue_ids']
    assert json.loads(format_newsletter_subject_line_reuse_json(r))['artifact_type']=='newsletter_subject_line_reuse'
    assert 'Newsletter Subject' in format_newsletter_subject_line_reuse_text(r)
    db=tmp_path/'db.sqlite'; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(['--db',str(db),'--format','text','--lookback-days','30'])==0
    assert 'launch checklist' in capsys.readouterr().out
