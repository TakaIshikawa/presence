from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.github_commit_author_mapping_gaps import build_github_commit_author_mapping_gaps_report_from_db, format_github_commit_author_mapping_gaps_json, format_github_commit_author_mapping_gaps_text
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'github_commit_author_mapping_gaps.py'; spec=importlib.util.spec_from_file_location('github_commit_author_mapping_gaps_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(':memory:'); c.row_factory=sqlite3.Row; c.executescript('CREATE TABLE github_commits (repo TEXT, sha TEXT, author_login TEXT, author_email TEXT, author_name TEXT, person_id TEXT, committed_at TEXT);')
    c.executemany('INSERT INTO github_commits VALUES (?,?,?,?,?,?,?)',[('r','a1','octo','o@e.test','Octo',None,'2026-05-01'),('r','a2','octo','o@e.test','Octo','', '2026-05-02'),('r','a3','mapped','m@e.test','Mapped','p1','2026-05-03')]); c.commit(); return c
def test_groups_unmapped_authors_and_cli(tmp_path,capsys):
    r=build_github_commit_author_mapping_gaps_report_from_db(_db(),repo='r')
    assert r['findings'][0]['commit_count']==2
    assert r['findings'][0]['sample_shas']==['a1','a2']
    assert json.loads(format_github_commit_author_mapping_gaps_json(r))['artifact_type']=='github_commit_author_mapping_gaps'
    assert 'GitHub Commit' in format_github_commit_author_mapping_gaps_text(r)
    db=tmp_path/'db.sqlite'; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(['--db',str(db),'--format','text','--repo','r'])==0
    assert 'o@e.test' in capsys.readouterr().out
