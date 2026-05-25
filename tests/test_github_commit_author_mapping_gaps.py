from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.github_commit_author_mapping_gaps import build_github_commit_author_mapping_gaps_report_from_db
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"github_commit_author_mapping_gaps.py"; spec=importlib.util.spec_from_file_location("github_commit_author_mapping_gaps_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE github_commits (repo TEXT, sha TEXT, author_email TEXT, author_name TEXT, person_id TEXT, authored_at TEXT)")
    c.executemany("INSERT INTO github_commits VALUES (?,?,?,?,?,?)",[("r","a","x@example.com","X",None,"2026-05-01"),("r","b","x@example.com","X","p1","2026-05-02"),("r","c","x@example.com","X",None,"2026-05-03")]); c.commit(); return c
def test_author_gaps_cli(tmp_path,capsys):
    assert build_github_commit_author_mapping_gaps_report_from_db(_db(),repo="r")["findings"][0]["commit_count"]==2
    db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(["--db",str(db),"--repo","r","--format","json"])==0
    assert json.loads(capsys.readouterr().out)["findings"][0]["sample_shas"]==["a","c"]
