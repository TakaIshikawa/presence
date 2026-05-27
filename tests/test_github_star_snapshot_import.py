from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.github_star_snapshot_import import parse_github_star_snapshots, upsert_github_star_snapshots
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_github_star_snapshots.py"; spec=importlib.util.spec_from_file_location("import_github_star_snapshots_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_github_star_snapshots_formats_normalization_and_upsert(tmp_path):
    rows=parse_github_star_snapshots('[{"repository":"Owner/Repo","date":"2026-05-01","stars":"3","watchers":"bad","source":"api"}]')
    assert rows[0]["repo"]=="owner/repo"; assert rows[0]["snapshot_at"]=="2026-05-01"; assert rows[0]["watchers_count"]==0
    c=sqlite3.connect(":memory:"); upsert_github_star_snapshots(c,rows); upsert_github_star_snapshots(c,[{**rows[0],"stargazers_count":5}])
    assert c.execute("SELECT COUNT(*),stargazers_count FROM github_star_snapshots").fetchone()==(1,5)
    assert len(parse_github_star_snapshots("repo,snapshot_at,stargazers_count\na/b,2026-05-02,1\n"))==1
    assert len(parse_github_star_snapshots('{"repo":"a/c","timestamp":"2026-05-03"}\n'))==1
    p=tmp_path/"stars.json"; p.write_text('{"snapshots":[{"repo":"a/b","date":"2026-05-01"}]}'); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--dry-run"])==0
