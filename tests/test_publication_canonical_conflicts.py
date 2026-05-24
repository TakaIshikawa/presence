from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation.publication_canonical_conflicts import build_publication_canonical_conflicts_report_from_db,format_publication_canonical_conflicts_json,format_publication_canonical_conflicts_text,normalize_url
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"publication_canonical_conflicts.py"; spec=importlib.util.spec_from_file_location("publication_canonical_conflicts_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_canonical_conflicts_and_cli(tmp_path,capsys):
 assert normalize_url("HTTP://Ex.com/a/?utm_source=x#frag")=="https://ex.com/a"
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE generated_content (id INTEGER, canonical_url TEXT); CREATE TABLE publications (id INTEGER, content_id INTEGER, platform TEXT, published_url TEXT, canonical_url TEXT, observed_canonical_url TEXT);")
 c.execute("INSERT INTO generated_content VALUES (1,'https://ex.com/a')"); c.execute("INSERT INTO generated_content VALUES (2,'https://ex.com/b')")
 c.execute("INSERT INTO publications VALUES (10,1,'web','https://ex.com/a?utm_campaign=z','https://ex.com/a/','https://ex.com/other')")
 c.execute("INSERT INTO publications VALUES (11,2,'web','https://ex.com/b',NULL,'https://ex.com/b')")
 r=build_publication_canonical_conflicts_report_from_db(c)
 assert {x["conflict_type"] for x in r["conflicts"]}>={"platform_mismatch","missing_declared"}; assert json.loads(format_publication_canonical_conflicts_json(r))["artifact_type"]=="publication_canonical_conflicts"; assert "Publication Canonical" in format_publication_canonical_conflicts_text(r)
 c.commit(); path=tmp_path/"db.sqlite"
 with sqlite3.connect(path) as out: c.backup(out)
 assert script.main(["--db",str(path),"--format","text"])==0; assert "Publication Canonical" in capsys.readouterr().out; assert script.main(["--db",str(path),"--limit","0"])==2
def test_missing_table():
 assert build_publication_canonical_conflicts_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]==["publications|publication_records"]
