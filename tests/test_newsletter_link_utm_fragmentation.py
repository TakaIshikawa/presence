from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from evaluation.newsletter_link_utm_fragmentation import build_newsletter_link_utm_fragmentation_report_from_db, format_newsletter_link_utm_fragmentation_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"newsletter_link_utm_fragmentation.py"; spec=importlib.util.spec_from_file_location("newsletter_link_utm_fragmentation_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_utm_fragmentation_and_cli(tmp_path,capsys):
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE newsletter_links (id INTEGER, issue_id TEXT, section TEXT, variant TEXT, url TEXT)")
    c.execute("INSERT INTO newsletter_links VALUES (1,'i1','top','a','https://Ex.com/post?utm_source=news&utm_medium=email&utm_campaign=a&x=1')")
    c.execute("INSERT INTO newsletter_links VALUES (2,'i2','top','b','https://ex.com/post/?x=1&utm_source=letter&utm_medium=email&utm_campaign=b')")
    r=build_newsletter_link_utm_fragmentation_report_from_db(c)
    assert r["findings"][0]["variant_count"]==2
    assert "Newsletter Link UTM" in format_newsletter_link_utm_fragmentation_text(r)
    c.commit(); path=tmp_path/"db.sqlite"; sqlite3.connect(path).close()
    with sqlite3.connect(path) as out: c.backup(out)
    assert script.main(["--db",str(path),"--format","text","--min-variants","2"])==0
    assert "Fragmentation" in capsys.readouterr().out
    assert script.main(["--db",str(path),"--limit","0"])==2
def test_utm_schema_gap():
    r=build_newsletter_link_utm_fragmentation_report_from_db(sqlite3.connect(":memory:"))
    assert r["missing_tables"]==["newsletter_links"]
