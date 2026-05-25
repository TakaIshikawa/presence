from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.blog_draft_reading_level_outliers import build_blog_draft_reading_level_outliers_report_from_db
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"blog_draft_reading_level_outliers.py"; spec=importlib.util.spec_from_file_location("blog_draft_reading_level_outliers_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE blog_drafts (id TEXT, title TEXT, body TEXT)"); hard=("Internationalization characterization documentation optimization " * 20)+". "; easy=("Short words work well. " * 20)
    c.executemany("INSERT INTO blog_drafts VALUES (?,?,?)",[("d1","hard",hard),("d2","easy",easy)]); c.commit(); return c
def test_outliers_and_cli(tmp_path,capsys):
    r=build_blog_draft_reading_level_outliers_report_from_db(_db(),min_words=10,max_average_sentence_words=10,max_complex_word_ratio=.2)
    assert [f["draft_id"] for f in r["findings"]]==["d1"]
    db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(["--db",str(db),"--min-words","10","--max-average-sentence-words","10","--format","json"])==0
    assert json.loads(capsys.readouterr().out)["findings"]
