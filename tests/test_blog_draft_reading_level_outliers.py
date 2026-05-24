from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.blog_draft_reading_level_outliers import build_blog_draft_reading_level_outliers_report_from_db, format_blog_draft_reading_level_outliers_json, format_blog_draft_reading_level_outliers_text
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'blog_draft_reading_level_outliers.py'; spec=importlib.util.spec_from_file_location('blog_draft_reading_level_outliers_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(':memory:'); c.row_factory=sqlite3.Row; c.execute('CREATE TABLE blog_drafts (id TEXT, title TEXT, content TEXT)')
    long=' '.join(['extraordinary']*60)+'.'; ok='Short words work well. Clear posts help readers.'
    c.executemany('INSERT INTO blog_drafts VALUES (?,?,?)',[('d1','hard',long),('d2','ok',ok)]); c.commit(); return c
def test_readability_metrics_flags_outlier_and_cli(tmp_path,capsys):
    r=build_blog_draft_reading_level_outliers_report_from_db(_db(),min_words=10,max_average_sentence_words=20,max_complex_word_ratio=.2)
    assert r['findings'][0]['draft_id']=='d1'
    assert 'complex_word_ratio' in r['findings'][0]['triggering_metrics']
    assert json.loads(format_blog_draft_reading_level_outliers_json(r))['artifact_type']=='blog_draft_reading_level_outliers'
    assert 'Blog Draft' in format_blog_draft_reading_level_outliers_text(r)
    db=tmp_path/'db.sqlite'; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(['--db',str(db),'--format','text','--min-words','10'])==0
    assert 'd1' in capsys.readouterr().out
