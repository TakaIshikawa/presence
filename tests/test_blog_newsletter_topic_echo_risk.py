from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.blog_newsletter_topic_echo_risk import build_blog_newsletter_topic_echo_risk_report_from_db, format_blog_newsletter_topic_echo_risk_json, format_blog_newsletter_topic_echo_risk_text
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'blog_newsletter_topic_echo_risk.py'; spec=importlib.util.spec_from_file_location('script_blog_newsletter_topic_echo_risk',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(':memory:'); c.row_factory=sqlite3.Row; c.executescript('CREATE TABLE blog_posts (id TEXT, title TEXT, topic TEXT, published_at TEXT); CREATE TABLE newsletter_issues (id TEXT, title TEXT, topic TEXT, sent_at TEXT);'); c.execute('INSERT INTO blog_posts VALUES (?,?,?,?)',('b1','AI launch checklist','ai launch','2026-05-01')); c.execute('INSERT INTO newsletter_issues VALUES (?,?,?,?)',('n1','AI launch guide','ai launch','2026-05-03')); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_blog_newsletter_topic_echo_risk_report_from_db(_db())
    assert r['artifact_type']=='blog_newsletter_topic_echo_risk'
    assert r['findings']
    assert json.loads(format_blog_newsletter_topic_echo_risk_json(r))['artifact_type']=='blog_newsletter_topic_echo_risk'
    assert 'Blog' in format_blog_newsletter_topic_echo_risk_text(r)
    db=tmp_path/'db.sqlite'; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(['--db',str(db),'--format','text']+['--cooldown-days', '7', '--similarity-threshold', '.4'])==0
    assert capsys.readouterr().out
    assert script.main(['--db',str(db),'--limit','0'])==2
def test_missing_schema():
    r=build_blog_newsletter_topic_echo_risk_report_from_db(sqlite3.connect(':memory:'))
    assert r['missing_tables'] or r['missing_columns']
