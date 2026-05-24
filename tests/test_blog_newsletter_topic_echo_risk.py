from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation import blog_newsletter_topic_echo_risk as mod
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'blog_newsletter_topic_echo_risk.py'
spec=importlib.util.spec_from_file_location('blog_newsletter_topic_echo_risk_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_report_and_cli(tmp_path,capsys):
 blogs=[{'id':1,'title':'AI testing workflow guide','published_at':'2026-05-01T00:00:00Z'}]; news=[{'id':2,'title':'AI testing workflow tips','sent_at':'2026-05-05T00:00:00Z'}]
 r=mod.build_blog_newsletter_topic_echo_risk_report(blogs,news,similarity_threshold=.4); assert r['findings'][0]['day_gap']==4
 db=tmp_path/'x.sqlite'; c=sqlite3.connect(db); c.executescript('CREATE TABLE blog_posts (id INTEGER,title TEXT,published_at TEXT); CREATE TABLE newsletter_issues (id INTEGER,title TEXT,sent_at TEXT); INSERT INTO blog_posts VALUES (1,"AI testing workflow guide","2026-05-01T00:00:00Z"); INSERT INTO newsletter_issues VALUES (2,"AI testing workflow tips","2026-05-05T00:00:00Z");'); c.close()
 assert script.main(['--db',str(db),'--format','text','--similarity-threshold','.4'])==0; assert 'Topic Echo' in capsys.readouterr().out
 assert script.main(['--db',str(db),'--cooldown-days','0'])==2
def test_missing(): assert mod.build_blog_newsletter_topic_echo_risk_report_from_db(sqlite3.connect(':memory:'))['missing_tables']==['blog_posts|generated_content','newsletter_issues']

