from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation import newsletter_archive_metric_backfill_candidates as mod
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'newsletter_archive_metric_backfill_candidates.py'
spec=importlib.util.spec_from_file_location('newsletter_archive_metric_backfill_candidates_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_report_and_cli(tmp_path,capsys):
 issues=[{'id':1,'status':'sent','sent_at':'2026-05-01T00:00:00Z','audience_size':500}]; metrics=[{'issue_id':1,'metric_type':'opens'}]
 r=mod.build_newsletter_archive_metric_backfill_candidates_report(issues,metrics,min_age_hours=1,now='2026-05-03T00:00:00Z')
 assert r['findings'][0]['missing_metrics']==['clicks','bounces','unsubscribes']
 db=tmp_path/'x.sqlite'; c=sqlite3.connect(db); c.executescript('CREATE TABLE newsletter_issues (id INTEGER,status TEXT,sent_at TEXT,audience_size INTEGER); CREATE TABLE newsletter_metrics (issue_id INTEGER,metric_type TEXT); INSERT INTO newsletter_issues VALUES (1,"sent","2026-05-01T00:00:00Z",500); INSERT INTO newsletter_metrics VALUES (1,"opens");'); c.close()
 assert script.main(['--db',str(db),'--format','text','--min-age-hours','1'])==0; assert 'Backfill' in capsys.readouterr().out
 assert script.main(['--db',str(db),'--limit','0'])==2
def test_missing(): assert mod.build_newsletter_archive_metric_backfill_candidates_report_from_db(sqlite3.connect(':memory:'))['missing_tables']==['newsletter_issues']

