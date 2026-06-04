from __future__ import annotations
import importlib.util,sqlite3
from datetime import datetime,timezone
from pathlib import Path
from evaluation.newsletter_open_rate_segment_outliers import build_newsletter_open_rate_segment_outliers_report
NOW=datetime(2026,6,1,tzinfo=timezone.utc)
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"newsletter_open_rate_segment_outliers.py"; spec=importlib.util.spec_from_file_location("script_newsletter_open_rate_segment_outliers",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_computes_low_and_high_outliers():
 rows=[{"campaign_id":"c","segment_key":"plan","segment_value":"pro","sent_count":100,"open_count":80},{"campaign_id":"c","segment_key":"plan","segment_value":"free","sent_count":100,"open_count":10}]
 r=build_newsletter_open_rate_segment_outliers_report(rows,min_segment_size=10,min_delta_rate=.2,now=NOW)
 assert {i["direction"] for i in r["outliers"]}=={"low","high"}
def test_filters_small_segments_and_direction():
 rows=[{"campaign_id":"c","segment_value":"small","sent_count":5,"open_count":5},{"campaign_id":"c","segment_value":"low","sent_count":100,"open_count":0},{"campaign_id":"c","segment_value":"base","sent_count":100,"open_count":50}]
 r=build_newsletter_open_rate_segment_outliers_report(rows,min_segment_size=10,min_delta_rate=.1,direction="low",now=NOW)
 assert all(i["direction"]=="low" for i in r["outliers"])
def test_cli(tmp_path,capsys):
 db=tmp_path/"db.sqlite"; c=sqlite3.connect(db); c.execute("CREATE TABLE newsletter_segment_metrics (campaign_id TEXT,segment_key TEXT,segment_value TEXT,sent_count INTEGER,open_count INTEGER)"); c.executemany("INSERT INTO newsletter_segment_metrics VALUES (?,?,?,?,?)",[("c","plan","pro",100,80),("c","plan","free",100,10)]); c.commit(); c.close()
 assert script.main(["--db",str(db),"--format","text","--min-segment-size","10","--min-delta-rate",".2"])==0; assert capsys.readouterr().out
