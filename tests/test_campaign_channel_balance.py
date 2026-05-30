from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation.campaign_channel_balance import build_campaign_channel_balance_report_from_db
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"campaign_channel_balance.py"; spec=importlib.util.spec_from_file_location("campaign_channel_balance_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE generated_content (content_type TEXT, campaign TEXT, metadata TEXT, created_at TEXT)")
 c.executemany("INSERT INTO generated_content VALUES (?,?,?,?)",[("x_post","c1",None,"2026-05-20T00:00:00+00:00"),("x_post","c1",None,"2026-05-20T00:00:00+00:00"),("blog_post","c1",None,"2026-05-20T00:00:00+00:00"),("newsletter","c2",None,"2026-05-20T00:00:00+00:00")]); c.commit(); return c
def test_flags_overconcentrated_campaign_and_cli(tmp_path,capsys):
 r=build_campaign_channel_balance_report_from_db(_db(),max_channel_share=.6)
 assert r["findings"][0]["campaign"]=="c2"; assert "x_thread" in r["findings"][0]["recommended_missing_channels"]
 assert build_campaign_channel_balance_report_from_db(_db(),campaign="c1")["summary"]["campaign_count"]==1
 db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close(); assert script.main(["--db",str(db),"--format","json"])==0; assert json.loads(capsys.readouterr().out)["artifact_type"]=="campaign_channel_balance"
