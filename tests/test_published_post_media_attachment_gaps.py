from __future__ import annotations
import importlib.util, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from evaluation.published_post_media_attachment_gaps import build_published_post_media_attachment_gaps_report, build_published_post_media_attachment_gaps_report_from_db
NOW=datetime(2026,6,1,tzinfo=timezone.utc)
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"published_post_media_attachment_gaps.py"; spec=importlib.util.spec_from_file_location("script_published_post_media_attachment_gaps",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_flags_media_markers_without_attachments():
    r=build_published_post_media_attachment_gaps_report([{"id":"p1","platform":"x","status":"published","body":"![alt](img.png)","attachment_count":0}],now=NOW)
    assert r["gaps"][0]["post_id"]=="p1"
def test_ignores_text_only_and_attached_and_platform_filter():
    rows=[{"id":"text","platform":"x","status":"published","body":"plain","attachment_count":0},{"id":"ok","platform":"x","status":"published","body":"<img src=a>","attachment_count":1},{"id":"other","platform":"bsky","status":"published","body":"<img src=a>","attachment_count":0}]
    assert build_published_post_media_attachment_gaps_report(rows,platform="x",now=NOW)["gaps"]==[]
def test_lookback_filter_and_db_cli(tmp_path,capsys):
    db=tmp_path/"db.sqlite"; c=sqlite3.connect(db); c.execute("CREATE TABLE published_posts (id TEXT, platform TEXT, status TEXT, body TEXT, attachment_count INTEGER, published_at TEXT)"); c.execute("INSERT INTO published_posts VALUES (?,?,?,?,?,?)",("p1","x","published","image: hero",0,"2026-05-31T00:00:00+00:00")); c.commit(); c.close()
    with sqlite3.connect(db) as r: r.row_factory=sqlite3.Row; assert build_published_post_media_attachment_gaps_report_from_db(r,now=NOW)["gaps"]
    assert script.main(["--db",str(db),"--format","text"])==0; assert capsys.readouterr().out
