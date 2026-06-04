from __future__ import annotations
import importlib.util, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from evaluation.reply_draft_mention_overuse import build_reply_draft_mention_overuse_report
NOW=datetime(2026,6,1,tzinfo=timezone.utc)
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"reply_draft_mention_overuse.py"; spec=importlib.util.spec_from_file_location("script_reply_draft_mention_overuse",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_extracts_mentions_without_email_addresses():
    r=build_reply_draft_mention_overuse_report([{"id":"r1","body":"hi @alice a@b.com @bob @carol"}],max_mentions=2,now=NOW)
    assert r["overuse"][0]["mention_count"]==3
def test_flags_repeated_handle_and_sorts_severity():
    rows=[{"id":"low","body":"@a @b @c"},{"id":"repeat","body":"@a @a"}]
    r=build_reply_draft_mention_overuse_report(rows,max_mentions=2,max_repeated_handle_count=1,now=NOW)
    assert [i["draft_id"] for i in r["overuse"]]==["low","repeat"]
def test_db_cli(tmp_path,capsys):
    db=tmp_path/"db.sqlite"; c=sqlite3.connect(db); c.execute("CREATE TABLE reply_drafts (id TEXT, target_id TEXT, body TEXT, status TEXT)"); c.execute("INSERT INTO reply_drafts VALUES (?,?,?,?)",("r1","t1","@a @b @c","draft")); c.commit(); c.close()
    assert script.main(["--db",str(db),"--format","text","--max-mentions","2"])==0; assert capsys.readouterr().out
