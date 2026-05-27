from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from ingestion.website_referral_session_import import parse_website_referral_sessions, upsert_website_referral_sessions
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_website_referral_sessions.py"; spec=importlib.util.spec_from_file_location("import_website_referral_sessions_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_website_referrals_normalize_path_labels_and_counts(tmp_path):
    rows=parse_website_referral_sessions('[{"date":"2026-05-01","source":"Reddit","medium":"Social","campaign":"Launch","landing_url":"https://example.com/a/b?x=1","sessions":"3.0","engaged_sessions":"bad","conversions":"1"}]')
    assert rows[0]["landing_path"]=="/a/b"; assert rows[0]["source"]=="reddit"; assert rows[0]["engaged_sessions"]==0
    c=sqlite3.connect(":memory:"); upsert_website_referral_sessions(c,rows); upsert_website_referral_sessions(c,[{**rows[0],"sessions":8}])
    assert c.execute("SELECT COUNT(*),sessions FROM website_referral_sessions").fetchone()==(1,8)
    p=tmp_path/"ref.csv"; p.write_text("date,source,medium,campaign,landing_path,sessions\n2026-05-01,A,B,C,/x?y=1,1\n"); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--dry-run"])==0
